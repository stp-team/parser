import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from okc_py.repos import DossierAPI, TutorsAPI
from stp_database.models.STP import Employee
from stp_database.repo.STP import MainRequestsRepo

from app.core.db import get_stp_session
from app.tasks.base import (
    APIProcessor,
    PeriodHelper,
    ProcessingConfig,
    create_config_factory,
    log_processing_time,
    safe_get_attr,
    validate_api_result,
)
from app.tasks.employees.filters import (
    filter_missing_any_data,
    filter_missing_birthday,
    filter_missing_employee_id,
    filter_missing_employment_date,
)
from app.tasks.employees.utils import (
    EmployeeDataExtractor,
    EmployeeProcessingConfig,
    EmployeeProcessor,
)

logger = logging.getLogger(__name__)

employee_config_factory = create_config_factory(
    EmployeeProcessingConfig,
    semaphore_limit=10,
    requires_detailed_api=True,
)

BIRTHDAYS_CONFIG = employee_config_factory(update_type="Дни рождений")
EMPLOYMENT_DATES_CONFIG = employee_config_factory(update_type="Даты трудоустройства")
IDS_CONFIG = employee_config_factory(
    update_type="Employee ID", requires_detailed_api=False
)
all_data_config = employee_config_factory(update_type="Полные данные")
TUTOR_INFO_CONFIG = employee_config_factory(
    update_type="Информация о наставничестве", requires_detailed_api=False
)


@dataclass
class TutorProcessingConfig(ProcessingConfig):
    """Конфигурация для обработки данных наставников."""

    update_type: str = "Информация о наставничестве"
    division_id: int = 2
    months_lookback: int = 6

    def get_delete_func(self):
        # Для наставников мы не удаляем старые данные, а сбрасываем флаги
        return None


class TutorProcessor(APIProcessor[dict, TutorProcessingConfig]):
    """Процессор для обработки данных наставников с оптимизированными batch операциями."""

    def __init__(self, tutors_api: Any, employees_api: Any):
        super().__init__(tutors_api)
        self.tutors_api = tutors_api
        self.employees_api = employees_api

    async def fetch_data(
        self, config: TutorProcessingConfig, **kwargs
    ) -> list[tuple[Any, ...]]:
        """Получаем данные о наставниках из API."""

        # Получаем фильтры для определения доступных опций
        graph_filters = await self.tutors_api.get_graph_filters(
            division_id=config.division_id
        )
        if not validate_api_result(graph_filters, "tutors"):
            self.logger.warning("Не получены фильтры графика из API")
            return []

        # Подготавливаем параметры для получения полного графика за N месяцев
        months_data = PeriodHelper.get_previous_months(config.months_lookback)
        first_period = months_data[0]  # Самый ранний период
        last_period = months_data[-1]  # Самый поздний период

        # Получаем границы дат для первого и последнего периодов
        start_date_str, _ = PeriodHelper.get_date_range_for_period(first_period)
        _, end_date_str = PeriodHelper.get_date_range_for_period(last_period)

        # Получаем все доступные ID для фильтров
        picked_units = [
            safe_get_attr(unit, "id")
            for unit in graph_filters.units
            if safe_get_attr(unit, "id")
        ]
        picked_tutor_types = [
            safe_get_attr(t_type, "id")
            for t_type in graph_filters.tutor_types
            if safe_get_attr(t_type, "id")
        ]
        picked_shift_types = [
            safe_get_attr(s_type, "id")
            for s_type in graph_filters.shift_types
            if safe_get_attr(s_type, "id")
        ]

        self.logger.info(f"Получаем данные за период {start_date_str} - {end_date_str}")

        # Получаем полный график наставников за последние N месяцев
        tutor_graph = await self.tutors_api.get_full_graph(
            division_id=config.division_id,
            start_date=start_date_str,
            stop_date=end_date_str,
            picked_units=picked_units,
            picked_tutor_types=picked_tutor_types,
            picked_shift_types=picked_shift_types,
        )

        if not validate_api_result(tutor_graph, "tutors"):
            self.logger.warning("Не получены данные о наставниках из полного графика")
            return []

        # Возвращаем кортежи с данными для дальнейшей обработки
        return [(graph_filters, tutor_graph)]

    def process_results(
        self, results: list[tuple[Any, ...]], config: TutorProcessingConfig, **kwargs
    ) -> list[dict]:
        """Обрабатываем данные наставников и создаем список для batch операций."""

        if not results:
            return []

        graph_filters, tutor_graph = results[0]

        # Создаем словарь всех наставников из N-месячного периода для поиска по ID
        full_tutors_dict = {}
        for tutor in tutor_graph.tutors:
            tutor_id = safe_get_attr(tutor.tutor_info, "tutor_id")
            if tutor_id:
                full_tutors_dict[tutor_id] = {
                    "tutor_id": tutor_id,
                    "full_name": safe_get_attr(tutor.tutor_info, "full_name", ""),
                    "tutor_type": safe_get_attr(tutor.tutor_info, "tutor_type", ""),
                }

        self.logger.info(
            f"Получено {len(full_tutors_dict)} уникальных наставников из полного графика"
        )

        # Получаем список текущих активных наставников
        current_tutors = graph_filters.tutors
        self.logger.info(f"Получено {len(current_tutors)} активных наставников")

        # Находим полные имена для активных наставников
        active_tutors_data = []
        for current_tutor in current_tutors:
            tutor_id = safe_get_attr(current_tutor, "id")
            if not tutor_id:
                continue

            if tutor_id in full_tutors_dict:
                # Используем полную информацию из N-месячного периода
                full_tutor_info = full_tutors_dict[tutor_id]
                active_tutors_data.append(
                    {
                        "tutor_id": tutor_id,
                        "full_name": full_tutor_info["full_name"],
                        "tutor_type": safe_get_attr(
                            current_tutor, "tutor_type", ""
                        ),  # Используем актуальный тип из фильтров
                    }
                )
            else:
                # Если не найден в N-месячном периоде, используем только имя из фильтров
                active_tutors_data.append(
                    {
                        "tutor_id": tutor_id,
                        "full_name": safe_get_attr(
                            current_tutor, "name", ""
                        ),  # Используем краткое имя
                        "tutor_type": safe_get_attr(current_tutor, "tutor_type", ""),
                    }
                )

        self.logger.info(
            f"Найдено {len(active_tutors_data)} активных наставников с полными именами"
        )
        return active_tutors_data

    async def save_data(
        self, data: list[dict], config: TutorProcessingConfig, **kwargs
    ) -> int:
        """Сохраняем данные наставников с использованием оптимизированных SQLAlchemy операций."""

        if not data:
            self.logger.info("Нет данных наставников для сохранения")
            return 0

        async with get_stp_session() as session:
            # Начинаем транзакцию для всех операций
            async with session.begin():
                from sqlalchemy import select, update

                # Шаг 1: Сбрасываем все флаги наставников одним SQL запросом
                reset_stmt = (
                    update(Employee)
                    .where(Employee.is_tutor)
                    .values(is_tutor=False, tutor_type=None)
                )
                reset_result = await session.execute(reset_stmt)
                reset_count = reset_result.rowcount
                self.logger.info(f"Сброшено флагов наставников: {reset_count}")

                # Шаг 2: Получаем только сотрудников, которых нужно обновить
                fullnames = [
                    tutor["full_name"] for tutor in data if tutor.get("full_name")
                ]

                if not fullnames:
                    self.logger.info("Нет валидных имен наставников для обновления")
                    return 0

                # Получаем только нужных сотрудников одним запросом
                employees_stmt = select(Employee.user_id, Employee.fullname).where(
                    Employee.fullname.in_(fullnames)
                )
                employees_result = await session.execute(employees_stmt)
                employees_dict = {row.fullname: row.user_id for row in employees_result}

                self.logger.info(
                    f"Найдено {len(employees_dict)} сотрудников для обновления"
                )

                # Шаг 3: Подготавливаем данные для batch update
                update_operations = []
                tutor_employees_found = 0

                for tutor in data:
                    fullname = tutor.get("full_name")
                    if not fullname:
                        continue

                    user_id = employees_dict.get(fullname)
                    if user_id:
                        update_operations.append(
                            {
                                "user_id": user_id,
                                "tutor_type": tutor.get("tutor_type", ""),
                            }
                        )
                        tutor_employees_found += 1
                        self.logger.debug(f"Найден наставник: {fullname}")
                    else:
                        self.logger.debug(f"Не найден сотрудник с именем: {fullname}")

                # Шаг 4: Обновляем наставников batch операциями
                update_count = 0
                if update_operations:
                    # Выполняем обновления небольшими группами для лучшей производительности
                    batch_size = 100
                    for i in range(0, len(update_operations), batch_size):
                        batch = update_operations[i : i + batch_size]
                        user_ids = [op["user_id"] for op in batch]

                        # Сначала устанавливаем is_tutor = True для этой группы
                        update_stmt = (
                            update(Employee)
                            .where(Employee.user_id.in_(user_ids))
                            .values(is_tutor=True)
                        )
                        batch_result = await session.execute(update_stmt)
                        update_count += batch_result.rowcount

                        # Затем обновляем tutor_type индивидуально для каждого (быстро для малых групп)
                        for op in batch:
                            tutor_type_stmt = (
                                update(Employee)
                                .where(Employee.user_id == op["user_id"])
                                .values(tutor_type=op["tutor_type"])
                            )
                            await session.execute(tutor_type_stmt)

                self.logger.info(
                    f"Найдено и обновлено {tutor_employees_found} наставников"
                )
                self.logger.info(f"SQL обновлено записей: {update_count}")
                self.logger.info(
                    f"Всего затронуто записей: {reset_count + update_count}"
                )

                return update_count


# Конфигурация для оптимизированной обработки наставников
TUTOR_CONFIG = TutorProcessingConfig()


def create_employee_task(
    config: EmployeeProcessingConfig,
    filter_func: Callable[[Any], bool],
    data_extractor: Callable[[Any, Any], EmployeeUpdateData | None] = None,
):
    """Создает таски из конфига."""

    @log_processing_time(f"заполнение {config.update_type.lower()}")
    async def task_function(api: Any) -> list[Any]:
        processor = EmployeeProcessor(api)

        # Получаем всех сотрудников из API
        employees = await api.get_employees(exclude_fired=True)

        if not employees:
            processor.logger.warning("Не найдено сотрудников в API")
            return []

        # Получаем сотрудников из БД и находим пары
        async with get_stp_session() as session:
            repo = MainRequestsRepo(session)
            db_employees = await repo.employee.get_users()

            employee_pairs = processor.matcher.find_employee_pairs(
                db_employees, employees, filter_func
            )

            if not employee_pairs:
                processor.logger.info(
                    f"Нет сотрудников для обновления в {config.update_type}"
                )
                return employees

            # Процессим найденные пары
            await processor.process_with_config(
                config, employee_pairs=employee_pairs, data_extractor=data_extractor
            )

        return employees

    return task_function


fill_birthdays = create_employee_task(
    BIRTHDAYS_CONFIG,
    filter_missing_birthday,
    EmployeeDataExtractor.extract_birthday_data,
)

fill_employment_dates = create_employee_task(
    EMPLOYMENT_DATES_CONFIG,
    filter_missing_employment_date,
    EmployeeDataExtractor.extract_employment_date_data,
)

fill_employee_ids = create_employee_task(IDS_CONFIG, filter_missing_employee_id)

fill_all_employee_data = create_employee_task(
    all_data_config, filter_missing_any_data, EmployeeDataExtractor.extract_all_data
)


@log_processing_time("заполнение информации о наставничестве")
async def fill_tutors(tutors_api: Any, employees_api: Any) -> list[Any]:
    """
    Оптимизированная версия заполнения информации о наставничестве сотрудников.

    Использует batch операции для значительного повышения производительности:
    - Создает индекс сотрудников в памяти для быстрого поиска
    - Выполняет batch обновления вместо индивидуальных запросов к БД
    - Следует архитектурным паттернам из claude.md

    Args:
        tutors_api: API для получения данных о наставниках
        employees_api: API для получения данных о сотрудниках

    Returns:
        Список обработанных данных наставников
    """
    processor = TutorProcessor(tutors_api, employees_api)

    # Выполняем полный ETL pipeline: Extract, Transform, Load
    results = await processor.fetch_data(TUTOR_CONFIG)
    if results:
        processed_data = processor.process_results(results, TUTOR_CONFIG)
        # Сохраняем данные в базу (ранее отсутствующий шаг)
        await processor.save_data(processed_data, TUTOR_CONFIG)
        return processed_data
    return []
