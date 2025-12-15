import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from stp_database.repo.STP import MainRequestsRepo

from app.api.employees import EmployeesAPI
from app.core.db import get_stp_session
from app.models.employees import EmployeeUpdateData
from app.tasks.base import (
    APIProcessor,
    BatchDBOperator,
    ConcurrentAPIFetcher,
    create_lookup_index,
    filter_items,
    log_processing_time,
)

logger = logging.getLogger(__name__)


@dataclass
class EmployeeProcessingConfig:
    """Конфигурация для обработки данных сотрудников."""

    update_type: str
    semaphore_limit: int = 10
    requires_detailed_api: bool = True  # Требует ли детальных API запросов

    def get_delete_func(self):
        # Для сотрудников не используется удаление, только обновление
        return None


class EmployeeMatcher:
    """Утилита для сопоставления сотрудников между БД и API."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def find_employee_pairs(
        self, db_employees: list[Any], api_employees: list[Any], filter_func: callable
    ) -> list[tuple[Any, Any]]:
        """
        Находит пары сотрудников между БД и API с оптимизированным поиском.

        Args:
            db_employees: Список сотрудников из БД
            api_employees: Список сотрудников из API
            filter_func: Функция фильтрации сотрудников БД

        Returns:
            Список пар (db_employee, api_employee)
        """
        # Фильтруем сотрудников БД
        filtered_employees = filter_items(db_employees, filter_func)

        self.logger.info(
            f"Найдено {len(filtered_employees)} сотрудников для обновления"
        )

        if not filtered_employees:
            return []

        # Создаем индексы для быстрого поиска
        api_by_id = create_lookup_index(api_employees, lambda emp: str(emp.id))
        api_by_name = create_lookup_index(api_employees, lambda emp: emp.fullname)

        # Сопоставляем сотрудников
        pairs = []
        stats = {"matched_by_id": 0, "matched_by_name": 0}

        for db_employee in filtered_employees:
            if not db_employee.user_id:
                continue

            api_employee = self._find_api_employee(
                db_employee, api_by_id, api_by_name, stats
            )

            if api_employee:
                pairs.append((db_employee, api_employee))
            else:
                self.logger.debug(f"Не найдена пара для {db_employee.fullname} в API")

        self.logger.info(
            f"Найдено {len(pairs)} пар для обновления "
            f"(по ID: {stats['matched_by_id']}, по ФИО: {stats['matched_by_name']})"
        )
        return pairs

    def _find_api_employee(
        self, db_employee: Any, api_by_id: dict, api_by_name: dict, stats: dict
    ) -> Any | None:
        """Находит соответствующего сотрудника API для сотрудника БД."""
        # Поиск по employee_id (если есть в БД)
        if hasattr(db_employee, "employee_id") and db_employee.employee_id:
            try:
                employee_id = str(int(db_employee.employee_id))
                api_employee = api_by_id.get(employee_id)
                if api_employee:
                    stats["matched_by_id"] += 1
                    self.logger.debug(
                        f"Найден по employee_id {employee_id}: {db_employee.fullname}"
                    )
                    return api_employee
            except (ValueError, TypeError):
                pass  # employee_id не является числом

        # Поиск по ФИО
        api_employee = api_by_name.get(db_employee.fullname)
        if api_employee:
            stats["matched_by_name"] += 1
            self.logger.debug(f"Найден по ФИО: {db_employee.fullname}")
            return api_employee

        return None


class EmployeeDataExtractor:
    """Извлекатель данных сотрудников для различных типов обновлений."""

    @staticmethod
    def extract_birthday_data(
        employee_data: Any, db_employee: Any
    ) -> EmployeeUpdateData | None:
        """Извлекает данные дня рождения для обновления."""
        if (
            employee_data
            and hasattr(employee_data, "employeeInfo")
            and employee_data.employeeInfo.birthday
        ):
            return EmployeeUpdateData(
                user_id=db_employee.user_id,
                fullname=db_employee.fullname,
                birthday=employee_data.employeeInfo.birthday,
            )
        return None

    @staticmethod
    def extract_employment_date_data(
        employee_data: Any, db_employee: Any
    ) -> EmployeeUpdateData | None:
        """Извлекает данные даты трудоустройства для обновления."""
        if (
            employee_data
            and hasattr(employee_data, "employeeInfo")
            and employee_data.employeeInfo.employment_date
        ):
            return EmployeeUpdateData(
                user_id=db_employee.user_id,
                fullname=db_employee.fullname,
                employment_date=employee_data.employeeInfo.employment_date,
            )
        return None

    @staticmethod
    def extract_employee_id_data(
        employee_data: Any, db_employee: Any
    ) -> EmployeeUpdateData | None:
        """Извлекает employee_id для обновления."""
        if (
            employee_data
            and hasattr(employee_data, "employeeInfo")
            and employee_data.employeeInfo.id
        ):
            return EmployeeUpdateData(
                user_id=db_employee.user_id,
                fullname=db_employee.fullname,
                employee_id=int(employee_data.employeeInfo.id),
            )
        return None

    @staticmethod
    def extract_employee_id_from_list(
        api_employee: Any, db_employee: Any
    ) -> EmployeeUpdateData | None:
        """Извлекает employee_id из списка сотрудников API (без детального запроса)."""
        if api_employee and api_employee.id:
            return EmployeeUpdateData(
                user_id=db_employee.user_id,
                fullname=db_employee.fullname,
                employee_id=api_employee.id,
            )
        return None

    @staticmethod
    def extract_all_data(
        employee_data: Any, db_employee: Any
    ) -> EmployeeUpdateData | None:
        """Извлекает все доступные данные для обновления."""
        if not employee_data or not hasattr(employee_data, "employeeInfo"):
            return None

        emp_info = employee_data.employeeInfo

        update_data = EmployeeUpdateData(
            user_id=db_employee.user_id, fullname=db_employee.fullname
        )

        # Заполняем только непустые поля
        if emp_info.birthday:
            update_data.birthday = emp_info.birthday
        if emp_info.id:
            update_data.employee_id = int(emp_info.id)
        if emp_info.employment_date:
            update_data.employment_date = emp_info.employment_date

        # Возвращаем только если есть хотя бы одно поле для обновления
        if (
            update_data.birthday
            or update_data.employee_id
            or update_data.employment_date
        ):
            return update_data
        return None


class EmployeeProcessor(APIProcessor[EmployeeUpdateData, EmployeeProcessingConfig]):
    """Процессор для обработки данных сотрудников."""

    def __init__(self, api: EmployeesAPI):
        super().__init__(api)
        self.matcher = EmployeeMatcher()
        self.fetcher = ConcurrentAPIFetcher()

    async def fetch_data(
        self,
        config: EmployeeProcessingConfig,
        employee_pairs: list[tuple[Any, Any]] = None,
        **kwargs,
    ) -> list[tuple[Any, ...]]:
        """Получает данные сотрудников из API."""
        if not config.requires_detailed_api:
            # Для быстрого обновления employee_id данные уже есть в парах
            return [(pair, pair) for pair in employee_pairs] if employee_pairs else []

        # Для детальных запросов получаем данные параллельно
        if not employee_pairs:
            return []

        tasks = [(api_emp.id, db_emp.fullname) for db_emp, api_emp in employee_pairs]

        async def fetch_employee_detail(emp_id, emp_name):
            try:
                employee_data = await self.api.get_employee(employee_id=emp_id)
                return employee_data, emp_name, emp_id
            except Exception as e:
                self.logger.error(f"Ошибка получения сотрудника {emp_name} из API: {e}")
                return None, emp_name, emp_id

        return await self.fetcher.fetch_parallel(tasks, fetch_employee_detail)

    def process_results(
        self,
        results: list[tuple[Any, ...]],
        config: EmployeeProcessingConfig,
        data_extractor: callable = None,
        employee_pairs: list[tuple[Any, Any]] = None,
        **kwargs,
    ) -> list[EmployeeUpdateData]:
        """Обрабатывает результаты API и подготавливает данные для обновления БД."""
        updates = []
        successful_count = 0

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.error(f"Ошибка при выполнении запроса: {result}")
                continue

            if not config.requires_detailed_api:
                # Для быстрого обновления
                (db_employee, api_employee) = result[0]
                update_data = EmployeeDataExtractor.extract_employee_id_from_list(
                    api_employee, db_employee
                )
            else:
                # Для детальных запросов
                employee_data, db_employee_name, api_employee_id = result[1]
                if not employee_data:
                    continue

                db_employee = employee_pairs[i][0]
                update_data = data_extractor(employee_data, db_employee)

            if update_data:
                updates.append(update_data)
                successful_count += 1
                self.logger.debug(f"Подготовлены данные для {update_data.fullname}")
            else:
                self.logger.warning("Не удалось извлечь данные для сотрудника")

        self.logger.info(f"Успешно подготовлено {successful_count} обновлений")
        return updates

    async def save_data(
        self, data: list[EmployeeUpdateData], config: EmployeeProcessingConfig, **kwargs
    ) -> int:
        """Сохраняет данные сотрудников в БД."""
        if not data:
            return 0

        async with get_stp_session() as session:
            repo = MainRequestsRepo(session)
            db_operator = BatchDBOperator(session)

            async def update_employee(update_data: EmployeeUpdateData):
                kwargs = {"user_id": update_data.user_id}

                if update_data.birthday is not None:
                    kwargs["birthday"] = update_data.birthday
                if update_data.employee_id is not None:
                    kwargs["employee_id"] = update_data.employee_id
                if update_data.employment_date is not None:
                    kwargs["employment_date"] = update_data.employment_date

                await repo.employee.update_user(**kwargs)

            updates_dict = [
                {
                    "user_id": item.user_id,
                    "fullname": item.fullname,
                    "birthday": item.birthday,
                    "employee_id": item.employee_id,
                    "employment_date": item.employment_date,
                }
                for item in data
            ]

            return await db_operator.bulk_update_with_transaction(
                updates_dict,
                lambda d: update_employee(
                    EmployeeUpdateData(**{k: v for k, v in d.items() if v is not None})
                ),
                config.update_type,
            )


# Конфигурации для различных типов обновлений
BIRTHDAY_CONFIG = EmployeeProcessingConfig(
    update_type="Дни рождений", semaphore_limit=10, requires_detailed_api=True
)

EMPLOYMENT_DATE_CONFIG = EmployeeProcessingConfig(
    update_type="Даты трудоустройства", semaphore_limit=10, requires_detailed_api=True
)

EMPLOYEE_ID_CONFIG = EmployeeProcessingConfig(
    update_type="Employee ID", semaphore_limit=10, requires_detailed_api=True
)

EMPLOYEE_ID_FAST_CONFIG = EmployeeProcessingConfig(
    update_type="Employee ID Fast",
    semaphore_limit=0,  # Не используется для быстрого режима
    requires_detailed_api=False,
)

FULL_UPDATE_CONFIG = EmployeeProcessingConfig(
    update_type="Полные данные", semaphore_limit=10, requires_detailed_api=True
)


# Функции фильтрации сотрудников
def filter_missing_birthday(emp) -> bool:
    """Фильтр для сотрудников без дня рождения."""
    return not emp.birthday and emp.user_id


def filter_missing_employment_date(emp) -> bool:
    """Фильтр для сотрудников без даты трудоустройства."""
    return not emp.employment_date and emp.user_id


def filter_missing_employee_id(emp) -> bool:
    """Фильтр для сотрудников без employee_id."""
    return not emp.employee_id and emp.user_id


def filter_missing_any_data(emp) -> bool:
    """Фильтр для сотрудников с любыми недостающими данными."""
    return (
        not emp.birthday or not emp.employee_id or not emp.employment_date
    ) and emp.user_id


async def _process_employees_with_config(
    api: EmployeesAPI,
    config: EmployeeProcessingConfig,
    filter_func: callable,
    data_extractor: callable = None,
) -> list[Any]:
    """Универсальная функция обработки сотрудников с конфигурацией."""
    processor = EmployeeProcessor(api)

    # Получаем всех сотрудников из API
    employees = await api.get_employees(exclude_fired=True)
    processor.logger.debug(f"Найдено {len(employees)} сотрудников в API")

    if not employees:
        processor.logger.warning("Не найдено сотрудников в API")
        return []

    # Получаем сотрудников из БД и находим пары
    async with get_stp_session() as session:
        repo = MainRequestsRepo(session)
        db_employees = await repo.employee.get_users()
        processor.logger.debug(f"Найдено {len(db_employees)} сотрудников в БД")

        employee_pairs = processor.matcher.find_employee_pairs(
            db_employees, employees, filter_func
        )

        if not employee_pairs:
            processor.logger.info(
                f"Нет сотрудников для обновления в {config.update_type}"
            )
            return employees

        # Обрабатываем данные
        await processor.process_with_config(
            config, employee_pairs=employee_pairs, data_extractor=data_extractor
        )

    return employees


# Публичные функции API
@log_processing_time("заполнение дней рождений")
async def fill_birthdays(api: EmployeesAPI) -> list[Any]:
    """Заполняет дни рождений сотрудников."""
    return await _process_employees_with_config(
        api,
        BIRTHDAY_CONFIG,
        filter_missing_birthday,
        EmployeeDataExtractor.extract_birthday_data,
    )


@log_processing_time("заполнение дат трудоустройства")
async def fill_employment_dates(api: EmployeesAPI) -> list[Any]:
    """Заполняет даты трудоустройства сотрудников."""
    return await _process_employees_with_config(
        api,
        EMPLOYMENT_DATE_CONFIG,
        filter_missing_employment_date,
        EmployeeDataExtractor.extract_employment_date_data,
    )


@log_processing_time("быстрое заполнение employee_id")
async def fill_employee_ids(api: EmployeesAPI) -> list[Any]:
    """Быстро заполняет employee_id сотрудников из основного списка API."""
    return await _process_employees_with_config(
        api, EMPLOYEE_ID_FAST_CONFIG, filter_missing_employee_id
    )


@log_processing_time("заполнение всех данных сотрудников")
async def fill_all_employee_data(api: EmployeesAPI) -> list[Any]:
    """Заполняет все недостающие данные сотрудников."""
    return await _process_employees_with_config(
        api,
        FULL_UPDATE_CONFIG,
        filter_missing_any_data,
        EmployeeDataExtractor.extract_all_data,
    )


@log_processing_time("полное обновление данных сотрудников")
async def fill_employees(api: EmployeesAPI) -> list[Any]:
    """
    Основная функция для вызова в планировщике.

    Оптимизированный подход:
    1. Быстро заполняет employee_id из основного списка
    2. Заполняет дни рождения (требует детальных API запросов)
    3. Заполняет даты трудоустройства (требует детальных API запросов)
    """
    try:
        # Быстро заполняем employee_id
        employees = await fill_employee_ids(api)

        # Параллельно заполняем остальные данные
        await asyncio.gather(
            fill_birthdays(api), fill_employment_dates(api), return_exceptions=True
        )

        return employees

    except Exception as e:
        logger.error(f"Ошибка обновления данных сотрудников: {e}")
        raise
