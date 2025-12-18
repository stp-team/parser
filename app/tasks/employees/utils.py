import logging
from collections.abc import Callable
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
    safe_get_attr,
    validate_api_result,
)


@dataclass
class EmployeeProcessingConfig:
    """Конфигурация для обработки данных сотрудников."""

    update_type: str
    semaphore_limit: int = 10
    requires_detailed_api: bool = True


class EmployeeMatcher:
    """Утилита для сопоставления сотрудников между БД и API."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def find_employee_pairs(
        self,
        db_employees: list[Any],
        api_employees: list[Any],
        filter_func: Callable[[Any], bool],
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
        """
        Находит соответствующего сотрудника API для сотрудника БД.
        Оптимизированная версия с использованием validate_api_result и safe_get_attr.
        """
        # Поиск по employee_id (если есть в БД)
        employee_id = safe_get_attr(db_employee, "employee_id", converter=str)
        if employee_id:
            api_employee = api_by_id.get(employee_id)
            if validate_api_result(api_employee):
                stats["matched_by_id"] += 1
                self.logger.debug(
                    f"Найден по employee_id {employee_id}: {db_employee.fullname}"
                )
                return api_employee

        # Поиск по ФИО с безопасным получением атрибута
        fullname = safe_get_attr(db_employee, "fullname")
        if fullname:
            api_employee = api_by_name.get(fullname)
            if validate_api_result(api_employee):
                stats["matched_by_name"] += 1
                self.logger.debug(f"Найден по ФИО: {fullname}")
                return api_employee

        return None


class EmployeeDataExtractor:
    """
    Оптимизированный извлекатель данных сотрудников.
    Использует универсальный подход вместо дублирования методов.
    """

    # Маппинг полей для разных типов обновлений
    FIELD_MAPPINGS = {
        "birthday": ["birthday"],
        "employment_date": ["employment_date"],
        "employee_id": ["id"],
        "all": ["birthday", "employment_date", "id"],
    }

    @staticmethod
    def _validate_employee_data(employee_data: Any) -> bool:
        """Валидирует данные сотрудника с использованием новых утилит."""
        return validate_api_result(employee_data, "employeeInfo")

    @staticmethod
    def _create_base_update_data(db_employee: Any) -> EmployeeUpdateData:
        """Создает базовый объект обновления."""
        return EmployeeUpdateData(
            user_id=db_employee.user_id, fullname=db_employee.fullname
        )

    @staticmethod
    def extract_data_by_type(
        employee_data: Any, db_employee: Any, update_type: str
    ) -> EmployeeUpdateData | None:
        """
        Универсальный экстрактор данных по типу обновления.
        Заменяет все специализированные методы.
        """
        if not EmployeeDataExtractor._validate_employee_data(employee_data):
            return None

        emp_info = employee_data.employeeInfo
        update_data = EmployeeDataExtractor._create_base_update_data(db_employee)

        # Получаем список полей для данного типа обновления
        fields_to_extract = EmployeeDataExtractor.FIELD_MAPPINGS.get(update_type, [])

        fields_updated = False

        for field in fields_to_extract:
            value = safe_get_attr(emp_info, field)
            if value:
                if field == "birthday":
                    update_data.birthday = value
                    fields_updated = True
                elif field == "employment_date":
                    update_data.employment_date = value
                    fields_updated = True
                elif field == "id":
                    update_data.employee_id = safe_get_attr(
                        emp_info, field, converter=int
                    )
                    if update_data.employee_id:
                        fields_updated = True

        return update_data if fields_updated else None

    @staticmethod
    def extract_birthday_data(
        employee_data: Any, db_employee: Any
    ) -> EmployeeUpdateData | None:
        """Извлекает данные дня рождения для обновления."""
        return EmployeeDataExtractor.extract_data_by_type(
            employee_data, db_employee, "birthday"
        )

    @staticmethod
    def extract_employment_date_data(
        employee_data: Any, db_employee: Any
    ) -> EmployeeUpdateData | None:
        """Извлекает данные даты трудоустройства для обновления."""
        return EmployeeDataExtractor.extract_data_by_type(
            employee_data, db_employee, "employment_date"
        )

    @staticmethod
    def extract_employee_id_data(
        employee_data: Any, db_employee: Any
    ) -> EmployeeUpdateData | None:
        """Извлекает employee_id для обновления."""
        return EmployeeDataExtractor.extract_data_by_type(
            employee_data, db_employee, "employee_id"
        )

    @staticmethod
    def extract_employee_id_from_list(
        api_employee: Any, db_employee: Any
    ) -> EmployeeUpdateData | None:
        """Извлекает employee_id из списка сотрудников API (без детального запроса)."""
        if not validate_api_result(api_employee, "id"):
            return None

        employee_id = safe_get_attr(api_employee, "id", converter=int)
        if not employee_id:
            return None

        update_data = EmployeeDataExtractor._create_base_update_data(db_employee)
        update_data.employee_id = employee_id
        return update_data

    @staticmethod
    def extract_all_data(
        employee_data: Any, db_employee: Any
    ) -> EmployeeUpdateData | None:
        """Извлекает все доступные данные для обновления."""
        return EmployeeDataExtractor.extract_data_by_type(
            employee_data, db_employee, "all"
        )


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
        data_extractor: Callable[[Any, Any], EmployeeUpdateData | None] = None,
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


def create_employee_config(
    update_type: str, fast_mode: bool = False
) -> EmployeeProcessingConfig:
    """
    Фабрика конфигураций сотрудников для устранения дублирования.

    Args:
        update_type: Тип обновления для логирования
        fast_mode: Использовать быстрый режим (без детальных API запросов)

    Returns:
        Настроенная конфигурация

    Examples:
        >>> config = create_employee_config("Дни рождений")
        >>> fast_config = create_employee_config("Employee ID", fast_mode=True)
    """
    return EmployeeProcessingConfig(
        update_type=update_type,
        semaphore_limit=0 if fast_mode else 10,
        requires_detailed_api=not fast_mode,
    )
