import logging
from collections.abc import Callable
from typing import Any

from stp_database.repo.STP import MainRequestsRepo

from app.api.employees import EmployeesAPI
from app.core.db import get_stp_session
from app.models.employees import EmployeeUpdateData
from app.tasks.base import (
    create_config_factory,
    log_processing_time,
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


def create_employee_task(
    config: EmployeeProcessingConfig,
    filter_func: Callable[[Any], bool],
    data_extractor: Callable[[Any, Any], EmployeeUpdateData | None] = None,
):
    """Создает таски из конфига."""

    @log_processing_time(f"заполнение {config.update_type.lower()}")
    async def task_function(api: EmployeesAPI) -> list[Any]:
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
