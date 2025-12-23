"""
Задачи для обработки данных назначенных тестов.
Следует архитектуре APIProcessor из app/tasks/base.py.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession
from stp_database.models.Stats import AssignedTest
from stp_database.repo.STP import MainRequestsRepo

from app.api.tests import TestsAPI
from app.core.db import get_stats_session, get_stp_session
from app.tasks.base import (
    APIProcessor,
    BatchDBOperator,
    PeriodHelper,
    ProcessingConfig,
    log_processing_time,
    validate_api_result,
)
from app.tasks.tests.utils import (
    create_assigned_test_from_api_data,
    filter_valid_tests,
    format_date_range_for_tests,
    get_default_subdivisions,
)

logger = logging.getLogger(__name__)


async def delete_all_assigned_tests(session):
    """Удаляет все записи из таблицы AssignedTest."""
    try:
        # Удаляем все записи
        await session.execute(delete(AssignedTest))
        logger.info("Все записи тестов удалены из таблицы")
    except Exception as e:
        logger.error(f"Ошибка при удалении записей тестов: {e}")
        raise


@dataclass
class TestsProcessingConfig(ProcessingConfig):
    """Конфигурация для обработки данных тестов."""

    update_type: str
    start_date: str = None
    end_date: str = None
    subdivisions: list[int] = None
    delete_func: Callable[[AsyncSession], Any] = None

    def __post_init__(self):
        """Инициализация значений по умолчанию."""
        if self.subdivisions is None:
            self.subdivisions = get_default_subdivisions()

        # Если даты не указаны, используем период от 12 месяцев назад до сегодня
        if self.start_date is None or self.end_date is None:
            now = datetime.now()

            # Начальная дата - 3 месяца назад
            if self.start_date is None:
                three_months_ago = PeriodHelper.add_months(now, -12)
                self.start_date = PeriodHelper.format_date_for_api(
                    three_months_ago, "DD.MM.YYYY"
                )

            # Конечная дата - сегодня
            if self.end_date is None:
                self.end_date = PeriodHelper.format_date_for_api(now, "DD.MM.YYYY")

    def get_delete_func(self):
        """Возвращает функцию удаления всех данных тестов."""
        return self.delete_func


class TestsProcessor(APIProcessor[AssignedTest, TestsProcessingConfig]):
    """Процессор для обработки данных назначенных тестов."""

    def __init__(self, api: TestsAPI):
        super().__init__(api)

    async def fetch_data(
        self, config: TestsProcessingConfig, **kwargs
    ) -> list[tuple[Any, ...]]:
        """
        Получает данные о назначенных тестах из API.

        Args:
            config: Конфигурация обработки
            **kwargs: Дополнительные параметры

        Returns:
            Список кортежей (параметры_запроса, результат_api)
        """
        try:
            # Подготавливаем параметры для API запроса
            start_date, end_date = format_date_range_for_tests(
                config.start_date, config.end_date
            )

            self.logger.info(
                f"Запрашиваем тесты за период {start_date} - {end_date} "
                f"для подразделений: {config.subdivisions}"
            )

            # Выполняем API запрос
            api_result = await self.api.get_assigned_tests(
                start_date=start_date,
                stop_date=end_date,
                subdivisions=config.subdivisions,
            )

            # Возвращаем в формате, ожидаемом базовым классом
            task_params = (start_date, end_date, config.subdivisions)
            return [(task_params, api_result)]

        except Exception as e:
            self.logger.error(f"Ошибка получения данных тестов: {e}")
            return []

    def process_results(
        self, results: list[tuple[Any, ...]], config: TestsProcessingConfig, **kwargs
    ) -> list[AssignedTest]:
        """
        Обрабатывает результаты API и конвертирует в модели AssignedTest.

        Args:
            results: Список результатов от fetch_data
            config: Конфигурация обработки
            **kwargs: Дополнительные параметры

        Returns:
            Список объектов AssignedTest
        """
        processed_tests = []

        for task_params, api_result in results:
            if not validate_api_result(api_result):
                self.logger.warning(
                    f"Невалидный API результат для параметров {task_params}"
                )
                continue

            # Если api_result это уже список тестов
            tests_data = api_result if isinstance(api_result, list) else [api_result]

            # Фильтруем валидные тесты
            valid_tests_data = filter_valid_tests(tests_data)

            self.logger.info(
                f"Получено {len(valid_tests_data)} валидных тестов "
                f"из {len(tests_data)} для параметров {task_params}"
            )

            # Конвертируем в объекты AssignedTest
            for test_data in valid_tests_data:
                assigned_test = create_assigned_test_from_api_data(test_data)
                if assigned_test:
                    processed_tests.append(assigned_test)

        self.logger.info(f"Всего обработано {len(processed_tests)} тестов")
        return processed_tests

    async def save_data(
        self, data: list[AssignedTest], config: TestsProcessingConfig, **kwargs
    ) -> int:
        """
        Сохраняет данные тестов в базу данных.

        Args:
            data: Список объектов AssignedTest для сохранения
            config: Конфигурация обработки
            **kwargs: Дополнительные параметры

        Returns:
            Количество сохраненных записей
        """
        if not data:
            self.logger.warning("Нет данных тестов для сохранения")
            return 0

        try:
            # Получаем список всех пользователей из STP базы для валидации
            async with get_stp_session() as stp_session:
                repo = MainRequestsRepo(stp_session)
                all_users = await repo.employee.get_users()

                # Создаем множество ФИО для быстрого поиска
                valid_employees = {user.fullname for user in all_users if user.fullname}
                self.logger.info(
                    f"Загружено {len(valid_employees)} сотрудников из STP базы"
                )

            # Фильтруем тесты - оставляем только те, где сотрудник найден в STP базе
            validated_tests = []
            skipped_count = 0

            for test in data:
                if test.employee_fullname in valid_employees:
                    validated_tests.append(test)
                else:
                    skipped_count += 1
                    self.logger.debug(
                        f"Пропущен тест для сотрудника '{test.employee_fullname}' "
                        f"- не найден в STP базе"
                    )

            self.logger.info(
                f"Валидация завершена: {len(validated_tests)} валидных тестов, "
                f"{skipped_count} пропущено"
            )

            if not validated_tests:
                self.logger.warning(
                    "Нет валидных тестов для сохранения после проверки сотрудников"
                )
                return 0

            # Сохраняем только валидные тесты
            async with get_stats_session() as session:
                db_operator = BatchDBOperator(session)

                # Создаем функцию-обертку для удаления, которая использует текущую сессию
                async def delete_wrapper():
                    if config.get_delete_func():
                        await config.get_delete_func()(session)

                # Выполняем массовую вставку с предварительной очисткой
                saved_count = await db_operator.bulk_insert_with_cleanup(
                    data_list=validated_tests,
                    delete_func=delete_wrapper,
                    operation_name=config.update_type,
                )

                return saved_count

        except Exception as e:
            self.logger.error(f"Ошибка сохранения данных тестов: {e}")
            return 0


TESTS_CONFIG = TestsProcessingConfig(
    update_type="заполнение данных тестов",
    delete_func=delete_all_assigned_tests,
)

CURRENT_MONTH_TESTS_CONFIG = TestsProcessingConfig(
    update_type="заполнение текущих тестов",
    delete_func=delete_all_assigned_tests,
)


@log_processing_time("заполнение данных назначенных тестов")
async def fill_assigned_tests(
    api: TestsAPI,
    start_date: str = None,
    end_date: str = None,
    subdivisions: list[int] = None,
) -> int:
    """
    Заполняет данные о назначенных тестах.

    Args:
        api: Экземпляр TestsAPI
        start_date: Начальная дата в формате DD.MM.YYYY (опционально)
        end_date: Конечная дата в формате DD.MM.YYYY (опционально)
        subdivisions: Список ID подразделений (опционально)

    Returns:
        Количество обработанных записей
    """
    config = TestsProcessingConfig(
        update_type="заполнение данных назначенных тестов",
        start_date=start_date,
        end_date=end_date,
        subdivisions=subdivisions,
        delete_func=delete_all_assigned_tests,
    )

    processor = TestsProcessor(api)
    return await processor.process_with_config(config)


@log_processing_time("заполнение текущих данных тестов")
async def fill_current_tests(api: TestsAPI) -> int:
    """
    Заполняет данные тестов за текущий месяц.

    Args:
        api: Экземпляр TestsAPI

    Returns:
        Количество обработанных записей
    """
    processor = TestsProcessor(api)
    return await processor.process_with_config(CURRENT_MONTH_TESTS_CONFIG)


@log_processing_time("заполнение тестов за период")
async def fill_period_tests(
    api: TestsAPI,
    period: str,
    subdivisions: list[int] = None,
) -> int:
    """
    Заполняет данные тестов за указанный период.

    Args:
        api: Экземпляр TestsAPI
        period: Период в формате YYYY-MM
        subdivisions: Список ID подразделений (опционально)

    Returns:
        Количество обработанных записей
    """
    start_date_str, end_date_str = PeriodHelper.get_date_range_for_period(period)

    config = TestsProcessingConfig(
        update_type=f"заполнение тестов за {period}",
        start_date=start_date_str,
        end_date=end_date_str,
        subdivisions=subdivisions,
        delete_func=delete_all_assigned_tests,
    )

    processor = TestsProcessor(api)
    return await processor.process_with_config(config)
