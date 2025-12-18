import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import Delete
from sqlalchemy.ext.asyncio import AsyncSession
from stp_database.models.Stats.sl import SL

from app.api.sl import SlAPI
from app.core.db import get_stats_session
from app.tasks.base import (
    APIProcessor,
    BatchDBOperator,
    ConcurrentAPIFetcher,
    DataFieldMapper,
    ProcessingConfig,
    validate_api_result,
)

logger = logging.getLogger(__name__)


def generate_sl_daily_periods(days: int = 30) -> list[tuple[str, str]]:
    """
    Генерирует список дневных периодов для SL данных.

    Args:
        days: Количество дней назад от вчерашнего дня

    Returns:
        Список кортежей (start_date, end_date) в формате DD.MM.YYYY
        где start_date - дата извлечения, end_date - следующий день
    """
    periods = []
    # Начинаем со вчерашнего дня
    end_date = datetime.now() - timedelta(days=1)

    for days_ago in range(days):
        current_date = end_date - timedelta(days=days_ago)
        next_date = current_date + timedelta(days=1)
        start_str = current_date.strftime("%d.%m.%Y")
        end_str = next_date.strftime("%d.%m.%Y")
        periods.append((start_str, end_str))

    return periods


def generate_sl_last_month_periods() -> list[tuple[str, str]]:
    """
    Генерирует дневные периоды за весь прошлый месяц.

    Returns:
        Список кортежей (start_date, end_date) в формате DD.MM.YYYY
        где start_date - дата извлечения, end_date - следующий день
    """
    # Получаем границы прошлого месяца
    today = datetime.now()
    first_day_this_month = today.replace(day=1)
    last_day_last_month = first_day_this_month - timedelta(days=1)
    first_day_last_month = last_day_last_month.replace(day=1)

    periods = []
    current_date = first_day_last_month

    while current_date <= last_day_last_month:
        next_date = current_date + timedelta(days=1)
        start_str = current_date.strftime("%d.%m.%Y")
        end_str = next_date.strftime("%d.%m.%Y")
        periods.append((start_str, end_str))
        current_date += timedelta(days=1)

    return periods


def generate_sl_yesterday_period() -> list[tuple[str, str]]:
    """
    Генерирует период для вчерашнего дня.

    Returns:
        Список с одним кортежем (yesterday, today) в формате DD.MM.YYYY
        где yesterday - дата извлечения, today - следующий день
    """
    yesterday = datetime.now() - timedelta(days=1)
    today = datetime.now()
    start_str = yesterday.strftime("%d.%m.%Y")
    end_str = today.strftime("%d.%m.%Y")
    return [(start_str, end_str)]


class SLDBManager:
    """Управление операциями БД для Service Level данных."""

    @staticmethod
    async def delete_old_sl_data(
        session: AsyncSession, extraction_datetime: datetime
    ) -> None:
        """Удаляет данные SL за указанную дату/время."""
        # Пробуем разные возможные поля для сопоставления
        if hasattr(SL, "date"):
            if (
                hasattr(SL.date.property.columns[0].type, "python_type")
                and SL.date.property.columns[0].type.python_type == datetime
            ):
                query = Delete(SL).where(SL.date == extraction_datetime)
            else:
                query = Delete(SL).where(SL.date == extraction_datetime.date())
        elif hasattr(SL, "period"):
            query = Delete(SL).where(SL.period == extraction_datetime)
        elif hasattr(SL, "extraction_period"):
            query = Delete(SL).where(SL.extraction_period == extraction_datetime)
        elif hasattr(SL, "extraction_date"):
            if (
                hasattr(SL.extraction_date.property.columns[0].type, "python_type")
                and SL.extraction_date.property.columns[0].type.python_type == datetime
            ):
                query = Delete(SL).where(SL.extraction_date == extraction_datetime)
            else:
                query = Delete(SL).where(
                    SL.extraction_date == extraction_datetime.date()
                )
        else:
            # Если не можем найти подходящее поле, логируем предупреждение
            logger.warning(
                f"Could not find suitable date field in SL model for deletion. Available fields: {[attr for attr in dir(SL) if not attr.startswith('_')]}"
            )
            return

        if query is not None:
            await session.execute(query)


@dataclass
class SLProcessingConfig(ProcessingConfig):
    """Конфигурация для обработки данных Service Level."""

    start_date: str  # Формат "DD.MM.YYYY"
    stop_date: str  # Формат "DD.MM.YYYY"
    units: list[int] = None
    delete_func: Callable[[AsyncSession, datetime], Any] = (
        SLDBManager.delete_old_sl_data
    )
    semaphore_limit: int = 10

    def __post_init__(self):
        if self.units is None:
            self.units = [7]  # Значение по умолчанию

    def get_delete_func(self):
        """Возвращает функцию удаления для совместимости с базовой архитектурой."""
        return self.delete_func


class SLProcessor(APIProcessor[SL, SLProcessingConfig]):
    """Процессор для обработки данных Service Level."""

    def __init__(self, api: SlAPI):
        super().__init__(api)
        self.fetcher = ConcurrentAPIFetcher()
        # Создаем маппер полей для конвертации данных
        self.field_mapper = DataFieldMapper(
            {
                "Поступило": "received_contacts",
                "Принято": "accepted_contacts",
                "Пропущено": "missed_contacts",
                "Принято в SL": "sl_contacts",
                "% Принятых": "accepted_contacts_percent",
                "% Пропущенных": "missed_contacts_percent",
                "SL": "sl",
                "Ср. время обр.": "average_proc_time",
            }
        )

    async def fetch_data(
        self,
        config: SLProcessingConfig,
        periods: list[tuple[str, str]] = None,
        **kwargs,
    ) -> list[tuple[Any, ...]]:
        """
        Получает данные SL из API для указанных периодов параллельно.
        Если периоды не указаны, использует start_date и stop_date из конфигурации.
        """
        # Если периоды не переданы, используем даты из конфигурации
        if not periods:
            periods = [(config.start_date, config.stop_date)]

        # Получаем список очередей один раз для всех периодов
        queues_obj = await self.api.get_vq_chat_filter()

        # Валидируем результат получения очередей
        if not validate_api_result(queues_obj, "ntp_nck"):
            raise ValueError("Не удалось получить список очередей")

        queue_list = [vq for queue in queues_obj.ntp_nck.queues for vq in queue.vqList]
        self.logger.info(f"Найдено {len(queue_list)} очередей для обработки")

        # Создаем задачи для параллельной обработки
        tasks = [(start_date, stop_date) for start_date, stop_date in periods]

        async def fetch_sl_for_period(start_date, stop_date):
            try:
                self.logger.info(
                    f"Получение SL данных за период {start_date} - {stop_date}"
                )

                # Получаем SL данные для каждого периода
                sl_result = await self.api.get_sl(
                    start_date=start_date,
                    stop_date=stop_date,
                    units=config.units,
                    queues=queue_list,
                )

                # Валидируем результат SL запроса
                if not validate_api_result(sl_result, "totalData"):
                    self.logger.warning(
                        f"Не удалось получить SL данные за период {start_date}-{stop_date}"
                    )
                    return start_date, stop_date, None
                else:
                    return start_date, stop_date, sl_result

            except Exception as e:
                self.logger.error(
                    f"Ошибка получения SL данных для {start_date}-{stop_date}: {e}"
                )
                return start_date, stop_date, None

        # Выполняем параллельно с использованием семафора
        return await self.fetcher.fetch_parallel(tasks, fetch_sl_for_period)

    def process_results(
        self, results: list[tuple[Any, ...]], config: SLProcessingConfig, **kwargs
    ) -> list[SL]:
        """Обрабатывает результаты API и создает объекты SL."""
        processed_sl_objects = []

        for result in results:
            if isinstance(result, Exception):
                continue

            # ConcurrentAPIFetcher returns results in format: (task_params, actual_result)
            start_date, stop_date, sl_result = result[1]

            if not sl_result or not validate_api_result(sl_result, "totalData"):
                self.logger.warning(
                    f"Пропуск невалидного результата SL для {start_date}-{stop_date}"
                )
                continue

            # Создаем объект SL
            sl_object = SL()

            # Используем маппер для автоматического заполнения полей
            self.field_mapper.map_data_to_object(
                data_items=sl_result.totalData,
                target_obj=sl_object,
                source_field="text",
                value_field="value",
            )

            # Устанавливаем дату извлечения данных как datetime using start_date
            try:
                extraction_datetime = datetime.strptime(start_date, "%d.%m.%Y")

                # Пробуем разные возможные поля для даты извлечения
                if hasattr(sl_object, "date"):
                    sl_object.date = extraction_datetime
                elif hasattr(sl_object, "period"):
                    sl_object.period = extraction_datetime
                elif hasattr(sl_object, "extraction_period"):
                    sl_object.extraction_period = extraction_datetime
                elif hasattr(sl_object, "extraction_date"):
                    sl_object.extraction_date = extraction_datetime
                else:
                    # Если ни одно из полей не найдено, добавляем как атрибут
                    sl_object.extraction_datetime = extraction_datetime

            except Exception as e:
                self.logger.error(f"Ошибка парсинга даты {start_date}: {e}")
                continue

            processed_sl_objects.append(sl_object)
            self.logger.debug(f"Обработан SL объект для {start_date}")

        self.logger.info(f"Обработано {len(processed_sl_objects)} SL объектов")
        return processed_sl_objects

    async def save_data(
        self, data: list[SL], config: SLProcessingConfig, **kwargs
    ) -> int:
        """Сохраняет данные SL в БД используя паттерн премий/наставников."""
        if not data:
            self.logger.warning(f"No {config.update_type} data to save")
            return 0

        async with get_stats_session() as session:
            db_operator = BatchDBOperator(session)

            # Группируем данные по датам для правильного удаления (как в премиях/наставниках)
            dates_to_clean = set()
            for item in data:
                # Пробуем разные поля для извлечения даты
                extraction_datetime = None
                if hasattr(item, "date"):
                    extraction_datetime = item.date
                elif hasattr(item, "period"):
                    extraction_datetime = item.period
                elif hasattr(item, "extraction_period"):
                    extraction_datetime = item.extraction_period
                elif hasattr(item, "extraction_date"):
                    extraction_datetime = item.extraction_date
                elif hasattr(item, "extraction_datetime"):
                    extraction_datetime = item.extraction_datetime

                if extraction_datetime:
                    # Если это datetime, используем как есть; если это date, конвертируем в datetime
                    if isinstance(extraction_datetime, datetime):
                        dates_to_clean.add(extraction_datetime)
                    else:
                        dates_to_clean.add(
                            datetime.combine(extraction_datetime, datetime.min.time())
                        )

            # Удаляем старые данные за все обрабатываемые даты
            for extraction_datetime in dates_to_clean:
                await config.delete_func(session, extraction_datetime)
                self.logger.debug(
                    f"Cleared existing SL records for date {extraction_datetime.strftime('%Y-%m-%d')}"
                )

            # Вставляем новые данные
            return await db_operator.bulk_insert_with_cleanup(
                data,
                None,  # delete_func=None, так как уже очистили выше
                config.update_type,
            )
