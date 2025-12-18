"""
Модуль для заполнения и обновления данных Service Level (SL).

Рефакторированная версия с использованием базовых классов:
- Использует APIProcessor для стандартного ETL pipeline
- DataFieldMapper для автоматического маппинга полей
- BatchDBOperator для транзакционных операций
- Конфигурируемые параметры вместо хардкода
- Валидация API результатов и обработка ошибок
"""

import logging
from dataclasses import dataclass
from typing import Any

from stp_database.models.Stats.sl import SL

from app.api.sl import SlAPI
from app.core.db import get_stats_session
from app.tasks.base import (
    APIProcessor,
    BatchDBOperator,
    DataFieldMapper,
    PeriodHelper,
    ProcessingConfig,
    log_processing_time,
    validate_api_result,
)

logger = logging.getLogger(__name__)


@dataclass
class SLProcessingConfig(ProcessingConfig):
    """Конфигурация для обработки данных Service Level."""

    start_date: str  # Формат "DD.MM.YYYY"
    stop_date: str  # Формат "DD.MM.YYYY"
    units: list[int] = None
    semaphore_limit: int = 10

    def __post_init__(self):
        if self.units is None:
            self.units = [7]  # Значение по умолчанию

    def get_delete_func(self):
        """SL обычно перезаписывается, удаление не требуется."""
        return None


class SLProcessor(APIProcessor[SL, SLProcessingConfig]):
    """Процессор для обработки данных Service Level."""

    def __init__(self, api: SlAPI):
        super().__init__(api)
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
        self, config: SLProcessingConfig, **kwargs
    ) -> list[tuple[Any, ...]]:
        """Получает данные SL из API."""
        self.logger.info(
            f"Получение SL данных за период {config.start_date} - {config.stop_date}"
        )

        # Получаем список очередей
        queues_obj = await self.api.get_vq_chat_filter()

        # Валидируем результат получения очередей
        if not validate_api_result(queues_obj, "ntp_nck"):
            raise ValueError("Не удалось получить список очередей")

        queue_list = [vq for queue in queues_obj.ntp_nck.queues for vq in queue.vqList]

        self.logger.info(f"Найдено {len(queue_list)} очередей для обработки")

        # Получаем SL данные
        sl_result = await self.api.get_sl(
            start_date=config.start_date,
            stop_date=config.stop_date,
            units=config.units,
            queues=queue_list,
        )

        # Валидируем результат SL запроса
        if not validate_api_result(sl_result, "totalData"):
            raise ValueError(
                f"Не удалось получить SL данные за период {config.start_date}-{config.stop_date}"
            )

        # Возвращаем данные в стандартном формате (task_params, api_result)
        return [(config, sl_result)]

    def process_results(
        self, results: list[tuple[Any, ...]], config: SLProcessingConfig, **kwargs
    ) -> list[SL]:
        """Обрабатывает результаты API и создает объекты SL."""
        processed_sl_objects = []

        for _task_params, sl_result in results:
            if not validate_api_result(sl_result, "totalData"):
                self.logger.warning("Пропуск невалидного результата SL")
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

            processed_sl_objects.append(sl_object)

        self.logger.info(f"Обработано {len(processed_sl_objects)} SL объектов")
        return processed_sl_objects

    async def save_data(
        self, data: list[SL], config: SLProcessingConfig, **kwargs
    ) -> int:
        """Сохраняет данные SL в базу данных."""
        if not data:
            self.logger.warning("Нет данных SL для сохранения")
            return 0

        async with get_stats_session() as session:
            db_operator = BatchDBOperator(session)

            # SL обычно не требует удаления старых данных, просто добавляем новые
            return await db_operator.bulk_insert_with_cleanup(
                data_list=data,
                delete_func=None,  # Без удаления
                operation_name=config.update_type,
            )


# Конфигурация по умолчанию - данные за текущий день
DEFAULT_SL_CONFIG = SLProcessingConfig(
    update_type="заполнение Service Level данных",
    start_date=PeriodHelper.format_date_for_api(
        PeriodHelper.get_month_boundaries(
            PeriodHelper.parse_period_string("2025-12")[0]
        )[0],
        "DD.MM.YYYY",
    ),
    stop_date=PeriodHelper.format_date_for_api(
        PeriodHelper.get_month_boundaries(
            PeriodHelper.parse_period_string("2025-12")[0]
        )[1],
        "DD.MM.YYYY",
    ),
    units=[7],
    semaphore_limit=1,
)


# =====================================================================================
# ПУБЛИЧНЫЕ ФУНКЦИИ
# =====================================================================================


@log_processing_time("заполнение Service Level данных")
async def fill_sl(api: SlAPI, **kwargs) -> None:
    """
    Получает значения SL и заполняет таблицы.

    Args:
        api: Экземпляр SlAPI
        start_date: Начальная дата (опционально, DD.MM.YYYY)
        stop_date: Конечная дата (опционально, DD.MM.YYYY)
        units: Список unit ID (опционально)

    Examples:
        >>> await fill_sl(api)  # Использует текущий день
        >>> await fill_sl(api, start_date="01.12.2024", stop_date="31.12.2024")
    """
    # Создаем конфигурацию с переданными параметрами или по умолчанию
    config = SLProcessingConfig(
        update_type="заполнение Service Level данных",
        start_date=kwargs.get("start_date", DEFAULT_SL_CONFIG.start_date),
        stop_date=kwargs.get("stop_date", DEFAULT_SL_CONFIG.stop_date),
        units=kwargs.get("units", DEFAULT_SL_CONFIG.units),
        semaphore_limit=1,
    )

    processor = SLProcessor(api)
    await processor.process_with_config(config)
