import asyncio
import logging
from typing import Any

from app.tasks.base import (
    log_processing_time,
)
from app.tasks.kpi.utils import KPIProcessor, create_kpi_config

logger = logging.getLogger(__name__)


@log_processing_time("заполнение дневных KPI")
async def fill_day_kpi(api: Any) -> None:
    """Заполняет дневную таблицу KPI."""
    processor = KPIProcessor(api)
    config = create_kpi_config("day")
    await processor.process_with_config(config)


@log_processing_time("заполнение недельных KPI")
async def fill_week_kpi(api: Any) -> None:
    """Заполняет недельную таблицу KPI."""
    processor = KPIProcessor(api)
    config = create_kpi_config("week")
    await processor.process_with_config(config)


@log_processing_time("заполнение месячных KPI")
async def fill_month_kpi(api: Any) -> None:
    """Заполняет месячную таблицу KPI."""
    processor = KPIProcessor(api)
    config = create_kpi_config("month")
    await processor.process_with_config(config)


@log_processing_time("заполнение всех KPI")
async def fill_kpi(api: Any) -> None:
    """
    Основная функция для вызова в планировщике.

    Запускает сбор данных для всех периодов параллельно.
    """
    # Создаем единственный экземпляр процессора
    processor = KPIProcessor(api)

    # Создаем конфигурации для всех периодов
    configs = [
        create_kpi_config("day"),
        create_kpi_config("week"),
        create_kpi_config("month"),
    ]

    # Запускаем обработку всех периодов параллельно
    try:
        await asyncio.gather(
            *[processor.process_with_config(config) for config in configs],
            return_exceptions=True,
        )
    except Exception as e:
        logger.error(f"Ошибка получения KPI: {e}")
        raise
