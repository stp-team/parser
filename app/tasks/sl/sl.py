import logging

from app.api.sl import SlAPI
from app.tasks.base import (
    create_config_factory,
    log_processing_time,
)
from app.tasks.sl.utils import (
    SLProcessingConfig,
    SLProcessor,
    generate_sl_yesterday_period,
)

logger = logging.getLogger(__name__)

# Базовая конфигурация
_create_sl_config = create_config_factory(
    SLProcessingConfig,
    start_date="",
    stop_date="",
    units=[7],
    semaphore_limit=1,
)


@log_processing_time("заполнение Service Level данных")
async def fill_sl(api: SlAPI, periods: list[tuple[str, str]] = None, **kwargs) -> int:
    """
    Основная функция для получения SL данных.

    Args:
        api: Экземпляр SlAPI
        periods: Список периодов в формате [(start_date, end_date), ...] где даты в формате "DD.MM.YYYY".
                Если не указан, используется вчерашний день.
        **kwargs: Дополнительные параметры (start_date, stop_date, units)

    Returns:
        int: Количество обработанных записей
    """
    try:
        if not periods:
            # Если переданы start_date и stop_date в kwargs, используем их
            if "start_date" in kwargs and "stop_date" in kwargs:
                periods = [(kwargs["start_date"], kwargs["stop_date"])]
            else:
                # По умолчанию используем вчерашний день
                periods = generate_sl_yesterday_period()

        config = _create_sl_config(
            update_type="Service Level данные",
            start_date="",
            stop_date="",
            units=kwargs.get("units", [7]),
        )

        processor = SLProcessor(api)
        return await processor.process_with_config(config, periods=periods)

    except Exception as e:
        logger.error(f"Ошибка обновления Service Level данных: {e}")
        raise


@log_processing_time("заполнение SL данных за указанные периоды")
async def fill_sl_for_periods(api: SlAPI, periods: list[tuple[str, str]]) -> int:
    """
    Заполняет SL данные за указанные периоды.

    Args:
        api: Экземпляр SlAPI
        periods: Список периодов в формате [(start_date, end_date), ...] где даты в формате "DD.MM.YYYY"

    Returns:
        int: Количество обработанных записей
    """
    return await fill_sl(api, periods=periods)
