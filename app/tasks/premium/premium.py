import asyncio
import logging

from okc_py.repos import PremiumAPI
from stp_database.models.Stats import HeadPremium, SpecPremium

from app.services.constants import unites
from app.tasks.base import (
    log_processing_time,
)
from app.tasks.premium.utils import (
    HEAD_DIVISIONS,
    PremiumDataConverter,
    PremiumDBManager,
    PremiumProcessingConfig,
    PremiumProcessor,
    generate_premium_periods,
)

logger = logging.getLogger(__name__)


# Конфигурации для разных типов премий
SPECIALIST_PREMIUM_CONFIG = PremiumProcessingConfig(
    update_type="Премии специалистов",
    divisions=unites,
    api_method="get_specialist_premium",
    db_model_class=SpecPremium,
    converter_func=PremiumDataConverter.convert_spec_premium_data,
    response_attr="items",
    delete_func=PremiumDBManager.delete_old_spec_data,
)

HEAD_PREMIUM_CONFIG = PremiumProcessingConfig(
    update_type="Премии руководителей",
    divisions=HEAD_DIVISIONS,
    api_method="get_head_premium",
    db_model_class=HeadPremium,
    converter_func=PremiumDataConverter.convert_head_premium_data,
    response_attr="premium",
    delete_func=PremiumDBManager.delete_old_head_data,
)


@log_processing_time("заполнение премий специалистов")
async def fill_specialists_premium(api: Any, period: str | None = None) -> None:
    """
    Получает данные о премиях специалистов.

    Args:
        api: Экземпляр API
        period: Период в формате "dd.mm.yyyy". Если не указан, используются последние 2 месяца.
    """
    processor = PremiumProcessor(api)

    periods = [period] if period else generate_premium_periods(2)

    await processor.process_with_config(SPECIALIST_PREMIUM_CONFIG, periods=periods)


@log_processing_time("заполнение премий руководителей")
async def fill_heads_premium(api: Any, period: str | None = None) -> None:
    """
    Получает данные о премиях руководителей.

    Args:
        api: Экземпляр API
        period: Период в формате "dd.mm.yyyy". Если не указан, используются последние 2 месяца.
    """
    processor = PremiumProcessor(api)

    periods = [period] if period else generate_premium_periods(2)

    await processor.process_with_config(HEAD_PREMIUM_CONFIG, periods=periods)


@log_processing_time("заполнение премий специалистов за несколько периодов")
async def fill_specialists_premium_multiple_periods(
    api: Any, periods: list[str] | None = None
) -> None:
    """
    Получает данные о премиях специалистов за несколько периодов.

    Args:
        api: Экземпляр API
        periods: Список периодов в формате "dd.mm.yyyy".
                 Если не указан, используются последние 6 месяцев.
    """
    processor = PremiumProcessor(api)

    if periods is None:
        periods = generate_premium_periods()

    await processor.process_with_config(SPECIALIST_PREMIUM_CONFIG, periods=periods)


@log_processing_time("заполнение премий руководителей за несколько периодов")
async def fill_heads_premium_multiple_periods(
    api: Any, periods: list[str] | None = None
) -> None:
    """
    Получает данные о премиях руководителей за несколько периодов.

    Args:
        api: Экземпляр API
        periods: Список периодов в формате "dd.mm.yyyy".
                 Если не указан, используются последние 6 месяцев.
    """
    processor = PremiumProcessor(api)

    if periods is None:
        periods = generate_premium_periods()

    await processor.process_with_config(HEAD_PREMIUM_CONFIG, periods=periods)


@log_processing_time("заполнение всех премий за последние 6 месяцев")
async def fill_all_premium_last_6_months(api: Any) -> None:
    """
    Получает данные о премиях всех сотрудников за последние 6 месяцев.

    Функция собирает данные о премиях специалистов и руководителей за последние 6 месяцев
    и сохраняет их в соответствующие таблицы БД параллельно.

    Args:
        api: Экземпляр API для получения данных о премиях
    """
    periods = generate_premium_periods()

    logger.info(f"Начинаем сбор всех премий за последние 6 месяцев: {periods}")

    # Запускаем сбор данных для специалистов и руководителей параллельно
    try:
        await asyncio.gather(
            fill_specialists_premium_multiple_periods(api, periods),
            fill_heads_premium_multiple_periods(api, periods),
            return_exceptions=True,
        )
    except Exception as e:
        logger.error(f"Ошибка при сборе премий: {e}")
        raise
