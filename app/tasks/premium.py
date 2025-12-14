import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from time import perf_counter
from typing import Any, Protocol

from sqlalchemy import Delete
from sqlalchemy.ext.asyncio import AsyncSession
from stp_database.models.KPI import HeadPremium, SpecPremium

from app.api.premium import PremiumAPI
from app.core.db import get_stats_session
from app.services.constants import unites
from app.services.helpers import get_current_month_first_day

logger = logging.getLogger(__name__)

# Constants
HEAD_DIVISIONS = ["НТП", "НЦК"]
MONTHS_IN_HISTORY = 6

# Type definitions
DBModel = SpecPremium | HeadPremium


@dataclass
class PremiumConfig:
    """Configuration for premium data processing."""

    divisions: list[str]
    api_method: str
    db_model_class: type[DBModel]
    converter_func: Callable[[Any, datetime], DBModel]
    response_attr: str
    delete_func: Callable[[AsyncSession, datetime], Any]


class APIResponse(Protocol):
    """Protocol for API response objects."""

    def __getattr__(self, name: str) -> Any: ...


def generate_last_6_months_periods() -> list[str]:
    """Генерирует список периодов за последние 6 месяцев.

    Каждый период начинается с 1 числа месяца в формате "dd.mm.yyyy".
    Возвращает периоды в обратном хронологическом порядке (от новых к старым).

    Returns:
        Список периодов в формате ["01.12.2025", "01.11.2025", ...]
    """
    periods = []
    current_date = datetime.now()

    for i in range(MONTHS_IN_HISTORY):
        year = current_date.year
        month = current_date.month - i

        while month <= 0:
            month += 12
            year -= 1

        period = f"01.{month:02d}.{year}"
        periods.append(period)

    return periods


def _convert_spec_premium_data(
    row: APIResponse, extraction_period: datetime
) -> SpecPremium:
    """Конвертирует данные API в модель SpecPremium."""
    return SpecPremium(
        fullname=row.user_fullname,
        contacts_count=row.total_contacts,
        csi=row.csi,
        csi_normative=row.csi_normative,
        csi_normative_rate=row.csi_normative_rate,
        csi_premium=row.csi_premium,
        csi_response=row.csi_response,
        csi_response_normative=row.csi_response_normative,
        csi_response_normative_rate=row.csi_response_normative_rate,
        flr=row.flr,
        flr_normative=row.flr_normative,
        flr_normative_rate=row.flr_normative_rate,
        flr_premium=row.flr_premium,
        gok=row.gok,
        gok_normative=row.gok_normative,
        gok_normative_rate=row.gok_normative_rate,
        gok_premium=row.gok_premium,
        target=row.target,
        target_type=row.target_type,
        target_normative_first=row.target_normative_first,
        target_normative_second=row.target_normative_second,
        target_normative_rate_first=row.target_normative_rate_first,
        target_normative_rate_second=row.target_normative_rate_second,
        target_premium=row.target_premium,
        pers_target_manual=row.pers_target_manual,
        discipline_premium=row.discipline_premium,
        tests_premium=row.tests_premium,
        thanks_premium=row.thanks_premium,
        tutors_premium=row.tutors_premium,
        head_adjust_premium=row.head_adjust_premium,
        total_premium=row.total_premium,
        extraction_period=extraction_period,
    )


def _convert_head_premium_data(
    row: APIResponse, extraction_period: datetime
) -> HeadPremium:
    """Конвертирует данные API в модель HeadPremium."""
    return HeadPremium(
        fullname=row.user_fullname,
        flr=row.flr,
        flr_normative=row.flr_normative,
        flr_normative_rate=row.flr_normative_rate,
        flr_premium=row.flr_premium,
        gok=row.gok,
        gok_normative=row.gok_normative,
        gok_normative_rate=row.gok_normative_rate,
        gok_premium=row.gok_premium,
        target=row.target,
        target_type=row.target_type,
        target_normative_first=row.target_normative_first,
        target_normative_second=row.target_normative_second,
        target_normative_rate_first=row.target_normative_rate_first,
        target_normative_rate_second=row.target_normative_rate_second,
        target_premium=row.target_premium,
        pers_target_manual=row.pers_target_manual,
        sl=row.sl,
        sl_normative_first=row.sl_normative_first,
        sl_normative_second=row.sl_normative_second,
        sl_normative_rate_first=row.sl_normative_rate_first,
        sl_normative_rate_second=row.sl_normative_rate_second,
        sl_premium=row.sl_premium,
        head_adjust_premium=row.head_adjust_premium,
        total_premium=row.total_premium,
        extraction_period=extraction_period,
    )


async def _delete_old_premium_data(
    session: AsyncSession, model_class: type[DBModel], extraction_period: datetime
) -> None:
    """Универсальная функция удаления старых данных премий."""
    query = Delete(model_class).where(
        model_class.extraction_period == extraction_period
    )
    await session.execute(query)


async def delete_old_spec_data(
    session: AsyncSession, extraction_period: datetime
) -> None:
    """Удаляет данные SpecPremium за указанный период."""
    await _delete_old_premium_data(session, SpecPremium, extraction_period)


async def delete_old_head_data(
    session: AsyncSession, extraction_period: datetime
) -> None:
    """Удаляет данные HeadPremium за указанный период."""
    await _delete_old_premium_data(session, HeadPremium, extraction_period)


async def _bulk_insert_premium_data(
    premium_list: list[DBModel],
    model_name: str,
    delete_func: Callable[[AsyncSession, datetime], Any],
) -> None:
    """Универсальная функция для массовой вставки данных премий.

    Args:
        premium_list: Список объектов модели премий
        model_name: Название модели для логирования
        delete_func: Функция для удаления старых данных
    """
    if not premium_list:
        logger.warning(f"No {model_name} premium data to insert")
        return

    async with get_stats_session() as session:
        # Получаем период из первого элемента (все элементы должны быть за один период)
        extraction_period = premium_list[0].extraction_period

        # Удаляем старые данные только за этот период
        await delete_func(session, extraction_period)

        # Вставляем записи одной операцией
        session.add_all(premium_list)
        await session.commit()

        logger.info(
            f"Inserted {len(premium_list)} records into {model_name} table "
            f"for period {extraction_period.strftime('%d.%m.%Y')}"
        )


async def _bulk_insert_premium(premium_list: list[SpecPremium]) -> None:
    """Очищает данные за период и вставляет записи премий специалистов."""
    await _bulk_insert_premium_data(premium_list, "SpecPremium", delete_old_spec_data)


async def _bulk_insert_head_premium(premium_list: list[HeadPremium]) -> None:
    """Очищает данные за период и вставляет записи премий руководителей."""
    await _bulk_insert_premium_data(premium_list, "HeadPremium", delete_old_head_data)


async def _fetch_premium_data_for_divisions(
    api: PremiumAPI, period: str, divisions: list[str], api_method_name: str
) -> list[tuple[str, Any]]:
    """Получает данные премий для списка подразделений параллельно.

    Args:
        api: Экземпляр API
        period: Период в формате "dd.mm.yyyy"
        divisions: Список подразделений
        api_method_name: Название метода API для вызова

    Returns:
        Список кортежей (division, result) с результатами API
    """
    # Создаем параллельные задачи для всех подразделений
    tasks = []
    for division in divisions:
        api_method = getattr(api, api_method_name)
        coro = api_method(period=period, division=division)
        tasks.append((division, coro))

    # Выполняем вызовы API одновременно
    results = await asyncio.gather(*(t[1] for t in tasks), return_exceptions=True)

    # Обрабатываем ошибки
    processed_results = []
    for i, result in enumerate(results):
        division = tasks[i][0]
        if isinstance(result, Exception):
            logger.error(
                f"Error getting premium for division {division}, period {period}: {result}"
            )
            processed_results.append((division, None))
        else:
            processed_results.append((division, result))

    return processed_results


def _process_api_response_to_models(
    results: list[tuple[str, Any]],
    period: str,
    response_attr: str,
    converter_func: Callable[[Any, datetime], DBModel],
) -> list[Any]:
    """Обрабатывает результаты API и конвертирует их в модели БД.

    Args:
        results: Список результатов API
        period: Период для парсинга даты
        response_attr: Атрибут ответа, содержащий данные
        converter_func: Функция конвертации строки API в модель БД

    Returns:
        Список моделей БД
    """
    premium_list = []

    for division, result in results:
        if not result or not hasattr(result, response_attr):
            logger.warning(
                f"No premium data returned for division: {division}, period: {period}"
            )
            continue

        data_items = getattr(result, response_attr)
        for row in data_items:
            # Используем период из строки для совместимости
            row_period = datetime.strptime(row.period, "%d.%m.%Y")
            premium_list.append(converter_func(row, row_period))

    return premium_list


async def _fill_premium_for_period(
    api: PremiumAPI, period: str, config: PremiumConfig
) -> None:
    """Универсальная функция заполнения данных премий за один период.

    Args:
        api: Экземпляр API
        period: Период в формате "dd.mm.yyyy"
        config: Конфигурация для обработки премий
    """
    timer_start = perf_counter()
    model_name = config.db_model_class.__name__
    logger.info(f"Starting {model_name} premium fill for period {period}")

    # Получаем данные из API
    results = await _fetch_premium_data_for_divisions(
        api, period, config.divisions, config.api_method
    )

    # Конвертируем в модели БД
    premium_list = _process_api_response_to_models(
        results, period, config.response_attr, config.converter_func
    )

    if not premium_list:
        logger.warning(f"No {model_name} premium data to insert for period {period}")
        return

    # Записываем данные в БД
    await _bulk_insert_premium_data(premium_list, model_name, config.delete_func)

    timer_stop = perf_counter()
    logger.info(
        f"Finished {model_name} premium fill for period {period}, "
        f"taken {timer_stop - timer_start:.2f} seconds"
    )


async def _fill_premium_for_multiple_periods(
    api: PremiumAPI, periods: list[str], config: PremiumConfig
) -> None:
    """Универсальная функция заполнения данных премий за несколько периодов.

    Args:
        api: Экземпляр API
        periods: Список периодов в формате "dd.mm.yyyy"
        config: Конфигурация для обработки премий
    """
    timer_start = perf_counter()
    model_name = config.db_model_class.__name__
    logger.info(
        f"Starting {model_name} premium fill for {len(periods)} periods: {periods}"
    )

    all_premium_list = []

    for period in periods:
        logger.info(f"Processing {model_name} premium for period: {period}")

        # Получаем данные из API
        results = await _fetch_premium_data_for_divisions(
            api, period, config.divisions, config.api_method
        )

        # Конвертируем в модели БД и добавляем к общему списку
        period_premium_list = _process_api_response_to_models(
            results, period, config.response_attr, config.converter_func
        )
        all_premium_list.extend(period_premium_list)

    if not all_premium_list:
        logger.warning(f"No {model_name} premium data to insert for all periods")
        return

    # Записываем все данные в БД одной операцией
    await _bulk_insert_premium_data(all_premium_list, model_name, config.delete_func)

    timer_stop = perf_counter()
    logger.info(
        f"Finished {model_name} premium fill for {len(periods)} periods, "
        f"inserted {len(all_premium_list)} records, taken {timer_stop - timer_start:.2f} seconds"
    )


# Конфигурации для разных типов премий
SPECIALIST_PREMIUM_CONFIG = PremiumConfig(
    divisions=unites,
    api_method="get_specialist_premium",
    db_model_class=SpecPremium,
    converter_func=_convert_spec_premium_data,
    response_attr="items",
    delete_func=delete_old_spec_data,
)

HEAD_PREMIUM_CONFIG = PremiumConfig(
    divisions=HEAD_DIVISIONS,
    api_method="get_head_premium",
    db_model_class=HeadPremium,
    converter_func=_convert_head_premium_data,
    response_attr="premium",
    delete_func=delete_old_head_data,
)


# Публичные функции API
async def fill_specialists_premium(api: PremiumAPI, period: str | None = None) -> None:
    """Получает данные о премиях для всех специалистов по всем подразделениям.

    Args:
        api: Экземпляр API
        period: Период в формате "dd.mm.yyyy". Если не указан, используется первое число текущего месяца.
    """
    if period is None:
        period = get_current_month_first_day()
    await _fill_premium_for_period(api, period, SPECIALIST_PREMIUM_CONFIG)


async def fill_heads_premium(api: PremiumAPI, period: str | None = None) -> None:
    """Получает данные о премиях для всех руководителей по всем подразделениям.

    Args:
        api: Экземпляр API
        period: Период в формате "dd.mm.yyyy". Если не указан, используется первое число текущего месяца.
    """
    if period is None:
        period = get_current_month_first_day()
    await _fill_premium_for_period(api, period, HEAD_PREMIUM_CONFIG)


async def fill_specialists_premium_multiple_periods(
    api: PremiumAPI, periods: list[str] | None = None
) -> None:
    """Получает данные о премиях специалистов за несколько периодов.

    Args:
        api: Экземпляр API
        periods: Список периодов в формате "dd.mm.yyyy".
                 Если не указан, используются последние 6 месяцев.
    """
    if periods is None:
        periods = generate_last_6_months_periods()

    await _fill_premium_for_multiple_periods(api, periods, SPECIALIST_PREMIUM_CONFIG)


async def fill_heads_premium_multiple_periods(
    api: PremiumAPI, periods: list[str] | None = None
) -> None:
    """Получает данные о премиях руководителей за несколько периодов.

    Args:
        api: Экземпляр API
        periods: Список периодов в формате "dd.mm.yyyy".
                 Если не указан, используются последние 6 месяцев.
    """
    if periods is None:
        periods = generate_last_6_months_periods()

    await _fill_premium_for_multiple_periods(api, periods, HEAD_PREMIUM_CONFIG)


async def fill_all_premium_last_6_months(api: PremiumAPI) -> None:
    """Получает данные о премиях всех сотрудников за последние 6 месяцев.

    Функция собирает данные о премиях специалистов и руководителей за последние 6 месяцев
    (начиная с 1 числа каждого месяца) и сохраняет их в соответствующие таблицы БД.

    Args:
        api: Экземпляр API для получения данных о премиях
    """
    timer_start = perf_counter()
    periods = generate_last_6_months_periods()

    logger.info(f"Starting full premium collection for last 6 months: {periods}")

    # Запускаем сбор данных для специалистов и руководителей параллельно
    try:
        await asyncio.gather(
            fill_specialists_premium_multiple_periods(api, periods),
            fill_heads_premium_multiple_periods(api, periods),
            return_exceptions=True,
        )
    except Exception as e:
        logger.error(f"Error during premium collection: {e}")
        raise

    timer_stop = perf_counter()
    logger.info(
        f"Completed full premium collection for last 6 months, "
        f"taken {timer_stop - timer_start:.2f} seconds"
    )
