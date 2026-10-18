import asyncio
import logging
from datetime import date as datetime_date
from datetime import datetime

from okc_py import PremiumAPI
from okc_py.api.models.premium import HeadPremiumData, SpecialistPremiumData
from stp_database.models.Stats import HeadPremium, SpecPremium

from src.core.db import get_stats_session
from src.tasks.base import (
    ConcurrentAPIFetcher,
    PeriodHelper,
    log_processing_time,
)

# Optional API tracking
try:
    from src.services.api_tracker import track_api_call

    API_TRACKING_AVAILABLE = True
except ImportError:
    API_TRACKING_AVAILABLE = False

logger = logging.getLogger(__name__)


def get_recent_periods(
    months: int = 6,
) -> list[str]:
    """
    Get recent months as period strings
    (including current month).

    Format:
        MM.YYYY

    Example:
        If current month is 2026-08 and months=2:
        ["08.2026", "07.2026"]
    """

    previous_months = PeriodHelper.get_previous_months(
        months_count=months - 1
    )

    current_month = datetime_date.today().strftime(
        "%Y-%m"
    )

    all_months = [
        current_month,
        *previous_months,
    ]

    periods = []

    for month_str in all_months:
        start_date, _ = PeriodHelper.parse_period_string(
            month_str
        )

        period = start_date.strftime(
            "%m.%Y"
        )

        periods.append(period)

    return periods


def map_premium_row(
    row: SpecialistPremiumData | HeadPremiumData, is_head: bool
) -> SpecPremium | HeadPremium:
    """Map Pydantic model to DB model."""
    premium = (HeadPremium if is_head else SpecPremium)()
    premium.extraction_period = datetime.strptime(row.period, "%d.%m.%Y")
    premium.employee_id = row.employee_id

    # Common fields: GOK
    premium.gok = row.gok
    premium.gok_normative = row.gok_normative
    premium.gok_pers_normative = row.gok_pers_normative
    premium.gok_normative_rate = row.gok_normative_rate
    premium.gok_premium = row.gok_premium

    # Common fields: total premium
    premium.total_premium = row.total_premium

    if is_head:
        # FLR fields for heads
        premium.flr = row.flr
        premium.flr_normative = row.flr_normative
        premium.flr_pers_normative = row.flr_pers_normative
        premium.flr_normative_rate = row.flr_normative_rate
        premium.flr_premium = row.flr_premium

        # AHT fields for heads
        premium.aht = row.aht
        premium.aht_normative = row.aht_normative
        premium.aht_pers_normative = row.aht_pers_normative
        premium.aht_normative_rate = row.aht_normative_rate
        premium.aht_premium = row.aht_premium
    else:
        # Contacts count for specialists
        premium.contacts_count = row.total_chats

        # CSAT fields for specialists
        premium.csat = row.csat
        premium.csat_normative = row.csat_normative
        premium.csat_pers_normative = row.csat_pers_normative
        premium.csat_normative_rate = row.csat_normative_rate
        premium.csat_premium = row.csat_premium

        # AHT fields for specialists
        premium.aht = row.aht
        premium.aht_normative = row.aht_normative
        premium.aht_pers_normative = row.aht_pers_normative
        premium.aht_normative_rate = row.aht_normative_rate
        premium.aht_premium = row.aht_premium

        # Revenue fields for specialists
        premium.revenue = row.revenue
        premium.revenue_normative = row.revenue_normative
        premium.revenue_normative_rate = row.revenue_normative_rate

    return premium



async def fill_premium(
    api: PremiumAPI,
    divisions: list[str],
    periods: list[str],
    is_head: bool,
) -> int:
    """
    Загружает премиум специалистов/РГ.

    Для НТП с октября 2026 используем
    новый объединённый отчёт.

    Исторический премиум НТП до октября
    не перезаписываем новым форматом.

    Каждому месяцу соответствует
    отдельная транзакция.
    """

    premium_type = (
        "Head" if is_head else "Specialist"
    )

    model = (
        HeadPremium if is_head else SpecPremium
    )

    ntp_new_report_from = datetime(
        2026, 10, 1
    )

    total_saved = 0
    failed_periods = []

    for period in periods:
        logger.info(
            "[%s Premium] Начало периода %s",
            premium_type,
            period,
        )

        try:
            period_date = datetime.strptime(
                period,
                "%m.%Y",
            )

            # Не используем новый отчёт НТП
            # для исторических месяцев, по которым
            # пока не подтверждена совместимость.
            if period_date < ntp_new_report_from:
                period_divisions = [
                    division
                    for division in divisions
                    if division == "НЦК"
                ]

                logger.info(
                    "[%s Premium] Период %s: "
                    "старые данные НТП сохраняются",
                    premium_type,
                    period,
                )

            else:
                period_divisions = divisions

            if not period_divisions:
                continue

            async def fetch(
                division: str,
            ):
                if is_head:
                    return await api.get_head_premium(
                        period=period,
                        division=division,
                    )

                return await api.get_specialist_premium(
                    period=period,
                    division=division,
                )

            fetcher = ConcurrentAPIFetcher(
                semaphore_limit=4
            )

            results = await fetcher.fetch_parallel(
                [(division,) for division in period_divisions],
                fetch,
            )

            premium_objects = []

            for (division,), response in results:
                if response is None:
                    raise RuntimeError(
                        f"Не получен премиум {division} "
                        f"за {period}"
                    )

                items = (
                    response.premium
                    if is_head
                    else response.items
                )

                if not items:
                    raise RuntimeError(
                        f"Пустой премиум {division} "
                        f"за {period}"
                    )

                logger.info(
                    "[%s Premium] %s / %s: %s строк",
                    premium_type,
                    division,
                    period,
                    len(items),
                )

                # Проверка, что сервер вернул
                # именно запрошенный месяц.
                for row in items:
                    row_period = datetime.strptime(
                        row.period,
                        "%d.%m.%Y",
                    )

                    if row_period != period_date:
                        raise ValueError(
                            "Период ответа ЕОС "
                            "не совпадает с запросом: "
                            f"{row.period} != {period}"
                        )

                    premium = map_premium_row(
                        row,
                        is_head,
                    )

                    if premium.employee_id:
                        premium_objects.append(premium)

            if not premium_objects:
                raise RuntimeError(
                    f"Нет записей премиума за {period}"
                )

            # Не берём последний дубль молча:
            # он может относиться к другой
            # группе или направлению.
            unique_objects = {}

            for premium in premium_objects:
                key = (
                    premium.employee_id,
                    premium.extraction_period,
                )

                if key in unique_objects:
                    raise RuntimeError(
                        "[Premium] Повторяющийся "
                        f"employee_id за период: {key}"
                    )

                unique_objects[key] = premium

            premium_objects = list(
                unique_objects.values()
            )

            # Важно: здесь больше нет
            # DELETE всего периода.
            #
            # merge обновит существующего
            # сотрудника либо добавит нового.
            # Другие записи и старые месяцы
            # не затрагиваются.
            async with get_stats_session() as session:
                async with session.begin():

                    for premium in premium_objects:
                        await session.merge(premium)

                    await session.flush()

            total_saved += len(premium_objects)

            logger.info(
                "[%s Premium] %s: сохранено %s строк",
                premium_type,
                period,
                len(premium_objects),
            )

        except Exception as exc:
            logger.exception(
                "[%s Premium] Ошибка за %s: %s",
                premium_type,
                period,
                exc,
            )

            failed_periods.append(period)

    # Периоды обрабатываются независимо.
    # Если сентябрь недоступен, октябрь всё
    # равно может успешно сохраниться.
    if failed_periods:
        raise RuntimeError(
            f"[{premium_type} Premium] "
            "Не удалось обработать периоды: "
            f"{failed_periods}. "
            f"Успешно сохранено строк: {total_saved}"
        )

    return total_saved

@log_processing_time("Specialist Premium data processing")
async def fill_specialists_premium(api: PremiumAPI, period: str | None = None) -> int:
    """
    Fill specialist premium data for last 2 months (current + previous).

    This ensures we get updates for:
    - Current month (partial data that may be updated)
    - Previous month (complete data that may have late updates)

    Args:
        api: PremiumAPI instance
        period: Optional specific period (format: MM.YYYY). If None, uses last 2 months.

    Returns:
        Number of records saved
    """
    # Новую схему премиума НТП используем
    # с октября 2026 года.
    premium_start = datetime(2026, 10, 1)

    if period:
        # Явный запрос периода оставляем без изменений.
        periods = [period]
    else:
        # Автоматически обновляем только месяцы,
        # начиная с перехода на объединённую НТП.
        periods = [
            p
            for p in get_recent_periods(2)
            if datetime.strptime(p, "%m.%Y") >= premium_start
        ]

    logger.info(
        "[Specialist Premium] Периоды для обновления: %s",
        periods,
    )

    count = await fill_premium(
        api=api,
        divisions=["НТП", "НЦК"],
        periods=periods,
        is_head=False,
    )
    logger.info(f"[Specialist Premium] Update completed: {count} records")
    return count


@log_processing_time("Head Premium data processing")
async def fill_heads_premium(api: PremiumAPI, period: str | None = None) -> int:
    """
    Fill head premium data for last 2 months (current + previous).

    This ensures we get updates for:
    - Current month (partial data that may be updated)
    - Previous month (complete data that may have late updates)

    Args:
        api: PremiumAPI instance
        period: Optional specific period (format: MM.YYYY). If None, uses last 2 months.

    Returns:
        Number of records saved
    """
    periods = [period] if period else get_recent_periods(2)
    logger.info(f"[Head Premium] Updating periods: {periods}")
    count = await fill_premium(api, ["НТП", "НЦК"], periods, is_head=True)
    logger.info(f"[Head Premium] Update completed: {count} records")
    return count


@log_processing_time("All Premium data processing (last 6 months)")
async def fill_all_premium_last_6_months(api: PremiumAPI) -> None:
    """Fill all premium data for last 6 months."""
    periods = get_recent_periods(6)
    logger.info(
        f"Starting full premium update for last 6 months: {len(periods)} periods"
    )
    results = await asyncio.gather(
        fill_premium(api, ["НТП", "НЦК"], periods, is_head=True),
        fill_premium(api, ["НТП", "НЦК"], periods, is_head=False),
    )
    total = sum(results)
    logger.info(f"Full premium update completed: {total} total records")
