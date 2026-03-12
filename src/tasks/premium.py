import asyncio
import logging
from datetime import date as datetime_date
from datetime import datetime

from okc_py import PremiumAPI
from okc_py.api.models.premium import HeadPremiumData, SpecialistPremiumData
from sqlalchemy import delete
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


def get_recent_periods(months: int = 6) -> list[str]:
    """
    Get recent months as period strings (including current month).

    Args:
        months: Number of months to fetch (including current month)

    Returns:
        List of period strings in DD.MM.YYYY format

    Example:
        If current month is 2026-02 and months=2:
        Returns ["01.02.2026", "01.01.2026"] (current and previous month)
    """
    previous_months = PeriodHelper.get_previous_months(months_count=months - 1)
    current_month = datetime_date.today().strftime("%Y-%m")
    all_months = [current_month] + previous_months

    periods = []
    for month_str in all_months:
        start_date, _ = PeriodHelper.parse_period_string(month_str)
        period = PeriodHelper.format_date_for_api(start_date, "DD.MM.YYYY")
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

    return premium


async def fill_premium(
    api: PremiumAPI,
    divisions: list[str],
    periods: list[str],
    is_head: bool,
) -> int:
    """Fill premium data."""
    premium_type = "Head" if is_head else "Specialist"
    logger.info(
        f"[{premium_type} Premium] Starting premium data update for {len(periods)} periods x {len(divisions)} divisions"
    )

    async def fetch(period: str, division: str):
        endpoint = "/api/premium/heads" if is_head else "/api/premium/specialists"
        if API_TRACKING_AVAILABLE:
            track_api_call(endpoint, "GET")
        if is_head:
            return await api.get_head_premium(period, division)
        return await api.get_specialist_premium(period, division)

    tasks = [(p, d) for p in periods for d in divisions]
    logger.info(f"[{premium_type} Premium] Fetching data for {len(tasks)} API calls")

    fetcher = ConcurrentAPIFetcher(semaphore_limit=15)
    results = await fetcher.fetch_parallel(tasks, fetch)

    premium_objects = []
    for (_period, _division), result in results:
        if result is None:
            continue

        items = result.premium if is_head else result.items
        for row in items:
            premium = map_premium_row(row, is_head)
            if premium.employee_id:
                premium_objects.append(premium)

    if not premium_objects:
        logger.warning(f"[{premium_type} Premium] No premium data retrieved from API")
        return 0

    # API can return the same employee for the same period from different divisions.
    # Keep only one record per DB primary key to avoid IntegrityError on bulk insert.
    unique_premium_objects: dict[tuple[int, datetime], SpecPremium | HeadPremium] = {}
    duplicates_count = 0
    for premium in premium_objects:
        key = (premium.employee_id, premium.extraction_period)
        if key in unique_premium_objects:
            duplicates_count += 1
        unique_premium_objects[key] = premium

    if duplicates_count:
        logger.warning(
            f"[{premium_type} Premium] Found {duplicates_count} duplicate records "
            "by (employee_id, extraction_period); keeping the last occurrence"
        )

    premium_objects = list(unique_premium_objects.values())

    logger.info(
        f"[{premium_type} Premium] Mapped {len(premium_objects)} premium records"
    )

    async with get_stats_session() as session:
        model = HeadPremium if is_head else SpecPremium

        # Get unique periods that will be updated
        unique_periods = {p.extraction_period for p in premium_objects}
        periods_str = ", ".join(str(p.date()) for p in sorted(unique_periods))
        logger.info(
            f"[{premium_type} Premium] Deleting old data for periods: {periods_str}"
        )

        # Delete rows where employee_id is None (data quality cleanup)
        await session.execute(delete(model).where(model.employee_id.is_(None)))

        # Delete old data for the periods we're about to update
        # This ensures we get fresh data for the last 2 months (current + previous)
        for _period in unique_periods:
            deleted = await session.execute(
                delete(model).where(model.extraction_period == _period)
            )
            logger.debug(
                f"[{premium_type} Premium] Deleted {deleted.rowcount} rows for period {_period.date()}"
            )

        # Insert fresh data
        session.add_all(premium_objects)
        await session.commit()
        logger.info(
            f"[{premium_type} Premium] Successfully saved {len(premium_objects)} records"
        )

    logger.info(
        f"[{premium_type} Premium] Completed: {len(premium_objects)} records saved"
    )
    return len(premium_objects)


@log_processing_time("Specialist Premium data processing")
async def fill_specialists_premium(api: PremiumAPI, period: str | None = None) -> int:
    """
    Fill specialist premium data for last 2 months (current + previous).

    This ensures we get updates for:
    - Current month (partial data that may be updated)
    - Previous month (complete data that may have late updates)

    Args:
        api: PremiumAPI instance
        period: Optional specific period (format: DD.MM.YYYY). If None, uses last 2 months.

    Returns:
        Number of records saved
    """
    from src.services.constants import unites

    periods = [period] if period else get_recent_periods(2)
    logger.info(f"[Specialist Premium] Updating periods: {periods}")
    count = await fill_premium(api, unites, periods, is_head=False)
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
        period: Optional specific period (format: DD.MM.YYYY). If None, uses last 2 months.

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
