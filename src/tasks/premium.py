import asyncio
import logging
from datetime import datetime

from okc_py import PremiumAPI
from okc_py.api.models.premium import HeadPremiumData, SpecialistPremiumData
from sqlalchemy import Delete
from stp_database.models.Stats import HeadPremium, SpecPremium

from src.core.db import get_stats_session
from src.tasks.base import PeriodHelper

logger = logging.getLogger(__name__)


def get_recent_periods(months: int = 6) -> list[str]:
    """Get recent months as period strings (including current month)."""
    # Get previous months (months_count - 1 to leave room for current month)
    previous_months = PeriodHelper.get_previous_months(months_count=months - 1)

    # Add current month
    from datetime import date

    current_month = date.today().strftime("%Y-%m")
    all_months = [current_month] + previous_months

    periods = []
    for month_str in all_months:
        start_date, _ = PeriodHelper.parse_period_string(month_str)
        period = PeriodHelper.format_date_for_api(start_date, "DD.MM.YYYY")
        periods.append(period)
    return periods


def map_spec_premium(row: SpecialistPremiumData) -> SpecPremium:
    """Map Pydantic model to DB model - direct field access."""
    premium = SpecPremium()
    premium.extraction_period = datetime.strptime(row.period, "%d.%m.%Y")
    premium.fullname = row.user_fullname
    premium.contacts_count = row.total_contacts
    premium.csi = row.csi
    premium.csi_normative = row.csi_normative
    premium.csi_normative_rate = row.csi_normative_rate
    premium.csi_premium = row.csi_premium
    premium.csi_response = row.csi_response
    premium.csi_response_normative = row.csi_response_normative
    premium.csi_response_normative_rate = row.csi_response_normative_rate
    premium.discipline_premium = row.discipline_premium
    premium.tests_premium = row.tests_premium
    premium.thanks_premium = row.thanks_premium
    premium.tutors_premium = (
        int(row.tutors_premium) if row.tutors_premium is not None else None
    )
    premium.flr = row.flr
    premium.flr_normative = row.flr_normative
    premium.flr_normative_rate = row.flr_normative_rate
    premium.flr_premium = row.flr_premium
    premium.gok = row.gok
    premium.gok_normative = row.gok_normative
    premium.gok_normative_rate = row.gok_normative_rate
    premium.gok_premium = row.gok_premium
    premium.target = row.target
    premium.target_type = row.target_type
    premium.target_normative_first = row.target_normative_first
    premium.target_normative_second = row.target_normative_second
    premium.target_normative_rate_first = row.target_normative_rate_first
    premium.target_normative_rate_second = row.target_normative_rate_second
    premium.target_premium = row.target_premium
    premium.pers_target_manual = row.pers_target_manual
    premium.head_adjust_premium = row.head_adjust_premium
    premium.total_premium = (
        int(row.total_premium) if row.total_premium is not None else None
    )
    return premium


def map_head_premium(row: HeadPremiumData) -> HeadPremium:
    """Map Pydantic model to DB model - direct field access."""
    premium = HeadPremium()
    premium.extraction_period = datetime.strptime(row.period, "%d.%m.%Y")
    premium.fullname = row.user_fullname
    premium.sl = row.sl
    premium.sl_normative_first = row.sl_normative_first
    premium.sl_normative_second = row.sl_normative_second
    premium.sl_normative_rate_first = row.sl_normative_rate_first
    premium.sl_normative_rate_second = row.sl_normative_rate_second
    premium.sl_premium = row.sl_premium
    premium.flr = row.flr
    premium.flr_normative = row.flr_normative
    premium.flr_normative_rate = row.flr_normative_rate
    premium.flr_premium = row.flr_premium
    premium.gok = row.gok
    premium.gok_normative = row.gok_normative
    premium.gok_normative_rate = row.gok_normative_rate
    premium.gok_premium = row.gok_premium
    premium.target = row.target
    premium.target_type = row.target_type
    premium.target_normative_first = row.target_normative_first
    premium.target_normative_second = row.target_normative_second
    premium.target_normative_rate_first = row.target_normative_rate_first
    premium.target_normative_rate_second = row.target_normative_rate_second
    premium.target_premium = row.target_premium
    premium.pers_target_manual = row.pers_target_manual
    premium.head_adjust_premium = row.head_adjust_premium
    premium.total_premium = (
        int(row.total_premium) if row.total_premium is not None else None
    )
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
        if is_head:
            return await api.get_head_premium(period, division)
        return await api.get_specialist_premium(period, division)

    tasks = [(p, d) for p in periods for d in divisions]
    logger.info(f"[{premium_type} Premium] Fetching data for {len(tasks)} API calls")

    results = await asyncio.gather(
        *[fetch(p, d) for p, d in tasks], return_exceptions=True
    )

    premium_objects = []
    for _, result in zip(tasks, results, strict=True):
        if isinstance(result, Exception) or result is None:
            continue

        items = result.premium if is_head else result.items
        for row in items:
            premium = map_head_premium(row) if is_head else map_spec_premium(row)
            premium_objects.append(premium)

    if not premium_objects:
        logger.warning(f"[{premium_type} Premium] No premium data retrieved from API")
        return 0

    logger.info(
        f"[{premium_type} Premium] Mapped {len(premium_objects)} premium records"
    )

    async with get_stats_session() as session:
        model = HeadPremium if is_head else SpecPremium
        logger.info(
            f"[{premium_type} Premium] Deleting old data and inserting new records"
        )

        for premium in premium_objects:
            await session.execute(
                Delete(model).where(
                    model.extraction_period == premium.extraction_period
                )
            )
        session.add_all(premium_objects)
        await session.commit()

    logger.info(
        f"[{premium_type} Premium] Completed: {len(premium_objects)} records saved"
    )
    return len(premium_objects)


async def fill_specialists_premium(api: PremiumAPI, period: str | None = None) -> int:
    """Fill specialist premium data."""
    from src.services.constants import unites
    from src.tasks.base import log_processing_time

    @log_processing_time("Specialist Premium data processing")
    async def _fill():
        periods = [period] if period else get_recent_periods(2)
        logger.info(f"Starting specialist premium update for periods: {periods}")
        count = await fill_premium(api, unites, periods, is_head=False)
        logger.info(f"Specialist premium update completed: {count} records")
        return count

    return await _fill()


async def fill_heads_premium(api: PremiumAPI, period: str | None = None) -> int:
    """Fill head premium data."""
    from src.tasks.base import log_processing_time

    @log_processing_time("Head Premium data processing")
    async def _fill():
        periods = [period] if period else get_recent_periods(2)
        logger.info(f"Starting head premium update for periods: {periods}")
        count = await fill_premium(api, ["НТП", "НЦК"], periods, is_head=True)
        logger.info(f"Head premium update completed: {count} records")
        return count

    return await _fill()


async def fill_all_premium_last_6_months(api: PremiumAPI) -> None:
    """Fill all premium data for last 6 months."""
    from src.tasks.base import log_processing_time

    @log_processing_time("All Premium data processing (last 6 months)")
    async def _fill():
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

    await _fill()
