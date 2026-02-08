import logging
from datetime import datetime, timedelta
from typing import Any

from okc_py import SlAPI
from sqlalchemy import delete
from stp_database.models.Stats.sl import SL

from src.core.db import get_stats_session
from src.tasks.base import ConcurrentAPIFetcher, log_processing_time

logger = logging.getLogger(__name__)


def get_default_periods() -> list[tuple[str, str]]:
    """Get default periods (yesterday to today)."""
    yesterday = datetime.now() - timedelta(days=1)
    today = datetime.now()
    return [(yesterday.strftime("%d.%m.%Y"), today.strftime("%d.%m.%Y"))]


def create_sl_object(result: Any, extraction_date: datetime) -> SL | None:
    """Create SL object from API result."""
    if not result or not result.total_data:
        return None

    sl = SL()
    sl.extraction_period = extraction_date
    td = result.total_data

    sl.received_contacts = td.total_entered
    sl.accepted_contacts = td.total_answered
    sl.missed_contacts = td.total_abandoned
    sl.sl_contacts = td.answered_in_sl
    sl.accepted_contacts_percent = td.answered_percent

    if result.detail_data.data:
        first_row = result.detail_data.data[0]
        sl.sl = first_row.sl
        sl.average_proc_time = first_row.average_release_time
        if td.total_entered > 0:
            sl.missed_contacts_percent = (td.total_abandoned / td.total_entered) * 100

    return sl


async def fill_sl(
    api: SlAPI, periods: list[tuple[str, str]] = None, units: list[int] = None
) -> int:
    """Fill SL data - direct field access from Pydantic models."""
    logger.info("[SL] Starting Service Level data update")

    if periods is None:
        periods = get_default_periods()

    if units is None:
        units = [7]

    logger.info(f"[SL] Processing {len(periods)} periods with units: {units}")

    # Get queues
    logger.info("[SL] Fetching queue list from API")
    queues_result = await api.get_vq_chat_filter()
    if not queues_result:
        logger.error("[SL] Failed to get queue list from API")
        raise ValueError("Failed to get queue list")
    queues = [vq for queue in queues_result.ntp_nck.queues for vq in queue.vqList]
    logger.info(f"[SL] Retrieved {len(queues)} queues")

    # Fetch all periods
    async def fetch(start_date: str, stop_date: str):
        return await api.get_sl(
            start_date=start_date, stop_date=stop_date, units=units, queues=queues
        )

    logger.info(f"[SL] Fetching SL data for {len(periods)} periods")
    fetcher = ConcurrentAPIFetcher(semaphore_limit=5)
    results = await fetcher.fetch_parallel(periods, fetch)

    # Create SL objects using helper
    sl_objects = []
    for (start, _), result in results:
        if result is None:
            logger.warning(f"[SL] No data for period starting {start}")
            continue

        extraction_date = datetime.strptime(start, "%d.%m.%Y")
        sl = create_sl_object(result, extraction_date)
        if sl:
            sl_objects.append(sl)

    if not sl_objects:
        logger.warning("[SL] No SL data to save")
        return 0

    logger.info(f"[SL] Mapped {len(sl_objects)} SL records")

    async with get_stats_session() as session:
        logger.info("[SL] Deleting old SL data and inserting new records")
        unique_periods = {sl.extraction_period for sl in sl_objects}
        for period in unique_periods:
            await session.execute(delete(SL).where(SL.extraction_period == period))
        session.add_all(sl_objects)
        await session.commit()

    logger.info(f"[SL] Completed: {len(sl_objects)} records saved")
    return len(sl_objects)


@log_processing_time("SL data processing for specific periods")
async def fill_sl_for_periods(api: SlAPI, periods: list[tuple[str, str]]) -> int:
    """Fill SL data for specific periods."""
    return await fill_sl(api, periods=periods)
