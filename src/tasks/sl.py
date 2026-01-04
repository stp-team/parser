import asyncio
import logging
from datetime import datetime, timedelta

from okc_py.repos import SlAPI
from sqlalchemy import Delete
from stp_database.models.Stats.sl import SL

from src.core.db import get_stats_session

logger = logging.getLogger(__name__)


async def fill_sl(
    api: SlAPI, periods: list[tuple[str, str]] = None, units: list[int] = None
) -> int:
    """Fill SL data - direct field access from Pydantic models."""
    logger.info("[SL] Starting Service Level data update")

    if periods is None:
        yesterday = datetime.now() - timedelta(days=1)
        today = datetime.now()
        periods = [(yesterday.strftime("%d.%m.%Y"), today.strftime("%d.%m.%Y"))]

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
    results = await asyncio.gather(
        *[fetch(s, e) for s, e in periods], return_exceptions=True
    )

    # Create SL objects - direct field mapping!
    sl_objects = []
    for (start, _), result in zip(periods, results, strict=True):
        if isinstance(result, Exception) or result is None:
            logger.warning(f"[SL] No data for period starting {start}")
            continue

        sl = SL()
        sl.extraction_period = datetime.strptime(start, "%d.%m.%Y")

        # Direct field access from Pydantic model - NO MAPPING NEEDED!
        td = result.total_data
        sl.received_contacts = td.total_entered
        sl.accepted_contacts = td.total_answered
        sl.missed_contacts = td.total_abandoned
        sl.sl_contacts = td.answered_in_sl
        sl.accepted_contacts_percent = td.answered_percent

        # Get additional fields from detail_data (first row contains summary)
        if result.detail_data.data:
            first_row = result.detail_data.data[0]
            sl.sl = first_row.sl
            sl.average_proc_time = first_row.average_release_time
            # Calculate missed percent from totals
            if td.total_entered > 0:
                sl.missed_contacts_percent = (
                    td.total_abandoned / td.total_entered
                ) * 100

        sl_objects.append(sl)

    if not sl_objects:
        logger.warning("[SL] No SL data to save")
        return 0

    logger.info(f"[SL] Mapped {len(sl_objects)} SL records")

    # Save
    async with get_stats_session() as session:
        logger.info("[SL] Deleting old SL data and inserting new records")
        for sl in sl_objects:
            await session.execute(
                Delete(SL).where(SL.extraction_period == sl.extraction_period)
            )
        session.add_all(sl_objects)
        await session.commit()

    logger.info(f"[SL] Completed: {len(sl_objects)} records saved")
    return len(sl_objects)


async def fill_sl_for_periods(api: SlAPI, periods: list[tuple[str, str]]) -> int:
    """Fill SL data for specific periods."""
    from src.tasks.base import log_processing_time

    @log_processing_time("SL data processing for specific periods")
    async def _fill():
        return await fill_sl(api, periods=periods)

    return await _fill()
