import asyncio
import logging
from datetime import datetime

from okc_py import UreAPI
from okc_py.api.models.ure import (
    AHTDataRecord,
    CSATDataRecord,
    CSIDataRecord,
    DelayDataRecord,
    FLRDataRecord,
    PaidServiceRecord,
    POKDataRecord,
    SalesDataRecord,
    SalesPotentialDataRecord,
)
from sqlalchemy import delete
from sqlalchemy.orm import DeclarativeBase
from stp_database.models.Stats import SpecDayKPI, SpecMonthKPI, SpecWeekKPI

from src.core.db import get_stats_session
from src.services.helpers import (
    get_month_period_for_kpi,
    get_week_start_date,
    get_yesterday_date,
)

logger = logging.getLogger(__name__)

# Report types and their corresponding Pydantic models
REPORT_TYPES = {
    "AHT": AHTDataRecord,
    "FLR": FLRDataRecord,
    "CSI": CSIDataRecord,
    "POK": POKDataRecord,
    "DELAY": DelayDataRecord,
    "Sales": SalesDataRecord,
    "SalesPotential": SalesPotentialDataRecord,
    "PaidService": PaidServiceRecord,
    "CSAT": CSATDataRecord,
}


def aggregate_kpi_data(
    api_results: list[tuple],
    model_class: type[DeclarativeBase],
    extraction_period: datetime,
) -> list:
    """Aggregate KPI data by employee_id."""
    kpi_by_employee_id = {}

    for (_, report_type), api_result in api_results:
        if not api_result or not hasattr(api_result, "data"):
            continue

        record_class = REPORT_TYPES.get(report_type)
        if not record_class:
            continue

        for item in api_result.data:
            # API returns already-parsed Pydantic models, use them directly
            if isinstance(item, record_class):
                record = item
            elif isinstance(item, dict):
                # Fallback: if it's a dict, validate it
                try:
                    record = record_class.model_validate(item)
                except Exception:
                    continue
            else:
                # Try to validate anyway
                try:
                    record = record_class.model_validate(item)
                except Exception:
                    continue

            employee_id = record.id
            if not employee_id:
                continue

            # Convert employee_id to integer if it's a string
            if isinstance(employee_id, str):
                # Handle composite IDs like "106913-16188" -> extract first part
                if "-" in employee_id:
                    employee_id = employee_id.split("-")[0]
                try:
                    employee_id = int(employee_id)
                except ValueError:
                    # Skip completely malformed IDs
                    logger.warning(f"Skipping malformed employee_id (string): {employee_id}")
                    continue

            if employee_id not in kpi_by_employee_id:
                kpi_obj = model_class()
                kpi_obj.employee_id = employee_id
                kpi_obj.extraction_period = extraction_period
                kpi_by_employee_id[employee_id] = kpi_obj

            kpi_obj = kpi_by_employee_id[employee_id]

            # Маппинг в зависимости от типа репорта
            if report_type == "AHT":
                kpi_obj.aht = record.aht
                kpi_obj.aht_chats_web = record.aht_chats_web
                kpi_obj.aht_chats_mobile = record.aht_chats_mobile
                kpi_obj.aht_chats_dhcp = record.aht_chats_dhcp
                kpi_obj.aht_chats_smartdom = record.aht_chats_smartdom
                kpi_obj.aht_chats_telegram = record.aht_chats_telegram
                kpi_obj.aht_chats_viber = record.aht_chats_viber
                kpi_obj.contacts_count = record.aht_total_contacts

            elif report_type == "FLR":
                kpi_obj.flr = record.flr
                kpi_obj.flr_services = record.flr_services
                kpi_obj.flr_services_cross = record.flr_services_cross
                kpi_obj.flr_services_transfer = record.flr_services_transfers

            elif report_type == "CSI":
                kpi_obj.csi = record.csi

            elif report_type == "POK":
                kpi_obj.pok = record.pok
                kpi_obj.pok_rated_contacts = record.pok_rated_contacts

            elif report_type == "DELAY":
                kpi_obj.delay = record.delay

            elif report_type == "Sales":
                kpi_obj.sales = record.sales
                kpi_obj.sales_videos = record.sales_videos
                kpi_obj.sales_routers = record.sales_routers
                kpi_obj.sales_tvs = record.sales_tvs
                kpi_obj.sales_intercoms = record.sales_intercoms
                kpi_obj.sales_conversion = record.sales_conversion

            elif report_type == "SalesPotential":
                kpi_obj.sales_potential = record.sales_potential
                kpi_obj.sales_potential_video = record.sales_potential_video
                kpi_obj.sales_potential_routers = record.sales_potential_routers
                kpi_obj.sales_potential_tvs = record.sales_potential_tvs
                kpi_obj.sales_potential_intercoms = record.sales_potential_intercoms
                kpi_obj.sales_potential_conversion = record.sales_potential_conversion

            elif report_type == "PaidService":
                kpi_obj.services = record.services
                kpi_obj.services_remote = record.services_remote
                kpi_obj.services_onsite = record.services_onsite
                kpi_obj.services_conversion = record.services_conversion

            elif report_type == "CSAT":
                kpi_obj.csat = record.csat
                kpi_obj.csat_rated = record.total_rated
                kpi_obj.csat_high_rated = record.total_high_rated

    # Filter out any records where employee_id is None or not a valid integer
    valid_kpi_data = []
    for kpi_obj in kpi_by_employee_id.values():
        emp_id = kpi_obj.employee_id
        # Skip if employee_id is None, not an integer, or equals 0
        if emp_id is None or not isinstance(emp_id, int) or emp_id == 0:
            logger.warning(f"Skipping invalid employee_id: {emp_id} (type: {type(emp_id)})")
            continue
        valid_kpi_data.append(kpi_obj)
    return valid_kpi_data


async def fetch_kpi_reports(
    api: UreAPI,
    divisions: list[str],
    report_types: list[str],
    start_date: datetime = None,
    use_week_period: bool = False,
) -> list:
    """Fetch KPI reports from API."""
    tasks = []
    for division in divisions:
        for report_type in report_types:
            tasks.append((division, report_type))

    async def fetch_kpi(division: str, report_type: str):
        try:
            if start_date:
                result = await api.get_custom_period_kpi(
                    division=division,
                    report=report_type,
                    start_date=start_date,
                    use_week_period=use_week_period,
                )
            else:
                result = await api.get_period_kpi(
                    division=division, report=report_type, days=1
                )
            return (division, report_type), result
        except Exception as e:
            logger.error(f"Error fetching {division}/{report_type}: {e}")
            return (division, report_type), None

    results = await asyncio.gather(
        *[fetch_kpi(d, r) for d, r in tasks], return_exceptions=True
    )
    return [r for r in results if not isinstance(r, Exception)]


async def save_kpi_data(data: list, model_class: type[DeclarativeBase]) -> int:
    """Save KPI data to database."""
    async with get_stats_session() as session:
        # Delete rows where employee_id is None first
        await session.execute(
            delete(model_class).where(model_class.employee_id.is_(None))
        )
        # Truncate the entire table
        await session.execute(delete(model_class))

        if data:
            # Insert new data for the period being extracted
            session.add_all(data)

        await session.commit()

    return len(data) if data else 0


async def process_kpi(
    api: UreAPI,
    model_class: type[DeclarativeBase],
    extraction_period: datetime,
    start_date: datetime = None,
    use_week_period: bool = False,
) -> int:
    """Process KPI data for given period."""
    from src.services.constants import unites

    model_name = model_class.__name__
    logger.info(f"[{model_name}] Processing KPI data for period: {extraction_period}")

    divisions = unites
    report_types = [
        "AHT",
        "FLR",
        "CSI",
        "POK",
        "DELAY",
        "Sales",
        "SalesPotential",
        "PaidService",
        "CSAT",
    ]

    logger.info(
        f"[{model_name}] Fetching {len(divisions)} divisions x {len(report_types)} report types"
    )

    # Fetch
    api_results = await fetch_kpi_reports(
        api, divisions, report_types, start_date, use_week_period
    )

    logger.info(
        f"[{model_name}] Fetched {len(api_results)} API results, aggregating data"
    )

    # Aggregate
    kpi_data = aggregate_kpi_data(api_results, model_class, extraction_period)

    if not kpi_data:
        logger.warning(f"[{model_name}] No KPI data aggregated after processing")
        return 0

    logger.info(f"[{model_name}] Aggregated {len(kpi_data)} employee records")

    # Save
    saved_count = await save_kpi_data(kpi_data, model_class)
    logger.info(f"[{model_name}] Saved {saved_count} records to database")
    return saved_count


# Public API
async def fill_day_kpi(api: UreAPI) -> None:
    """Fill daily KPI data."""
    from src.tasks.base import log_processing_time

    @log_processing_time("Daily KPI data processing")
    async def _fill():
        extraction_date = get_yesterday_date()
        logger.info(f"Starting daily KPI data update for {extraction_date}")
        count = await process_kpi(api, SpecDayKPI, extraction_date)
        logger.info(f"Daily KPI data update completed: {count} records")

    await _fill()


async def fill_week_kpi(api: UreAPI) -> None:
    """Fill weekly KPI data."""
    from src.tasks.base import log_processing_time

    @log_processing_time("Weekly KPI data processing")
    async def _fill():
        extraction_date = get_week_start_date()
        logger.info(f"Starting weekly KPI data update for {extraction_date}")
        count = await process_kpi(
            api,
            SpecWeekKPI,
            extraction_date,
            start_date=get_week_start_date(),
            use_week_period=True,
        )
        logger.info(f"Weekly KPI data update completed: {count} records")

    await _fill()


async def fill_month_kpi(api: UreAPI) -> None:
    """Fill monthly KPI data."""
    from src.tasks.base import log_processing_time

    @log_processing_time("Monthly KPI data processing")
    async def _fill():
        extraction_date = get_month_period_for_kpi()
        logger.info(f"Starting monthly KPI data update for {extraction_date}")
        count = await process_kpi(
            api,
            SpecMonthKPI,
            extraction_date,
            start_date=get_month_period_for_kpi(),
        )
        logger.info(f"Monthly KPI data update completed: {count} records")

    await _fill()


async def fill_kpi(api: UreAPI) -> None:
    """Fill all KPI types."""
    from src.tasks.base import log_processing_time

    @log_processing_time("All KPI data processing")
    async def _fill():
        logger.info("Starting full KPI data update (day, week, month)")
        await asyncio.gather(
            fill_day_kpi(api),
            fill_week_kpi(api),
            fill_month_kpi(api),
        )
        logger.info("Full KPI data update completed")

    await _fill()
