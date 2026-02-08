import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any

from okc_py import UreAPI
from okc_py.api.models import ThanksReportItem
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
from okc_py.api.repos.thanks import ThanksAPI
from sqlalchemy import delete
from stp_database.models.Stats import SpecDayKPI, SpecMonthKPI, SpecWeekKPI

from src.core.db import get_stats_session
from src.services.helpers import (
    get_month_period_for_kpi,
    get_week_start_date,
    get_yesterday_date,
)
from src.tasks.base import BatchDBOperator, ConcurrentAPIFetcher, log_processing_time

logger = logging.getLogger(__name__)

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
# Field mapping configuration for each report type
FIELD_MAPPERS = {
    "AHT": lambda kpi, r: setattr_kpi(
        kpi,
        r,
        "aht",
        "aht_chats_web",
        "aht_chats_mobile",
        "aht_chats_dhcp",
        "aht_chats_smartdom",
        "aht_chats_telegram",
        "aht_chats_viber",
        contacts_count="aht_total_contacts",
    ),
    "FLR": lambda kpi, r: setattr_kpi(
        kpi,
        r,
        "flr",
        "flr_services",
        flr_services_cross="flr_services_cross",
        flr_services_transfer="flr_services_transfers",
    ),
    "CSI": lambda kpi, r: setattr_kpi(kpi, r, "csi"),
    "POK": lambda kpi, r: setattr_kpi(kpi, r, "pok", "pok_rated_contacts"),
    "DELAY": lambda kpi, r: setattr_kpi(kpi, r, "delay"),
    "Sales": lambda kpi, r: setattr_kpi(
        kpi,
        r,
        "sales",
        "sales_videos",
        "sales_routers",
        "sales_tvs",
        "sales_intercoms",
        "sales_conversion",
    ),
    "SalesPotential": lambda kpi, r: setattr_kpi(
        kpi,
        r,
        "sales_potential",
        "sales_potential_video",
        "sales_potential_routers",
        "sales_potential_tvs",
        "sales_potential_intercoms",
        "sales_potential_conversion",
    ),
    "PaidService": lambda kpi, r: setattr_kpi(
        kpi, r, "services", "services_remote", "services_onsite", "services_conversion"
    ),
    "CSAT": lambda kpi, r: setattr_kpi(
        kpi, r, "csat", csat_rated="total_rated", csat_high_rated="total_high_rated"
    ),
}


def setattr_kpi(kpi_obj: Any, record: Any, *attrs: str, **mapping: str) -> None:
    """Set attributes on KPI object from record with optional field name mapping."""
    for attr in attrs:
        setattr(kpi_obj, attr, getattr(record, attr, None))
    for kpi_attr, record_attr in mapping.items():
        setattr(kpi_obj, kpi_attr, getattr(record, record_attr, None))


def parse_employee_id(employee_id: Any) -> int | None:
    """Parse and validate employee_id from various formats."""
    if not employee_id:
        return None

    if isinstance(employee_id, int):
        return employee_id if employee_id != 0 else None

    if isinstance(employee_id, str):
        emp_id = employee_id.split("-")[0] if "-" in employee_id else employee_id
        try:
            return int(emp_id) if int(emp_id) != 0 else None
        except ValueError:
            return None

    return None


def aggregate_kpi_data(
    api_results: list[tuple],
    model_class: type,
    extraction_period: datetime,
    thanks_results: list[tuple[int, Any]] = None,
) -> list:
    """Aggregate KPI data by employee_id."""
    kpi_by_employee_id = {}

    # Process standard KPI reports
    for (_, report_type), api_result in api_results:
        if not api_result or not hasattr(api_result, "data"):
            continue

        record_class = REPORT_TYPES.get(report_type)
        mapper = FIELD_MAPPERS.get(report_type)
        if not record_class or not mapper:
            continue

        for item in api_result.data:
            try:
                record = (
                    item
                    if isinstance(item, record_class)
                    else record_class.model_validate(item)
                )
            except Exception:
                continue

            employee_id = parse_employee_id(record.id)
            if employee_id is None:
                continue

            if employee_id not in kpi_by_employee_id:
                kpi_obj = model_class()
                kpi_obj.employee_id = employee_id
                kpi_obj.extraction_period = extraction_period
                kpi_by_employee_id[employee_id] = kpi_obj

            mapper(kpi_by_employee_id[employee_id], record)

    # Process thanks data
    if thanks_results:
        for _, thanks_result in thanks_results:
            if not thanks_result:
                continue

            items = thanks_result if isinstance(thanks_result, list) else []

            for item in items:
                if isinstance(item, dict):
                    employee_id = item.get("whomId")
                elif isinstance(item, ThanksReportItem):
                    employee_id = item.whom_id
                else:
                    continue

                if employee_id is None or employee_id == 0:
                    continue

                if employee_id not in kpi_by_employee_id:
                    kpi_obj = model_class()
                    kpi_obj.employee_id = employee_id
                    kpi_obj.extraction_period = extraction_period
                    kpi_by_employee_id[employee_id] = kpi_obj

                current_thanks = (
                    getattr(kpi_by_employee_id[employee_id], "thanks", 0) or 0
                )
                kpi_by_employee_id[employee_id].thanks = current_thanks + 1

    return [kpi for kpi in kpi_by_employee_id.values() if kpi.employee_id]


async def fetch_kpi_reports(
    api: UreAPI,
    divisions: list[str],
    report_types: list[str],
    start_date: datetime = None,
    use_week_period: bool = False,
) -> list:
    """Fetch KPI reports from API using concurrent fetcher."""

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
            return result
        except Exception as e:
            logger.error(f"Error fetching {division}/{report_type}: {e}")
            return None

    tasks = [
        (division, report_type)
        for division in divisions
        for report_type in report_types
    ]
    fetcher = ConcurrentAPIFetcher(semaphore_limit=15)
    results = await fetcher.fetch_parallel(tasks, fetch_kpi)
    return [
        (task_params, result) for task_params, result in results if result is not None
    ]


async def fetch_thanks_reports(
    api: UreAPI,
    divisions: list[int],
    start_date: str,
    stop_date: str,
) -> list:
    """Fetch thanks reports from API using concurrent fetcher."""
    thanks_api = ThanksAPI(api.client)

    async def fetch_thanks(division: int):
        try:
            result = await thanks_api.get_report(
                whom_units=[division],
                start_date=start_date,
                stop_date=stop_date,
                statuses=[2],
            )
            if hasattr(result, "items"):
                data = result.items
            elif isinstance(result, list):
                data = result
            else:
                data = []

            if data:
                logger.info(f"Division {division}: fetched {len(data)} thanks records")
            return (division, data)
        except Exception as e:
            logger.error(f"Error fetching thanks for division {division}: {e}")
            return (division, [])

    fetcher = ConcurrentAPIFetcher(semaphore_limit=10)
    results = await fetcher.fetch_parallel(
        [(division,) for division in divisions], fetch_thanks
    )
    return [
        result for task_params, result in results if result is not None and result[1]
    ]


async def save_kpi_data(data: list, model_class: type) -> int:
    """Save KPI data to database using BatchDBOperator."""

    async def delete_old_data():
        async with get_stats_session() as session:
            await session.execute(
                delete(model_class).where(model_class.employee_id.is_(None))
            )
            await session.execute(delete(model_class))
            await session.commit()

    if not data:
        return 0

    async with get_stats_session() as session:
        db_operator = BatchDBOperator(session)
        return await db_operator.bulk_insert_with_cleanup(
            data_list=data,
            delete_func=delete_old_data,
            operation_name=f"{model_class.__name__} Update",
        )


async def process_kpi(
    api: UreAPI,
    model_class: type,
    extraction_period: datetime,
    start_date: datetime = None,
    use_week_period: bool = False,
) -> int:
    """Process KPI data for given period."""
    from src.services.constants import unites

    model_name = model_class.__name__
    logger.info(f"[{model_name}] Processing KPI data for period: {extraction_period}")

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
        f"[{model_name}] Fetching {len(unites)} divisions x {len(report_types)} report types"
    )

    api_results = await fetch_kpi_reports(
        api, unites, report_types, start_date, use_week_period
    )
    logger.info(
        f"[{model_name}] Fetched {len(api_results)} API results, aggregating data"
    )

    # Fetch thanks data
    thanks_unit_ids = list(api.unites.values())

    if start_date:
        if use_week_period:
            end_date = start_date + timedelta(days=7)
        elif model_name == "SpecMonthKPI":
            if start_date.month == 12:
                end_date = start_date.replace(year=start_date.year + 1, month=1, day=1)
            else:
                end_date = start_date.replace(month=start_date.month + 1, day=1)
        else:
            end_date = start_date + timedelta(days=1)
        thanks_start = start_date.strftime("%d.%m.%Y")
        thanks_end = end_date.strftime("%d.%m.%Y")
    else:
        yesterday = get_yesterday_date()
        thanks_start = yesterday.strftime("%d.%m.%Y")
        thanks_end = (yesterday + timedelta(days=1)).strftime("%d.%m.%Y")

    logger.info(
        f"[{model_name}] Fetching thanks data for period {thanks_start} - {thanks_end}"
    )
    thanks_results = await fetch_thanks_reports(
        api, thanks_unit_ids, thanks_start, thanks_end
    )

    kpi_data = aggregate_kpi_data(
        api_results, model_class, extraction_period, thanks_results
    )
    if not kpi_data:
        logger.warning(f"[{model_name}] No KPI data aggregated after processing")
        return 0

    logger.info(f"[{model_name}] Aggregated {len(kpi_data)} employee records")
    saved_count = await save_kpi_data(kpi_data, model_class)
    logger.info(f"[{model_name}] Saved {saved_count} records to database")
    return saved_count


# Public API
@log_processing_time("Daily KPI data processing")
async def fill_day_kpi(api: UreAPI) -> None:
    """Fill daily KPI data."""
    extraction_date = get_yesterday_date()
    logger.info(f"Starting daily KPI data update for {extraction_date}")
    count = await process_kpi(api, SpecDayKPI, extraction_date)
    logger.info(f"Daily KPI data update completed: {count} records")


@log_processing_time("Weekly KPI data processing")
async def fill_week_kpi(api: UreAPI) -> None:
    """Fill weekly KPI data."""
    extraction_date = get_week_start_date()
    logger.info(f"Starting weekly KPI data update for {extraction_date}")
    count = await process_kpi(
        api,
        SpecWeekKPI,
        extraction_date,
        start_date=extraction_date,
        use_week_period=True,
    )
    logger.info(f"Weekly KPI data update completed: {count} records")


@log_processing_time("Monthly KPI data processing")
async def fill_month_kpi(api: UreAPI) -> None:
    """Fill monthly KPI data."""
    extraction_date = get_month_period_for_kpi()
    logger.info(f"Starting monthly KPI data update for {extraction_date}")
    count = await process_kpi(
        api, SpecMonthKPI, extraction_date, start_date=extraction_date
    )
    logger.info(f"Monthly KPI data update completed: {count} records")


@log_processing_time("All KPI data processing")
async def fill_kpi(api: UreAPI) -> None:
    """Fill all KPI types."""
    logger.info("Starting full KPI data update (day, week, month)")
    await asyncio.gather(fill_day_kpi(api), fill_week_kpi(api), fill_month_kpi(api))
    logger.info("Full KPI data update completed")
