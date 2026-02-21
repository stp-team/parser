import logging
from datetime import datetime
from typing import Any

from okc_py import DossierAPI, TutorsAPI
from okc_py.api.models.dossier import EmployeeInfo
from sqlalchemy import select
from stp_database.models.STP import Employee

from src.core.db import get_stp_session
from src.tasks.base import ConcurrentAPIFetcher, PeriodHelper, log_processing_time

# Optional API tracking
try:
    from src.services.api_tracker import track_api_call

    API_TRACKING_AVAILABLE = True
except ImportError:
    API_TRACKING_AVAILABLE = False

logger = logging.getLogger(__name__)


def parse_date(date_str: str | None) -> datetime | None:
    """Parse date string in DD.MM.YYYY format."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%d.%m.%Y")
    except ValueError:
        return None


async def find_employee_by_fullname(session, fullname: str) -> Employee | None:
    """Find employee by fullname in database."""
    stmt = select(Employee).where(Employee.fullname == fullname)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def fetch_employee_details_concurrent(
    dossier_api: DossierAPI,
    employees: list[Any],
    semaphore_limit: int = 10,
    **api_kwargs,
) -> list:
    """Fetch employee details concurrently using ConcurrentAPIFetcher.

    IMPORTANT: This function expects database Employee objects with 'employee_id' attribute.
    The DossierAPI.get_employee() method requires employee_id (OKC ID).
    """

    async def fetch_detail(employee_id: int):
        if API_TRACKING_AVAILABLE:
            track_api_call("/api/dossier/employee", "GET")
        return await dossier_api.get_employee(employee_id=employee_id, **api_kwargs)

    # Extract employee_id from database Employee objects
    tasks = [
        (e.employee_id,)
        for e in employees
        if hasattr(e, "employee_id") and e.employee_id
    ]
    fetcher = ConcurrentAPIFetcher(semaphore_limit=semaphore_limit)
    results = await fetcher.fetch_parallel(tasks, fetch_detail)
    return [result for _, result in results if result is not None]


@log_processing_time("Employee birthdays update")
async def update_birthdays(dossier_api: DossierAPI) -> int:
    """Update employee birthdays from DossierAPI."""
    logger.info("[Employees] Starting employee birthdays update")
    if API_TRACKING_AVAILABLE:
        track_api_call("/api/dossier/employees", "GET")
    employees_data = await dossier_api.get_employees(exclude_fired=True)
    if not employees_data:
        logger.warning("[Employees] No employees data received from API")
        return 0

    logger.info(f"[Employees] Processing {len(employees_data)} employees")

    updated_count = 0
    async with get_stp_session() as session:
        for emp_pydantic in employees_data:
            if not emp_pydantic.fullname:
                continue

            db_emp = await find_employee_by_fullname(session, emp_pydantic.fullname)
            if db_emp and emp_pydantic.fired_date:
                db_emp.fired_date = parse_date(emp_pydantic.fired_date)
                updated_count += 1

        await session.commit()

    logger.info(f"[Employees] Updated {updated_count} employee records")
    return updated_count


@log_processing_time("Employee employment dates update")
async def update_employment_dates(dossier_api: DossierAPI) -> int:
    """Update employee employment dates from DossierAPI - only for employees missing data."""
    logger.info("[Employees] Starting employee employment dates update")

    # Get employees from database that are missing employment_date AND have employee_id set
    async with get_stp_session() as session:
        stmt = select(Employee).where(
            Employee.employment_date.is_(None), Employee.employee_id.is_not(None)
        )
        result = await session.execute(stmt)
        db_employees_needing_update = result.scalars().all()

    if not db_employees_needing_update:
        logger.info(
            "[Employees] No employees missing employment_date (with employee_id)"
        )
        return 0

    logger.info(
        f"[Employees] Found {len(db_employees_needing_update)} employees missing employment_date"
    )

    # Fetch detailed data using employee_id from database (concurrently)
    logger.info(
        f"[Employees] Fetching details for {len(db_employees_needing_update)} employees..."
    )
    emp_details = await fetch_employee_details_concurrent(
        dossier_api, db_employees_needing_update, semaphore_limit=10
    )

    # Update database
    updated_count = 0
    async with get_stp_session() as session:
        for db_emp, emp_detail in zip(
            db_employees_needing_update, emp_details, strict=True
        ):
            if not emp_detail or not emp_detail.employeeInfo:
                continue

            info = emp_detail.employeeInfo

            if info.employment_date:
                db_emp.employment_date = parse_date(info.employment_date)
                if db_emp.employment_date:
                    updated_count += 1

        await session.commit()

    logger.info(f"[Employees] Updated {updated_count} employment dates")
    return updated_count


@log_processing_time("Employee IDs update")
async def update_employee_ids(dossier_api: DossierAPI) -> int:
    """Update employee IDs from DossierAPI - only for employees missing employee_id."""
    logger.info("[Employees] Starting employee IDs update")

    # Get employees from database that are missing employee_id
    async with get_stp_session() as session:
        stmt = select(Employee).where(Employee.employee_id.is_(None))
        result = await session.execute(stmt)
        db_employees_needing_update = result.scalars().all()

    if not db_employees_needing_update:
        logger.info("[Employees] No employees missing employee_id")
        return 0

    logger.info(
        f"[Employees] Found {len(db_employees_needing_update)} employees missing employee_id"
    )

    # Get all employees from API (single call) - the API response includes the employee ID
    if API_TRACKING_AVAILABLE:
        track_api_call("/api/dossier/employees", "GET")
    employees_data = await dossier_api.get_employees(exclude_fired=True)
    if not employees_data:
        logger.warning("[Employees] No employees data received from API")
        return 0

    logger.info(f"[Employees] Retrieved {len(employees_data)} employees from API")

    # Match by fullname and update employee_id
    api_employees_by_fullname = {e.fullname: e for e in employees_data if e.fullname}
    updated_count = 0

    async with get_stp_session() as session:
        for db_emp in db_employees_needing_update:
            if db_emp.fullname in api_employees_by_fullname:
                api_emp = api_employees_by_fullname[db_emp.fullname]
                db_emp.employee_id = api_emp.id
                updated_count += 1

        await session.commit()

    logger.info(f"[Employees] Updated {updated_count} employee IDs")
    return updated_count


@log_processing_time("All employee data update")
async def update_all_employee_data(dossier_api: DossierAPI) -> int:
    """Update all employee data (employee_id, employment dates, birthdays) - only for employees missing data.

    This function:
    1. Fills in employee_id for employees missing it (calls get_employees once)
    2. Fetches detailed data (employment_date, birthday) for employees with employee_id
    """
    logger.info("[Employees] Starting comprehensive employee data update")

    # Step 1: Fill in employee_id for anyone missing it
    logger.info("[Employees] Step 1: Filling missing employee_id values")
    await update_employee_ids(dossier_api)

    # Step 2: Get employees from database that are missing employment_date or birthday
    # and already have employee_id set
    async with get_stp_session() as session:
        stmt = select(Employee).where(
            (Employee.employment_date.is_(None) | Employee.birthday.is_(None)),
            Employee.employee_id.is_not(None),
        )
        result = await session.execute(stmt)
        db_employees_needing_update = result.scalars().all()

    if not db_employees_needing_update:
        logger.info(
            "[Employees] No employees missing employment_date or birthday (with employee_id)"
        )
        return 0

    logger.info(
        f"[Employees] Found {len(db_employees_needing_update)} employees missing employment_date/birthday"
    )

    # Step 3: Fetch detailed data using employee_id from database
    logger.info(
        f"[Employees] Fetching details for {len(db_employees_needing_update)} employees..."
    )
    emp_details = await fetch_employee_details_concurrent(
        dossier_api,
        db_employees_needing_update,
        semaphore_limit=20,
        show_kpi=False,
        show_criticals=False,
    )

    # Step 4: Update database
    updated_count = 0
    async with get_stp_session() as session:
        for db_emp, emp_detail in zip(
            db_employees_needing_update, emp_details, strict=True
        ):
            if not emp_detail or not emp_detail.employeeInfo:
                if isinstance(emp_detail, Exception):
                    logger.warning(
                        f"[Employees] Error fetching employee_id={db_emp.employee_id}: {emp_detail}"
                    )
                continue

            info: EmployeeInfo = emp_detail.employeeInfo

            # Update employment_date if missing
            if info.employment_date and not db_emp.employment_date:
                db_emp.employment_date = parse_date(info.employment_date)
                if db_emp.employment_date:
                    updated_count += 1

            # Update birthday if missing
            if info.birthday and not db_emp.birthday:
                db_emp.birthday = parse_date(info.birthday)
                if db_emp.birthday:
                    updated_count += 1

        await session.commit()

    logger.info(
        f"[Employees] Updated {updated_count} employee records with employment_date/birthday"
    )
    return updated_count


@log_processing_time("Tutor information update")
async def update_tutor_info(tutors_api: TutorsAPI) -> int:
    """Update tutor information from TutorsAPI."""
    from datetime import date

    logger.info("[Employees] Starting tutor information update")

    # Get filters
    if API_TRACKING_AVAILABLE:
        track_api_call("/api/tutors/filters", "GET")
    graph_filters = await tutors_api.get_filters(division_id=2)
    if not graph_filters:
        logger.warning("[Employees] No graph filters received from API")
        return 0

    # Get period range (include current month)
    current_month = date.today().strftime("%Y-%m")
    previous_months = PeriodHelper.get_previous_months(6)
    months = previous_months + [current_month]

    first_period = months[0]
    last_period = months[-1]

    start_date, _ = PeriodHelper.get_date_range_for_period(first_period)
    _, end_date = PeriodHelper.get_date_range_for_period(last_period)

    logger.info(f"[Employees] Fetching tutor data from {start_date} to {end_date}")

    # Get all tutors
    picked_units = [u.id for u in graph_filters.units]
    picked_tutor_types = [t.id for t in graph_filters.tutor_types]
    picked_shift_types = [s.id for s in graph_filters.shift_types]

    if API_TRACKING_AVAILABLE:
        track_api_call("/api/tutors/full-graph", "GET")
    tutor_graph = await tutors_api.get_full_graph(
        division_id=2,
        start_date=start_date,
        stop_date=end_date,
        picked_units=picked_units,
        picked_tutor_types=picked_tutor_types,
        picked_shift_types=picked_shift_types,
    )

    if not tutor_graph or not tutor_graph.tutors:
        logger.warning("[Employees] No tutor data received from API")
        return 0

    logger.info(f"[Employees] Retrieved {len(tutor_graph.tutors)} tutors from API")

    # Create tutor lookup
    tutors_dict = {}
    subtype_stats = {"with_subtype": 0, "without_subtype": 0}
    for tutor in tutor_graph.tutors:
        ti = tutor.tutor_info
        logger.debug(
            f"[Employees] Tutor info - Name: {ti.full_name}, "
            f"ID: {ti.employee_id}, "
            f"Type: {ti.tutor_type}, "
            f"Subtype: {ti.tutor_subtype}"
        )
        if ti.tutor_subtype:
            subtype_stats["with_subtype"] += 1
        else:
            subtype_stats["without_subtype"] += 1

        tutors_dict[ti.full_name] = {
            "employee_id": ti.employee_id,
            "tutor_type": ti.tutor_type,
            "tutor_subtype": ti.tutor_subtype,
        }

    logger.info(
        f"[Employees] Tutor subtype stats: "
        f"{subtype_stats['with_subtype']} with subtype, "
        f"{subtype_stats['without_subtype']} without subtype"
    )

    # Update employees in DB
    updated_count = 0
    not_found_count = 0
    async with get_stp_session() as session:
        for fullname, tutor_info in tutors_dict.items():
            db_emp = await find_employee_by_fullname(session, fullname)

            if db_emp:
                emp_id = tutor_info["employee_id"]
                db_emp.employee_id = int(emp_id) if emp_id else None
                db_emp.is_tutor = True
                db_emp.tutor_type = tutor_info["tutor_type"]
                db_emp.tutor_subtype = tutor_info["tutor_subtype"]

                logger.debug(
                    f"[Employees] Updated tutor {fullname}: "
                    f"subtype={tutor_info['tutor_subtype']}, "
                    f"type={tutor_info['tutor_type']}, "
                    f"employee_id={tutor_info['employee_id']}"
                )
                updated_count += 1
            else:
                not_found_count += 1
                logger.debug(f"[Employees] Employee not found in DB: {fullname}")

        await session.commit()

    logger.info(
        f"[Employees] Updated {updated_count} tutor records "
        f"({not_found_count} tutors not found in DB)"
    )
    return updated_count


async def fill_birthdays(dossier_api: DossierAPI) -> int:
    """Fill employee birthdays."""
    return await update_birthdays(dossier_api)


async def fill_employment_dates(dossier_api: DossierAPI) -> int:
    """Fill employee employment dates."""
    return await update_employment_dates(dossier_api)


async def fill_employee_ids(dossier_api: DossierAPI) -> int:
    """Fill employee IDs."""
    return await update_employee_ids(dossier_api)


@log_processing_time("All employee data processing")
async def fill_employees(dossier_api: DossierAPI, tutors_api: TutorsAPI) -> None:
    """Main function to fill all employee-related data."""
    logger.info(
        "[Employees] Starting comprehensive employee data update (all data + tutor info)"
    )
    await update_all_employee_data(dossier_api)
    await update_tutor_info(tutors_api)
    logger.info("[Employees] Comprehensive employee data update completed")
