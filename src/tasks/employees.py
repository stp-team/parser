import asyncio
import logging
from datetime import datetime

from okc_py.models.dossier import EmployeeInfo
from okc_py.repos import DossierAPI, TutorsAPI
from sqlalchemy import select
from stp_database.models.STP import Employee

from src.core.db import get_stp_session

logger = logging.getLogger(__name__)


# ==============================================================================
# EMPLOYEE DATA UPDATES (using DossierAPI)
# ==============================================================================


async def update_birthdays(dossier_api: DossierAPI) -> int:
    """Update employee birthdays from DossierAPI."""
    from src.tasks.base import log_processing_time

    @log_processing_time("Employee birthdays update")
    async def _update():
        logger.info("[Employees] Starting employee birthdays update")
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

                # Find employee in DB
                stmt = select(Employee).where(
                    Employee.fullname == emp_pydantic.fullname
                )
                result = await session.execute(stmt)
                db_emp = result.scalar_one_or_none()

                if db_emp:
                    if emp_pydantic.fired_date:
                        try:
                            db_emp.fired_date = datetime.strptime(
                                emp_pydantic.fired_date, "%d.%m.%Y"
                            )
                        except ValueError:
                            pass
                    updated_count += 1

            await session.commit()

        logger.info(f"[Employees] Updated {updated_count} employee records")
        return updated_count

    return await _update()


async def update_employment_dates(dossier_api: DossierAPI) -> int:
    """Update employee employment dates from DossierAPI - concurrent API calls."""
    from src.tasks.base import log_processing_time

    @log_processing_time("Employee employment dates update")
    async def _update():
        logger.info("[Employees] Starting employee employment dates update")
        employees_data = await dossier_api.get_employees(exclude_fired=True)
        if not employees_data:
            logger.warning("[Employees] No employees data received from API")
            return 0

        valid_employees = [e for e in employees_data if e.fullname]
        logger.info(
            f"[Employees] Fetching details for {len(valid_employees)} employees"
        )

        # Fetch all employee details concurrently
        semaphore = asyncio.Semaphore(10)

        async def fetch_employee_detail(fullname: str):
            async with semaphore:
                return await dossier_api.get_employee(employee_fullname=fullname)

        tasks = [fetch_employee_detail(e.fullname) for e in valid_employees]
        emp_details = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        updated_count = 0
        async with get_stp_session() as session:
            for emp_pydantic, emp_detail in zip(
                valid_employees, emp_details, strict=True
            ):
                if isinstance(emp_detail, Exception):
                    continue
                if not emp_detail or not emp_detail.employeeInfo:
                    continue

                info = emp_detail.employeeInfo

                # Find employee in DB
                stmt = select(Employee).where(
                    Employee.fullname == emp_pydantic.fullname
                )
                result = await session.execute(stmt)
                db_emp = result.scalar_one_or_none()

                if db_emp and info.employment_date:
                    try:
                        db_emp.employment_date = datetime.strptime(
                            info.employment_date, "%d.%m.%Y"
                        )
                        updated_count += 1
                    except ValueError:
                        pass

            await session.commit()

        logger.info(f"[Employees] Updated {updated_count} employment dates")
        return updated_count

    return await _update()


async def update_employee_ids(dossier_api: DossierAPI) -> int:
    """Update employee IDs from DossierAPI - concurrent API calls."""
    from src.tasks.base import log_processing_time

    @log_processing_time("Employee IDs update")
    async def _update():
        logger.info("[Employees] Starting employee IDs update")
        employees_data = await dossier_api.get_employees(exclude_fired=True)
        if not employees_data:
            logger.warning("[Employees] No employees data received from API")
            return 0

        valid_employees = [e for e in employees_data if e.fullname]
        logger.info(
            f"[Employees] Fetching details for {len(valid_employees)} employees"
        )

        # Fetch all employee details concurrently
        semaphore = asyncio.Semaphore(10)

        async def fetch_employee_detail(fullname: str):
            async with semaphore:
                return await dossier_api.get_employee(employee_fullname=fullname)

        tasks = [fetch_employee_detail(e.fullname) for e in valid_employees]
        emp_details = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        updated_count = 0
        async with get_stp_session() as session:
            for emp_pydantic, emp_detail in zip(
                valid_employees, emp_details, strict=True
            ):
                if isinstance(emp_detail, Exception):
                    continue
                if not emp_detail or not emp_detail.employeeInfo:
                    continue

                info = emp_detail.employeeInfo

                # Find employee in DB
                stmt = select(Employee).where(
                    Employee.fullname == emp_pydantic.fullname
                )
                result = await session.execute(stmt)
                db_emp = result.scalar_one_or_none()

                if db_emp:
                    db_emp.employee_id = info.id
                    updated_count += 1

            await session.commit()

        logger.info(f"[Employees] Updated {updated_count} employee IDs")
        return updated_count

    return await _update()


async def update_all_employee_data(dossier_api: DossierAPI) -> int:
    """Update all employee data (birthdays, employment dates, IDs) - only for employees missing data."""
    from src.tasks.base import log_processing_time

    @log_processing_time("All employee data update")
    async def _update():
        logger.info("[Employees] Starting comprehensive employee data update")

        # Step 1: Get employees from database that are missing employment_date or birthday
        async with get_stp_session() as session:
            stmt = select(Employee).where(
                (Employee.employment_date.is_(None)) | (Employee.birthday.is_(None))
            )
            result = await session.execute(stmt)
            db_employees_needing_update = result.scalars().all()

        if not db_employees_needing_update:
            logger.info("[Employees] No employees missing employment_date or birthday")
            return 0

        logger.info(
            f"[Employees] Found {len(db_employees_needing_update)} employees missing data in database"
        )

        # Step 2: Get all employees from API
        employees_data = await dossier_api.get_employees(exclude_fired=True)
        if not employees_data:
            logger.warning("[Employees] No employees data received from DossierAPI")
            return 0

        logger.info(f"[Employees] Retrieved {len(employees_data)} employees from API")

        # Step 3: Match by fullname
        api_employees_by_fullname = {
            e.fullname: e for e in employees_data if e.fullname
        }

        # Find matches
        matched_employees = []
        for db_emp in db_employees_needing_update:
            if db_emp.fullname in api_employees_by_fullname:
                matched_employees.append(
                    (db_emp, api_employees_by_fullname[db_emp.fullname])
                )

        logger.info(
            f"[Employees] Matched {len(matched_employees)} employees between DB and API"
        )

        if not matched_employees:
            return 0

        # Step 4: Fetch detailed data only for matched employees (concurrently)
        semaphore = asyncio.Semaphore(20)

        async def fetch_employee_detail(fullname: str):
            async with semaphore:
                return await dossier_api.get_employee(
                    employee_fullname=fullname,
                    show_kpi=False,
                    show_criticals=False,
                )

        logger.info(
            f"[Employees] Fetching details for {len(matched_employees)} matched employees..."
        )
        tasks = [
            fetch_employee_detail(api_emp.fullname) for _, api_emp in matched_employees
        ]
        emp_details = await asyncio.gather(*tasks, return_exceptions=True)

        # Step 5: Update database
        updated_count = 0
        async with get_stp_session() as session:
            for (db_emp, api_emp), emp_detail in zip(
                matched_employees, emp_details, strict=True
            ):
                if isinstance(emp_detail, Exception):
                    logger.warning(
                        f"[Employees] Error fetching {api_emp.fullname}: {emp_detail}"
                    )
                    continue

                if not emp_detail or not emp_detail.employeeInfo:
                    continue

                info: EmployeeInfo = emp_detail.employeeInfo

                # Refresh db_emp from session
                stmt = select(Employee).where(Employee.fullname == db_emp.fullname)
                result = await session.execute(stmt)
                db_emp = result.scalar_one_or_none()

                if db_emp:
                    # Update all fields
                    db_emp.employee_id = info.id
                    if info.employment_date:
                        try:
                            db_emp.employment_date = datetime.strptime(
                                info.employment_date, "%d.%m.%Y"
                            )
                        except ValueError:
                            pass
                    if info.birthday:
                        try:
                            db_emp.birthday = datetime.strptime(
                                info.birthday, "%d.%m.%Y"
                            )
                        except ValueError:
                            pass
                    updated_count += 1

            await session.commit()

        logger.info(
            f"[Employees] Updated {updated_count} employee records with all data"
        )
        return updated_count

    return await _update()


# ==============================================================================
# TUTOR INFO UPDATES (using TutorsAPI)
# ==============================================================================


async def update_tutor_info(tutors_api: TutorsAPI) -> int:
    """Update tutor information from TutorsAPI."""
    from src.tasks.base import PeriodHelper, log_processing_time

    @log_processing_time("Tutor information update")
    async def _update():
        logger.info("[Employees] Starting tutor information update")

        # Get filters
        graph_filters = await tutors_api.get_graph_filters(division_id=2)
        if not graph_filters:
            logger.warning("[Employees] No graph filters received from API")
            return 0

        # Get period range
        months = PeriodHelper.get_previous_months(6)
        first_period = months[0]
        last_period = months[-1]

        start_date, _ = PeriodHelper.get_date_range_for_period(first_period)
        _, end_date = PeriodHelper.get_date_range_for_period(last_period)

        logger.info(f"[Employees] Fetching tutor data from {start_date} to {end_date}")

        # Get all tutors
        picked_units = [u.id for u in graph_filters.units]
        picked_tutor_types = [t.id for t in graph_filters.tutor_types]
        picked_shift_types = [s.id for s in graph_filters.shift_types]

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
        for tutor in tutor_graph.tutors:
            ti = tutor.tutor_info
            tutors_dict[ti.full_name] = {
                "employee_id": ti.employee_id,
                "tutor_type": ti.tutor_type,
                "tutor_subtype": ti.tutor_subtype,
            }

        # Update employees in DB
        updated_count = 0
        async with get_stp_session() as session:
            for fullname, tutor_info in tutors_dict.items():
                stmt = select(Employee).where(Employee.fullname == fullname)
                result = await session.execute(stmt)
                db_emp = result.scalar_one_or_none()

                if db_emp:
                    db_emp.employee_id = tutor_info["employee_id"]
                    db_emp.is_tutor = True
                    db_emp.tutor_type = tutor_info["tutor_type"]
                    db_emp.tutor_subtype = tutor_info["tutor_subtype"]
                    updated_count += 1

            await session.commit()

        logger.info(f"[Employees] Updated {updated_count} tutor records")
        return updated_count

    return await _update()


# ==============================================================================
# PUBLIC API FUNCTIONS
# ==============================================================================


async def fill_birthdays(dossier_api: DossierAPI) -> int:
    """Fill employee birthdays."""
    return await update_birthdays(dossier_api)


async def fill_employment_dates(dossier_api: DossierAPI) -> int:
    """Fill employee employment dates."""
    return await update_employment_dates(dossier_api)


async def fill_employee_ids(dossier_api: DossierAPI) -> int:
    """Fill employee IDs."""
    return await update_employee_ids(dossier_api)


async def fill_all_employee_data(dossier_api: DossierAPI) -> int:
    """Fill all employee data."""
    return await update_all_employee_data(dossier_api)


async def fill_tutor_info(tutors_api: TutorsAPI) -> int:
    """Fill tutor information."""
    return await update_tutor_info(tutors_api)


async def fill_employees(dossier_api: DossierAPI, tutors_api: TutorsAPI) -> None:
    """Main function to fill all employee-related data."""
    from src.tasks.base import log_processing_time

    @log_processing_time("All employee data processing")
    async def _fill():
        logger.info(
            "[Employees] Starting comprehensive employee data update (all data + tutor info)"
        )
        await update_all_employee_data(dossier_api)
        await update_tutor_info(tutors_api)
        logger.info("[Employees] Comprehensive employee data update completed")

    await _fill()
