import logging
from datetime import datetime

from okc_py.models.tests import AssignedTest as AssignedTestModel
from okc_py.repos import TestsAPI
from sqlalchemy import Delete
from stp_database.models.Stats import AssignedTest

from src.core.db import get_stats_session
from src.tasks.base import PeriodHelper

logger = logging.getLogger(__name__)


def get_default_date_range() -> tuple[str, str]:
    """Get default date range (12 months back to today)."""
    now = datetime.now()
    start_date = PeriodHelper.add_months(now, -12)
    return (
        PeriodHelper.format_date_for_api(start_date, "DD.MM.YYYY"),
        PeriodHelper.format_date_for_api(now, "DD.MM.YYYY"),
    )


async def fetch_assigned_tests(
    api: TestsAPI,
    start_date: str,
    stop_date: str,
) -> list[AssignedTestModel] | None:
    """Fetch assigned tests from API."""
    return await api.get_assigned_tests(
        start_date=start_date,
        stop_date=stop_date,
        # subdivisions=subdivisions,
    )


def map_assigned_test(api_test: AssignedTestModel) -> AssignedTest:
    """Map Pydantic model to DB model - direct field access."""
    test = AssignedTest()
    test.test_id = int(api_test.id) if api_test.id else None
    test.test_name = api_test.test_name
    test.employee_fullname = api_test.user_name
    test.head_fullname = api_test.head_name
    test.creator_fullname = api_test.creator_name
    test.status = api_test.status_name

    if api_test.active_from:
        try:
            test.extraction_period = datetime.strptime(api_test.active_from, "%d.%m.%Y")
        except ValueError:
            test.extraction_period = datetime.now()

    return test


async def save_assigned_tests(tests: list[AssignedTest]) -> int:
    """Save assigned tests to database."""
    if not tests:
        return 0

    async with get_stats_session() as session:
        # Delete all old data
        await session.execute(Delete(AssignedTest))
        session.add_all(tests)
        await session.commit()

    return len(tests)


async def fill_assigned_tests(
    api: TestsAPI,
    start_date: str = None,
    stop_date: str = None,
) -> int:
    """Fill assigned tests data."""
    from src.tasks.base import log_processing_time

    @log_processing_time("Assigned Tests data processing")
    async def _fill():
        nonlocal start_date, stop_date

        if start_date is None or stop_date is None:
            start_date, stop_date = get_default_date_range()

        logger.info(f"[Tests] Fetching assigned tests from {start_date} to {stop_date}")

        # Fetch
        api_tests = await fetch_assigned_tests(api, start_date, stop_date)

        if not api_tests:
            logger.warning("[Tests] No assigned tests data received from API")
            return 0

        logger.info(f"[Tests] Retrieved {len(api_tests)} assigned tests from API")

        # Map
        tests = [map_assigned_test(t) for t in api_tests]
        logger.info(f"[Tests] Mapped {len(tests)} test records")

        # Save
        count = await save_assigned_tests(tests)
        logger.info(f"[Tests] Completed: {count} records saved")
        return count

    return await _fill()


async def fill_all_tests_data(api: TestsAPI) -> int:
    """Main function for filling all tests data."""
    from src.tasks.base import log_processing_time

    @log_processing_time("All Tests data processing")
    async def _fill():
        logger.info("[Tests] Starting full assigned tests data update")
        count = await fill_assigned_tests(api)
        logger.info(f"[Tests] Full update completed: {count} records")
        return count

    return await _fill()
