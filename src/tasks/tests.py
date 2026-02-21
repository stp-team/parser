import logging
from datetime import datetime

from okc_py import TestsAPI
from okc_py.api.models.tests import AssignedTest as APIAssignedTest
from sqlalchemy import delete
from stp_database.models.Stats import AssignedTest

from src.core.db import get_stats_session
from src.tasks.base import PeriodHelper, log_processing_time

# Optional API tracking
try:
    from src.services.api_tracker import track_api_call, track_db_write

    API_TRACKING_AVAILABLE = True
except ImportError:
    API_TRACKING_AVAILABLE = False

logger = logging.getLogger(__name__)


def get_default_date_range() -> tuple[str, str]:
    """Get default date range (12 months back to today)."""
    now = datetime.now()
    start_date = PeriodHelper.add_months(now, -12)
    return (
        PeriodHelper.format_date_for_api(start_date, "DD.MM.YYYY"),
        PeriodHelper.format_date_for_api(now, "DD.MM.YYYY"),
    )


def parse_active_from(date_str: str) -> datetime:
    """Parse active_from date string with multiple format support."""
    active_from_str = date_str.strip()
    try:
        return datetime.strptime(active_from_str, "%d.%m.%Y %H:%M:%S")
    except ValueError:
        return datetime.strptime(active_from_str, "%d.%m.%Y")


async def fetch_assigned_tests(
    api: TestsAPI,
    start_date: str,
    stop_date: str,
) -> list[APIAssignedTest] | None:
    """Fetch assigned tests from API."""
    if API_TRACKING_AVAILABLE:
        track_api_call("/api/tests/assigned", "GET")
    return await api.get_assigned_tests(
        start_date=start_date,
        stop_date=stop_date,
    )


def create_db_test(
    api_test: APIAssignedTest, extraction_period: datetime
) -> AssignedTest:
    """Create SQLAlchemy model from Pydantic API model."""
    return AssignedTest(
        test_id=int(api_test.id),
        test_name=api_test.test_name,
        employee_fullname=api_test.user_name,
        head_fullname=api_test.head_name,
        creator_fullname=api_test.creator_name,
        status=api_test.status_name,
        active_from=parse_active_from(api_test.active_from),
        extraction_period=extraction_period,
        created_at=datetime.now(),
    )


async def save_assigned_tests(tests: list[AssignedTest]) -> int:
    """Save assigned tests to database with optimized cleanup."""
    if not tests:
        return 0

    async with get_stats_session() as session:
        await session.execute(delete(AssignedTest))
        session.add_all(tests)
        await session.commit()

        # Track DB write
        if API_TRACKING_AVAILABLE and tests:
            track_db_write("assigned_tests")

    return len(tests)


@log_processing_time("Assigned Tests data processing")
async def fill_assigned_tests(
    api: TestsAPI,
    start_date: str = None,
    stop_date: str = None,
) -> int:
    """Fill assigned tests data."""
    if start_date is None or stop_date is None:
        start_date, stop_date = get_default_date_range()

    logger.info(f"[Tests] Fetching assigned tests from {start_date} to {stop_date}")

    # Fetch
    api_tests = await fetch_assigned_tests(api, start_date, stop_date)

    if not api_tests:
        logger.warning("[Tests] No assigned tests data received from API")
        return 0

    logger.info(f"[Tests] Retrieved {len(api_tests)} assigned tests from API")

    # Map API models to DB models
    extraction_period = datetime.strptime(stop_date, "%d.%m.%Y")
    db_tests = [create_db_test(test, extraction_period) for test in api_tests]

    # Save
    count = await save_assigned_tests(db_tests)
    logger.info(f"[Tests] Completed: {count} records saved")
    return count


@log_processing_time("All Tests data processing")
async def fill_all_tests_data(api: TestsAPI) -> int:
    """Main function for filling all tests data."""
    logger.info("[Tests] Starting full assigned tests data update")
    count = await fill_assigned_tests(api)
    logger.info(f"[Tests] Full update completed: {count} records")
    return count
