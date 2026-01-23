import logging
from datetime import datetime, timedelta

from okc_py import TutorsAPI
from okc_py.api.models.tutors import ShiftPart, Trainee, Tutor
from sqlalchemy import delete
from stp_database.models.Stats import TutorsSchedule

from src.core.db import get_stats_session
from src.tasks.base import log_processing_time

logger = logging.getLogger(__name__)


def generate_periods(months: int = 2) -> list[tuple[str, str]]:
    """Generate periods for last N months."""
    periods = []
    end_date = datetime.now() - timedelta(days=1)

    for days_ago in range(months * 30):
        current_date = end_date - timedelta(days=days_ago)
        next_date = current_date + timedelta(days=1)
        periods.append(
            (current_date.strftime("%d.%m.%Y"), next_date.strftime("%d.%m.%Y"))
        )
        if current_date.day == 1:
            break

    return periods


def parse_time_to_datetime(
    base_date: datetime, time_str: str | None, default_time: str
) -> datetime:
    """Parse time string and combine with base date."""
    time_str = time_str if time_str else default_time
    hours, minutes = map(int, time_str.split(":"))
    return base_date.replace(hour=hours, minute=minutes)


def extract_trainee_data(
    tutor: Tutor,
    trainee: Trainee,
    shift_day: str,
    shift_parts: list[ShiftPart],
    extraction_period: datetime,
) -> list[TutorsSchedule]:
    """Extract trainee data from tutor graph - direct from Pydantic models."""
    training_day = datetime.strptime(shift_day, "%d.%m.%Y")

    schedule = TutorsSchedule()
    schedule.extraction_period = extraction_period
    schedule.training_day = training_day
    schedule.training_start_time = parse_time_to_datetime(
        training_day, shift_parts[0].start if shift_parts else None, "00:00"
    )
    schedule.training_end_time = parse_time_to_datetime(
        training_day, shift_parts[-1].end if shift_parts else None, "23:59"
    )
    schedule.tutor_fullname = tutor.tutor_info.full_name
    schedule.tutor_employee_id = tutor.tutor_info.employee_id
    schedule.tutor_division = tutor.tutor_info.unit
    schedule.trainee_fullname = trainee.full_name
    schedule.trainee_employee_id = trainee.employee_id
    schedule.trainee_type = trainee.trainee_type

    return [schedule]


async def fetch_tutor_graph(
    api: TutorsAPI,
    start_date: str,
    stop_date: str,
) -> list | None:
    """Fetch tutor graph from API."""
    result = await api.get_full_graph(
        division_id=2,
        start_date=start_date,
        stop_date=stop_date,
        picked_units=[7, 5, 6],
        picked_tutor_types=[1, 2, 3],
        picked_shift_types=[1, 2, 4, 3],
    )
    return result.tutors if result else None


async def process_tutor_period(
    api: TutorsAPI,
    period: tuple[str, str],
) -> list[TutorsSchedule]:
    """Process one period of tutor data."""
    start_date, stop_date = period
    extraction_date = datetime.strptime(start_date, "%d.%m.%Y")

    logger.debug(f"[Tutors] Processing period: {start_date} - {stop_date}")

    tutors_graph = await fetch_tutor_graph(api, start_date, stop_date)
    if not tutors_graph:
        logger.warning(f"[Tutors] No tutor data for period: {start_date}")
        return []

    schedules = []
    for tutor in tutors_graph:
        for shift, trainees_by_day in zip(tutor.shifts, tutor.trainees, strict=False):
            if not trainees_by_day:
                continue

            for trainee in trainees_by_day:
                if trainee.is_active != 1:
                    continue

                schedules.extend(
                    extract_trainee_data(
                        tutor, trainee, shift.day, shift.shift_parts, extraction_date
                    )
                )

    logger.debug(f"[Tutors] Generated {len(schedules)} schedules for {start_date}")
    return schedules


async def save_tutor_schedules(schedules: list[TutorsSchedule]) -> int:
    """Save tutor schedules to database with optimized cleanup."""
    if not schedules:
        logger.warning("[Tutors] No schedules to save")
        return 0

    logger.info(f"[Tutors] Saving {len(schedules)} tutor schedules")

    async with get_stats_session() as session:
        periods_to_delete = {s.extraction_period for s in schedules}
        logger.info(f"[Tutors] Deleting old data for {len(periods_to_delete)} periods")
        for period in periods_to_delete:
            await session.execute(
                delete(TutorsSchedule).where(TutorsSchedule.extraction_period == period)
            )

        session.add_all(schedules)
        await session.commit()

    logger.info(f"[Tutors] Successfully saved {len(schedules)} schedules")
    return len(schedules)


@log_processing_time("Tutor Schedule data processing")
async def fill_tutor_schedule(
    api: TutorsAPI,
    periods: list[tuple[str, str]] = None,
    full_update: bool = False,
) -> int:
    """Fill tutor schedule data."""
    if periods is None:
        months = 6 if full_update else 2
        periods = generate_periods(months)

    logger.info(f"[Tutors] Starting tutor schedule update for {len(periods)} periods")

    # Process all periods
    all_schedules = []
    for i, period in enumerate(periods, 1):
        logger.debug(f"[Tutors] Processing period {i}/{len(periods)}")
        schedules = await process_tutor_period(api, period)
        all_schedules.extend(schedules)

    logger.info(f"[Tutors] Total schedules generated: {len(all_schedules)}")

    # Save
    count = await save_tutor_schedules(all_schedules)
    logger.info(f"[Tutors] Tutor schedule update completed: {count} records")
    return count


async def fill_tutor_schedule_for_periods(
    api: TutorsAPI,
    periods: list[tuple[str, str]],
) -> int:
    """Fill tutor schedule for specific periods."""
    logger.info(f"[Tutors] Filling tutor schedules for {len(periods)} specific periods")
    count = await fill_tutor_schedule(api, periods=periods)
    logger.info(f"[Tutors] Completed specific periods update: {count} records")
    return count
