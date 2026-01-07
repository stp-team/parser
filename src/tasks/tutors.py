import logging
from datetime import datetime, timedelta

from okc_py import TutorsAPI
from okc_py.api.models.tutors import ShiftPart, Trainee, Tutor
from sqlalchemy import Delete
from stp_database.models.Stats import TutorsSchedule

from src.core.db import get_stats_session

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
        if current_date.day == 1:  # Stop after getting first day of each month
            break

    return periods


def extract_trainee_data(
    tutor: Tutor,
    trainee: Trainee,
    shift_day: str,
    shift_parts: list[ShiftPart],
    extraction_period: datetime,
) -> list[TutorsSchedule]:
    """Extract trainee data from tutor graph - direct from Pydantic models."""
    schedules = []

    schedule = TutorsSchedule()
    schedule.extraction_period = extraction_period
    training_day = datetime.strptime(shift_day, "%d.%m.%Y")
    schedule.training_day = training_day

    # Combine date with time for timestamp fields
    if shift_parts:
        time_str = shift_parts[0].start if shift_parts[0].start else "00:00"
        hours, minutes = map(int, time_str.split(":"))
        schedule.training_start_time = training_day.replace(hour=hours, minute=minutes)

        time_str = shift_parts[-1].end if shift_parts[-1].end else "23:59"
        hours, minutes = map(int, time_str.split(":"))
        schedule.training_end_time = training_day.replace(hour=hours, minute=minutes)
    schedule.tutor_fullname = tutor.tutor_info.full_name
    schedule.tutor_employee_id = tutor.tutor_info.employee_id
    schedule.tutor_division = tutor.tutor_info.unit
    schedule.trainee_fullname = trainee.full_name
    schedule.trainee_employee_id = trainee.employee_id
    schedule.trainee_type = trainee.trainee_type

    schedules.append(schedule)
    return schedules


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
        # Zip shifts and trainees together (trainees is list of lists, one per day)
        for shift, trainees_by_day in zip(tutor.shifts, tutor.trainees, strict=False):
            shift_day = shift.day
            shift_parts = shift.shift_parts

            if not trainees_by_day:
                continue

            for trainee in trainees_by_day:
                if trainee.is_active != 1:
                    continue

                schedules.extend(
                    extract_trainee_data(
                        tutor, trainee, shift_day, shift_parts, extraction_date
                    )
                )

    logger.debug(f"[Tutors] Generated {len(schedules)} schedules for {start_date}")
    return schedules


async def save_tutor_schedules(schedules: list[TutorsSchedule]) -> int:
    """Save tutor schedules to database."""
    if not schedules:
        logger.warning("[Tutors] No schedules to save")
        return 0

    logger.info(f"[Tutors] Saving {len(schedules)} tutor schedules")

    async with get_stats_session() as session:
        # Delete old data
        periods_to_delete = {s.extraction_period for s in schedules}
        logger.info(f"[Tutors] Deleting old data for {len(periods_to_delete)} periods")
        for period in periods_to_delete:
            await session.execute(
                Delete(TutorsSchedule).where(TutorsSchedule.extraction_period == period)
            )

        session.add_all(schedules)
        await session.commit()

    logger.info(f"[Tutors] Successfully saved {len(schedules)} schedules")
    return len(schedules)


async def fill_tutor_schedule(
    api: TutorsAPI,
    periods: list[tuple[str, str]] = None,
    full_update: bool = False,
) -> int:
    """Fill tutor schedule data."""
    from src.tasks.base import log_processing_time

    @log_processing_time("Tutor Schedule data processing")
    async def _fill():
        # Use local variable to avoid UnboundLocalError
        update_periods = periods
        if update_periods is None:
            months = 6 if full_update else 2
            update_periods = generate_periods(months)

        logger.info(
            f"[Tutors] Starting tutor schedule update for {len(update_periods)} periods"
        )

        # Process all periods
        all_schedules = []
        for i, period in enumerate(update_periods, 1):
            logger.debug(f"[Tutors] Processing period {i}/{len(update_periods)}")
            schedules = await process_tutor_period(api, period)
            all_schedules.extend(schedules)

        logger.info(f"[Tutors] Total schedules generated: {len(all_schedules)}")

        # Save
        count = await save_tutor_schedules(all_schedules)
        logger.info(f"[Tutors] Tutor schedule update completed: {count} records")
        return count

    return await _fill()


async def fill_tutor_schedule_for_periods(
    api: TutorsAPI,
    periods: list[tuple[str, str]],
) -> int:
    """Fill tutor schedule for specific periods."""
    logger.info(f"[Tutors] Filling tutor schedules for {len(periods)} specific periods")
    count = await fill_tutor_schedule(api, periods=periods)
    logger.info(f"[Tutors] Completed specific periods update: {count} records")
    return count
