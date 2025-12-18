import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import Delete
from sqlalchemy.ext.asyncio import AsyncSession
from stp_database.models.Stats.tutors_schedule import TutorsSchedule

from app.api.tutors import TutorsAPI
from app.core.db import get_stats_session
from app.tasks.base import (
    APIProcessor,
    BatchDBOperator,
    PeriodHelper,
    safe_get_attr,
    validate_api_result,
)

logger = logging.getLogger(__name__)
period_helper = PeriodHelper()


def get_first_day_of_month_ago(months_ago: int) -> datetime:
    """Получает первый день месяца N месяцев назад."""
    return period_helper.add_months(datetime.now(), -months_ago).replace(day=1)


def get_first_day_of_next_month(date: datetime) -> datetime:
    """Получает первый день следующего месяца от заданной даты."""
    return period_helper.add_months(date, 1).replace(day=1)


def generate_tutor_schedule_periods(months: int = 2) -> list[tuple[str, str]]:
    """
    Генерирует список диапазонов дат для получения расписания наставников.

    Args:
        months: Количество месяцев назад от текущего месяца

    Returns:
        Список кортежей (start_date, end_date) в формате DD.MM.YYYY
    """
    periods = []
    for months_ago in range(months):
        start_date = get_first_day_of_month_ago(months_ago)
        end_date = get_first_day_of_next_month(start_date)
        periods.append((start_date.strftime("%d.%m.%Y"), end_date.strftime("%d.%m.%Y")))
    return periods


class TutorScheduleDBManager:
    """Управление операциями БД для расписания наставников."""

    @staticmethod
    async def delete_old_schedule_data(
        session: AsyncSession, extraction_period: datetime
    ) -> None:
        """Удаляет данные расписания наставников за указанный период."""
        query = Delete(TutorsSchedule).where(
            TutorsSchedule.extraction_period == extraction_period
        )
        await session.execute(query)


@dataclass
class TutorScheduleProcessingConfig:
    """Конфигурация для обработки данных расписания наставников."""

    update_type: str
    division_id: int = 2
    picked_units: list[int] = None
    picked_tutor_types: list[int] = None
    picked_shift_types: list[int] = None
    delete_func: Callable[[AsyncSession, datetime], Any] = (
        TutorScheduleDBManager.delete_old_schedule_data
    )
    semaphore_limit: int = 10

    def __post_init__(self):
        if self.picked_units is None:
            self.picked_units = [7, 5, 6]
        if self.picked_tutor_types is None:
            self.picked_tutor_types = [1, 2, 3]
        if self.picked_shift_types is None:
            self.picked_shift_types = [1, 2, 4, 3]

    def get_delete_func(self):
        """Возвращает функцию удаления для совместимости с базовой архитектурой."""
        return self.delete_func


class TutorScheduleDataExtractor:
    """Извлекатель данных расписания наставников."""

    @staticmethod
    def extract_schedule_data(api_result: Any) -> list[TutorsSchedule]:
        """Извлекает данные расписания из результата API."""
        schedule_objects = []

        # Валидируем API результат
        if not validate_api_result(api_result, "tutors"):
            logger.warning("Invalid API result: no tutors data")
            return schedule_objects

        for tutor in api_result.tutors:
            # Безопасная проверка наставника и его стажеров
            trainees = safe_get_attr(tutor, "trainees", [])
            if not trainees:
                continue

            for trainee in trainees:
                if not trainee or not trainee[0]:
                    continue

                trainee_data = trainee[0]

                # Пропускаем записи без даты смены
                shift_day = safe_get_attr(trainee_data, "shift_day")
                if not shift_day:
                    employee_id = safe_get_attr(trainee_data, "employee_id", "unknown")
                    logger.warning(
                        f"Skipping trainee {employee_id} - shift_day is None"
                    )
                    continue

                try:
                    schedule_object = TutorsSchedule()

                    # Данные наставника с безопасным извлечением
                    tutor_info = safe_get_attr(tutor, "tutor_info")
                    if tutor_info:
                        schedule_object.tutor_employee_id = safe_get_attr(
                            tutor_info, "employee_id"
                        )
                        schedule_object.tutor_fullname = safe_get_attr(
                            tutor_info, "full_name"
                        )

                    # Данные стажера с безопасным извлечением
                    schedule_object.trainee_employee_id = safe_get_attr(
                        trainee_data, "employee_id"
                    )
                    schedule_object.trainee_fullname = safe_get_attr(
                        trainee_data, "full_name"
                    )
                    schedule_object.trainee_type = safe_get_attr(
                        trainee_data, "trainee_type"
                    )

                    # Парсинг даты тренировки
                    training_day = datetime.strptime(shift_day, "%d.%m.%Y")
                    schedule_object.training_day = training_day
                    training_date = training_day.date()

                    # Обработка времени начала смены
                    shift_start = safe_get_attr(trainee_data, "shift_start")
                    if shift_start:
                        schedule_object.training_start_time = datetime.combine(
                            training_date,
                            datetime.strptime(shift_start, "%H:%M").time(),
                        )
                    else:
                        schedule_object.training_start_time = None

                    # Обработка времени окончания смены
                    shift_end = safe_get_attr(trainee_data, "shift_end")
                    if shift_end:
                        schedule_object.training_end_time = datetime.combine(
                            training_date,
                            datetime.strptime(shift_end, "%H:%M").time(),
                        )
                    else:
                        schedule_object.training_end_time = None

                    # Период извлечения данных - первый день месяца, когда произошла тренировка
                    schedule_object.extraction_period = datetime(
                        training_day.year, training_day.month, 1
                    )

                    schedule_objects.append(schedule_object)

                    logger.debug(
                        f"Processed: {schedule_object.trainee_employee_id} - {shift_day} "
                        f"{shift_start}-{shift_end}"
                    )

                except Exception as e:
                    employee_id = safe_get_attr(trainee_data, "employee_id", "unknown")
                    logger.error(f"Error processing trainee {employee_id}: {e}")
                    continue

        return schedule_objects


class TutorScheduleProcessor(
    APIProcessor[TutorsSchedule, TutorScheduleProcessingConfig]
):
    """Процессор для обработки данных расписания наставников."""

    def __init__(self, api: TutorsAPI):
        super().__init__(api)

    async def fetch_data(
        self,
        config: TutorScheduleProcessingConfig,
        periods: list[tuple[str, str]] = None,
        **kwargs,
    ) -> list[tuple[Any, ...]]:
        """
        Получает данные расписания наставников из API для указанных периодов.
        Если периоды не указаны, используются последние 2 месяца.
        """
        if not periods:
            periods = generate_tutor_schedule_periods(2)

        results = []
        for start_date, end_date in periods:
            try:
                logger.info(
                    f"Fetching tutor schedule data from {start_date} to {end_date}"
                )

                result = await self.api.get_full_graph(
                    division_id=config.division_id,
                    start_date=start_date,
                    stop_date=end_date,
                    picked_units=config.picked_units,
                    picked_tutor_types=config.picked_tutor_types,
                    picked_shift_types=config.picked_shift_types,
                )

                results.append((start_date, end_date, result))

            except Exception as e:
                logger.error(
                    f"Error fetching tutor schedule data for {start_date}-{end_date}: {e}"
                )
                results.append((start_date, end_date, None))

        return results

    def process_results(
        self,
        results: list[tuple[Any, ...]],
        config: TutorScheduleProcessingConfig,
        **kwargs,
    ) -> list[TutorsSchedule]:
        """Обрабатывает результаты API и подготавливает данные для сохранения в БД."""
        all_schedules = []

        for result in results:
            if isinstance(result, Exception):
                continue

            start_date, end_date, api_result = result

            if not api_result:
                logger.warning(f"No API result for period {start_date}-{end_date}")
                continue

            schedules = TutorScheduleDataExtractor.extract_schedule_data(api_result)
            all_schedules.extend(schedules)
            logger.debug(
                f"Processed {len(schedules)} schedules for period {start_date}-{end_date}"
            )

        logger.info(f"Successfully processed {len(all_schedules)} schedule entries")
        return all_schedules

    async def save_data(
        self,
        data: list[TutorsSchedule],
        config: TutorScheduleProcessingConfig,
        **kwargs,
    ) -> int:
        """Сохраняет данные расписания в БД используя паттерн премий."""
        if not data:
            logger.warning(f"No {config.update_type} data to save")
            return 0

        async with get_stats_session() as session:
            db_operator = BatchDBOperator(session)

            # Группируем данные по периодам для правильного удаления (как в премиях)
            periods_to_clean = {item.extraction_period for item in data}

            # Удаляем старые данные за все обрабатываемые периоды
            for period in periods_to_clean:
                await config.delete_func(session, period)
                logger.debug(
                    f"Cleared existing records for period {period.strftime('%Y-%m')}"
                )

            # Вставляем новые данные
            return await db_operator.bulk_insert_with_cleanup(
                data,
                None,  # delete_func=None, так как уже очистили выше
                config.update_type,
            )
