import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import delete
from stp_database.models.Stats.tutors_schedule import TutorsSchedule

from app.api.tutors import TutorsAPI
from app.core.db import get_stats_session
from app.services.helpers import get_current_month_first_day
from app.tasks.base import (
    APIProcessor,
    PeriodHelper,
    log_processing_time,
    safe_get_attr,
    validate_api_result,
)

logger = logging.getLogger(__name__)


# Date utilities are now handled by PeriodHelper in base.py
# These functions are kept for compatibility but delegate to PeriodHelper
period_helper = PeriodHelper()


def get_first_day_of_month_ago(months_ago: int) -> datetime:
    """Получает первый день месяца N месяцев назад."""
    return period_helper._add_months(datetime.now(), -months_ago).replace(day=1)


def get_first_day_of_next_month(date: datetime) -> datetime:
    """Получает первый день следующего месяца от заданной даты."""
    return period_helper._add_months(date, 1).replace(day=1)


@dataclass
class TutorScheduleProcessingConfig:
    """Конфигурация для обработки данных расписания наставников."""

    update_type: str
    division_id: int = 2
    picked_units: list[int] = None
    picked_tutor_types: list[int] = None
    picked_shift_types: list[int] = None
    months_back: int = 6  # Количество месяцев назад для получения данных
    truncate_all: bool = False  # Полная очистка таблицы перед загрузкой

    def __post_init__(self):
        if self.picked_units is None:
            self.picked_units = [7, 5, 6]
        if self.picked_tutor_types is None:
            self.picked_tutor_types = [1, 2, 3]
        if self.picked_shift_types is None:
            self.picked_shift_types = [1, 2, 4, 3]

    def get_delete_func(self):
        """Возвращает асинхронную функцию удаления для очистки таблицы."""
        if self.truncate_all:

            async def delete_all(session):
                return await session.execute(delete(TutorsSchedule))

            return delete_all
        else:
            # Удаляем данные за последние 2 месяца (текущий и предыдущий месяц)
            current_month = get_current_month_first_day()
            previous_month = get_first_day_of_month_ago(1)

            async def delete_recent(session):
                return await session.execute(
                    delete(TutorsSchedule).where(
                        TutorsSchedule.extraction_period.in_(
                            [current_month, previous_month]
                        )
                    )
                )

            return delete_recent

    def get_date_range(self) -> tuple[str, str]:
        """Возвращает диапазон дат для запроса к API."""
        start_date = get_first_day_of_month_ago(self.months_back)
        end_date = get_first_day_of_next_month(datetime.now())

        return (start_date.strftime("%d.%m.%Y"), end_date.strftime("%d.%m.%Y"))


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
        **kwargs,
    ) -> list[tuple[Any, ...]]:
        """Получает данные расписания наставников из API."""
        try:
            start_date, end_date = config.get_date_range()

            logger.info(f"Fetching tutor schedule data from {start_date} to {end_date}")

            result = await self.api.get_full_graph(
                division_id=config.division_id,
                start_date=start_date,
                stop_date=end_date,
                picked_units=config.picked_units,
                picked_tutor_types=config.picked_tutor_types,
                picked_shift_types=config.picked_shift_types,
            )

            return [(result, None)]

        except Exception as e:
            logger.error(f"Error fetching tutor schedule data: {e}")
            return [(Exception(e), None)]

    def process_results(
        self,
        results: list[tuple[Any, ...]],
        config: TutorScheduleProcessingConfig,
        **kwargs,
    ) -> list[TutorsSchedule]:
        """Обрабатывает результаты API и подготавливает данные для сохранения в БД."""
        all_schedules = []

        for result in results:
            if isinstance(result[0], Exception):
                logger.error(f"Error in API result: {result[0]}")
                continue

            api_result = result[0]
            schedules = TutorScheduleDataExtractor.extract_schedule_data(api_result)
            all_schedules.extend(schedules)

        logger.info(f"Successfully processed {len(all_schedules)} schedule entries")
        return all_schedules

    async def save_data(
        self,
        data: list[TutorsSchedule],
        config: TutorScheduleProcessingConfig,
        **kwargs,
    ) -> int:
        """Сохраняет данные расписания в БД."""
        if not data:
            logger.warning("No schedule data to save")
            return 0

        async with get_stats_session() as session:
            # Очистка таблицы согласно конфигурации
            delete_func = config.get_delete_func()
            if delete_func:
                await delete_func(session)
                logger.info(f"Cleared existing records for {config.update_type}")

            # Добавление новых записей
            session.add_all(data)
            await session.commit()

            logger.info(f"Successfully saved {len(data)} schedule entries")
            return len(data)


# Конфигурации для различных типов обновлений
FULL_SCHEDULE_CONFIG = TutorScheduleProcessingConfig(
    update_type="Полное расписание (6 месяцев)",
    months_back=6,
    truncate_all=True,
)

INCREMENTAL_SCHEDULE_CONFIG = TutorScheduleProcessingConfig(
    update_type="Инкрементальное обновление (2 месяца)",
    months_back=2,
    truncate_all=False,
)


async def _process_tutor_schedule_with_config(
    api: TutorsAPI,
    config: TutorScheduleProcessingConfig,
) -> int:
    """Универсальная функция обработки расписания наставников с конфигурацией."""
    processor = TutorScheduleProcessor(api)
    return await processor.process_with_config(config)


# Публичные функции API
@log_processing_time("полное заполнение расписания наставников")
async def fill_full_tutor_schedule(api: TutorsAPI) -> int:
    """
    Заполняет полное расписание наставников за последние 6 месяцев.
    Очищает всю таблицу перед загрузкой.
    """
    return await _process_tutor_schedule_with_config(api, FULL_SCHEDULE_CONFIG)


@log_processing_time("инкрементальное обновление расписания наставников")
async def fill_incremental_tutor_schedule(api: TutorsAPI) -> int:
    """
    Обновляет расписание наставников за последние 2 месяца.
    Очищает только данные за последние 2 месяца перед загрузкой.
    """
    return await _process_tutor_schedule_with_config(api, INCREMENTAL_SCHEDULE_CONFIG)


@log_processing_time("обновление расписания наставников")
async def fill_tutor_schedule(api: TutorsAPI, full_update: bool = False) -> int:
    """
    Основная функция для вызова в планировщике.

    Args:
        api: API клиент для наставников
        full_update: Если True, выполняет полное обновление за 6 месяцев,
                    иначе инкрементальное за 2 месяца
    """
    try:
        if full_update:
            return await fill_full_tutor_schedule(api)
        else:
            return await fill_incremental_tutor_schedule(api)

    except Exception as e:
        logger.error(f"Ошибка обновления расписания наставников: {e}")
        raise
