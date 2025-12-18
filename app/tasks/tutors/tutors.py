import logging

from app.api.tutors import TutorsAPI
from app.tasks.base import create_config_factory, log_processing_time
from app.tasks.tutors.utils import (
    TutorScheduleProcessingConfig,
    TutorScheduleProcessor,
    generate_tutor_schedule_periods,
)

logger = logging.getLogger(__name__)

# Базовая конфигурация
_create_tutor_config = create_config_factory(
    TutorScheduleProcessingConfig,
    division_id=2,
    picked_units=[7, 5, 6],
    picked_tutor_types=[1, 2, 3],
    picked_shift_types=[1, 2, 4, 3],
    semaphore_limit=10,
)


@log_processing_time("обновление расписания наставников")
async def fill_tutor_schedule(
    api: TutorsAPI, periods: list[tuple[str, str]] = None, full_update: bool = False
) -> int:
    """
    Основная функция для обновления расписания наставников.

    Args:
        api: API клиент для наставников
        periods: Список периодов в формате [(start_date, end_date), ...] где даты в формате "DD.MM.YYYY".
                Если не указан, используется full_update для определения периода.
        full_update: Если True и periods не указан, обрабатывает 6 месяцев,
                    иначе 2 месяца. Игнорируется если указан periods.

    Returns:
        int: Количество обработанных записей
    """
    try:
        if not periods:
            months = 6 if full_update else 2
            periods = generate_tutor_schedule_periods(months)

        # Создание конфига
        update_type = f"Расписание наставников ({'полное' if full_update else 'инкрементальное'} обновление)"
        config = _create_tutor_config(update_type=update_type)

        processor = TutorScheduleProcessor(api)
        return await processor.process_with_config(config, periods=periods)

    except Exception as e:
        logger.error(f"Ошибка обновления расписания наставников: {e}")
        raise


@log_processing_time("заполнение расписания наставников за указанные периоды")
async def fill_tutor_schedule_for_periods(
    api: TutorsAPI, periods: list[tuple[str, str]]
) -> int:
    """
    Обновляет расписание наставников за указанные периоды.

    Args:
        api: API клиент для наставников
        periods: Список периодов в формате [(start_date, end_date), ...] где даты в формате "DD.MM.YYYY"

    Returns:
        int: Количество обработанных записей
    """
    return await fill_tutor_schedule(api, periods=periods)
