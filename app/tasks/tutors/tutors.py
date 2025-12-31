import logging
from typing import Any

from stp_database.repo.STP import MainRequestsRepo

from app.core.db import get_stp_session
from app.tasks.base import (
    APIProcessor,
    BatchDBOperator,
    PeriodHelper,
    create_config_factory,
    log_processing_time,
)
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
    api: Any, periods: list[tuple[str, str]] = None, full_update: bool = False
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
    api: Any, periods: list[tuple[str, str]]
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


# =====================================================================================
# TUTOR INFO UPDATING
# =====================================================================================


class TutorInfoProcessor(APIProcessor):
    """Процессор для обновления информации о наставничестве сотрудников."""

    def __init__(self, api: Any):
        super().__init__(api)

    async def fetch_data(self, config, **kwargs):
        """Получает данные о текущих активных наставниках."""
        # Получаем фильтры для определения доступных опций
        graph_filters = await self.api.get_graph_filters(division_id=2)
        if not graph_filters:
            self.logger.warning("Не получены фильтры графика из API")
            return []

        # Подготавливаем параметры для получения полного графика за последние 6 месяцев
        months_data = PeriodHelper.get_previous_months(6)
        first_period = months_data[0]
        last_period = months_data[-1]

        # Получаем границы дат для первого и последнего периодов
        start_date_str, _ = PeriodHelper.get_date_range_for_period(first_period)
        _, end_date_str = PeriodHelper.get_date_range_for_period(last_period)

        # Получаем все доступные ID для фильтров
        picked_units = [unit.id for unit in graph_filters.units]
        picked_tutor_types = [tutor_type.id for tutor_type in graph_filters.tutor_types]
        picked_shift_types = [shift_type.id for shift_type in graph_filters.shift_types]

        self.logger.info(f"Получаем данные за период {start_date_str} - {end_date_str}")

        # Получаем полный график наставников за последние 6 месяцев
        tutor_graph = await self.api.get_full_graph(
            division_id=2,
            start_date=start_date_str,
            stop_date=end_date_str,
            picked_units=picked_units,
            picked_tutor_types=picked_tutor_types,
            picked_shift_types=picked_shift_types,
        )

        if not tutor_graph or not tutor_graph.tutors:
            self.logger.warning("Не получены данные о наставниках из полного графика")
            return []

        # Создаем словарь всех наставников из 6-месячного периода для поиска по ID
        full_tutors_dict = {}
        for tutor in tutor_graph.tutors:
            tutor_id = tutor.tutor_info.tutor_id
            full_tutors_dict[tutor_id] = {
                "tutor_id": tutor_id,
                "full_name": tutor.tutor_info.full_name,
                "tutor_type": tutor.tutor_info.tutor_type,
            }

        self.logger.info(
            f"Получено {len(full_tutors_dict)} уникальных наставников из полного графика"
        )

        # Получаем список текущих активных наставников
        current_tutors = graph_filters.tutors
        self.logger.info(f"Получено {len(current_tutors)} активных наставников")

        # Находим полные имена для активных наставников
        active_tutors_data = []
        for current_tutor in current_tutors:
            tutor_id = current_tutor.id
            if tutor_id in full_tutors_dict:
                # Используем полную информацию из 6-месячного периода
                full_tutor_info = full_tutors_dict[tutor_id]
                active_tutors_data.append(
                    {
                        "tutor_id": tutor_id,
                        "full_name": full_tutor_info["full_name"],
                        "tutor_type": current_tutor.tutor_type,  # Используем актуальный тип из фильтров
                    }
                )
            else:
                # Если не найден в 6-месячном периоде, используем только имя из фильтров
                active_tutors_data.append(
                    {
                        "tutor_id": tutor_id,
                        "full_name": current_tutor.name,  # Используем краткое имя
                        "tutor_type": current_tutor.tutor_type,
                    }
                )

        self.logger.info(
            f"Найдено {len(active_tutors_data)} активных наставников с полными именами"
        )

        return active_tutors_data

    def process_results(self, results, config, **kwargs):
        """Обрабатывает результаты API и подготавливает данные для обновления БД."""
        return results  # Данные уже обработаны в fetch_data

    async def save_data(self, tutors_data, config, **kwargs):
        """Сохраняет информацию о наставничестве в БД используя batch операции."""
        if not tutors_data:
            return 0

        async with get_stp_session() as session:
            repo = MainRequestsRepo(session)
            db_operator = BatchDBOperator(session)

            updates = []

            # Сначала помечаем всех как не-наставников
            all_employees = await repo.employee.get_users()
            reset_count = 0
            for employee in all_employees:
                if employee.is_tutor:  # Только если был наставником
                    updates.append(
                        {
                            "user_id": employee.user_id,
                            "fullname": employee.fullname,
                            "is_tutor": False,
                            "tutor_type": None,
                            "operation": "reset",
                        }
                    )
                    reset_count += 1

            self.logger.info(f"Подготовлено к сбросу {reset_count} записей наставников")

            # Теперь находим и обновляем актуальных наставников
            tutor_updates = 0
            for tutor in tutors_data:
                fullname = tutor["full_name"]
                # Ищем сотрудника в БД по точному совпадению fullname
                employee = await repo.employee.get_users(fullname=fullname)

                if employee:
                    updates.append(
                        {
                            "user_id": employee.user_id,
                            "fullname": fullname,
                            "is_tutor": True,
                            "tutor_type": tutor["tutor_type"],
                            "operation": "update",
                        }
                    )
                    tutor_updates += 1
                    self.logger.debug(f"Найден наставник: {fullname}")
                else:
                    self.logger.debug(f"Не найден сотрудник с именем: {fullname}")

            self.logger.info(f"Подготовлено к обновлению {tutor_updates} наставников")

            # Выполняем batch обновление
            async def update_employee(update_data):
                await repo.employee.update_user(
                    user_id=update_data["user_id"],
                    is_tutor=update_data["is_tutor"],
                    tutor_type=update_data["tutor_type"],
                )

            updated_count = await db_operator.bulk_update_with_transaction(
                updates, update_employee, "Обновление информации о наставничестве"
            )

            self.logger.info(
                f"Сброшено {reset_count} и обновлено {tutor_updates} записей"
            )
            return updated_count


@log_processing_time("обновление информации о наставничестве")
async def fill_tutor_info(api: Any) -> int:
    """
    Обновляет информацию о наставничестве сотрудников.

    Args:
        api: API клиент для наставников

    Returns:
        int: Количество обработанных записей
    """
    processor = TutorInfoProcessor(api)

    # Создаем простую конфигурацию
    class TutorInfoConfig:
        update_type = "Информация о наставничестве"

    config = TutorInfoConfig()
    return await processor.process_with_config(config)
