import asyncio
import logging

from stp_database.repo.STP import MainRequestsRepo

from app.api.employees import EmployeesAPI
from app.core.db import get_stp_session

logger = logging.getLogger(__name__)


async def fetch_employee_data(
    employees_api, api_employee_id: int, db_employee_name: str
):
    """Получаем информацию о сотруднике.

    Args:
        employees_api: Экземпляр API
        api_employee_id: Идентификатор сотрудника в API
        db_employee_name: ФИО сотрудника в БД
    """
    try:
        employee_data = await employees_api.get_employee(employee_id=api_employee_id)
        return employee_data, db_employee_name, api_employee_id
    except Exception as e:
        logger.error(
            f"[Дни рождений] Ошибка получения сотрудника {db_employee_name} из API: {e}"
        )
        return None, db_employee_name, api_employee_id


async def fill_birthdays(api: EmployeesAPI):
    employees = await api.get_employees(exclude_fired=True)
    logger.debug(f"[Дни рождений] Найдено {len(employees)} сотрудников в API")

    if not employees:
        logger.warning("[Дни рождений] Не найдено сотрудников в API")
        return []

    async with get_stp_session() as session:
        repo = MainRequestsRepo(session)
        db_employees = await repo.employee.get_users()
        logger.debug(f"[Дни рождений] Найдено {len(db_employees)} сотрудников в БД")

        # Фильтруем сотрудников без дня рождения
        employees_without_birthday = [emp for emp in db_employees if not emp.birthday]

        logger.info(
            f"[Дни рождений] Найдено {len(employees_without_birthday)} сотрудников без дня рождения"
        )

        # Собираем пары для обновления
        employee_pairs = []
        for db_employee in employees_without_birthday:
            # Находим пары в АПИ и БД по ФИО
            api_employee = None
            for emp in employees:
                if emp.fullname == db_employee.fullname and db_employee.user_id:
                    api_employee = emp
                    break

            if api_employee:
                employee_pairs.append((db_employee, api_employee))
            else:
                logger.warning(
                    f"[Дни рождений] Не найден пара для {db_employee.fullname} в API"
                )

        logger.info(f"[Дни рождений] Найдено {len(employee_pairs)} пар для обновления")

        if not employee_pairs:
            logger.info("[Дни рождений] Нет сотрудников для обновления")
            return employees

        # Получаем данные по найденным сотрудникам
        logger.info("[Дни рождений] Получаем данные из API")
        fetch_tasks = [
            fetch_employee_data(api, api_emp.id, db_emp.fullname)
            for db_emp, api_emp in employee_pairs
        ]

        # Ограничиваем кол-во одновременных запросов к api
        semaphore = asyncio.Semaphore(10)

        async def limited_fetch(task):
            async with semaphore:
                return await task

        limited_tasks = [limited_fetch(task) for task in fetch_tasks]
        fetch_results = await asyncio.gather(*limited_tasks, return_exceptions=True)

        # Обрабатываем полученные из API данные и готовим пуш в БД
        updates_to_make = []
        successful_fetches = 0

        for i, result in enumerate(fetch_results):
            if isinstance(result, Exception):
                logger.error(f"[Дни рождений] Ошибка при выполнении: {result}")
                continue

            employee_data, db_employee_name, api_employee_id = result
            db_employee = employee_pairs[i][0]

            if employee_data and employee_data.employeeInfo.birthday:
                updates_to_make.append(
                    (db_employee, employee_data.employeeInfo.birthday)
                )
                successful_fetches += 1
                logger.debug(
                    f"[Дни рождений] День рождения {db_employee_name}: {employee_data.employeeInfo.birthday}"
                )
            else:
                logger.warning(
                    f"[Дни рождений] Не найден день рождения для {db_employee_name}"
                )

        logger.info(
            f"[Дни рождений] Успешно получено {successful_fetches} дней рождений"
        )

        # Обновляем БД одной транзакцией
        if updates_to_make:
            updated_count = 0

            try:
                for db_employee, birthday in updates_to_make:
                    await repo.employee.update_user(
                        user_id=db_employee.user_id,
                        birthday=birthday,
                    )
                    logger.debug(
                        f"[Дни рождений] Подготовлено обновление {db_employee.fullname}: {birthday}"
                    )
                    updated_count += 1

                # Коммитим изменения в БД
                await session.commit()
                logger.info(
                    f"[Дни рождений] Успешно закоммичено {updated_count} дней рождений"
                )

            except Exception as e:
                logger.error(f"[Дни рождений] Ошибка при массовом внесении: {e}")
                await session.rollback()
                updated_count = 0
        else:
            logger.info("[Дни рождений] Нет изменений")

    return employees
