import asyncio
import logging

from aiohttp import ClientSession

from app.api.employees import EmployeesAPI
from app.api.kpi import KpiAPI
from app.api.premium import PremiumAPI
from app.api.sl import SlAPI
from app.api.tutors import TutorsAPI
from app.core.auth import authenticate
from app.core.config import settings
from app.services.logger import setup_logging
from app.services.scheduler import Scheduler
from app.tasks.employees.employees import fill_all_employee_data
from app.tasks.kpi.kpi import fill_kpi
from app.tasks.premium.premium import fill_heads_premium, fill_specialists_premium
from app.tasks.sl.sl import fill_sl
from app.tasks.tutors.tutors import fill_tutor_schedule

logger = logging.getLogger(__name__)


async def main():
    setup_logging()
    logger.info("Запуск парсера...")

    session = None

    try:
        # Инициализация сессии и авторизация
        session = ClientSession(base_url=settings.OKC_BASE_URL)
        await authenticate(
            username=settings.OKC_USERNAME,
            password=settings.OKC_PASSWORD,
            session=session,
        )
        logger.info("Успешная авторизация на OKC")

        # Инициализация API клиентов
        employees_api = EmployeesAPI(session)
        kpi_api = KpiAPI(session)
        premium_api = PremiumAPI(session)
        sl_api = SlAPI(session)
        tutors_api = TutorsAPI(session)

        db_url = None
        if settings.SCHEDULER_ENABLE_PERSISTENCE and settings.SCHEDULER_JOB_STORE_URL:
            db_url = settings.SCHEDULER_JOB_STORE_URL
            logger.info(f"Scheduler persistence enabled with DB: {db_url}")

        # Инициализация планировщика
        scheduler = Scheduler(
            employees_api=employees_api,
            kpi_api=kpi_api,
            premium_api=premium_api,
            tutors_api=tutors_api,
            db_url=db_url,
            max_workers=settings.SCHEDULER_MAX_WORKERS,
        )

        async with scheduler.managed_lifecycle():
            logger.info("Планировщик запущен")

            status = scheduler.get_job_status()
            logger.info(f"Запланированные задачи: {len(status['jobs'])}")
            for job in status["jobs"]:
                logger.info(
                    f"  - {job['name']} (ID: {job['id']}) - Next run: {job['next_run']}"
                )

            # Заполнение данных при старте
            logger.info("Запуск получения данных при старте парсера...")
            await fill_all_employee_data(employees_api)
            await fill_kpi(kpi_api)
            await fill_heads_premium(premium_api)
            await fill_specialists_premium(premium_api)
            await fill_tutor_schedule(tutors_api)
            await fill_sl(sl_api)
            logger.info("Получение данных при старте завершено")

            try:
                while True:
                    await asyncio.sleep(10)

                    if logger.isEnabledFor(logging.DEBUG):
                        status = scheduler.get_job_status()
                        logger.debug(f"Scheduler stats: {status['stats']}")

            except KeyboardInterrupt:
                logger.info("Keyboard interrupt received. Shutting down gracefully...")
            except Exception as e:
                logger.error(f"Unexpected error in main loop: {e}")
                raise

    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        raise

    finally:
        # Очистка ресурсов
        if session:
            await session.close()
            logger.info("HTTP session closed")


if __name__ == "__main__":
    asyncio.run(main())
