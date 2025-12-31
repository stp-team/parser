import asyncio
import logging

from okc_py import Client
from okc_py.config import Settings

from app.core.config import settings
from app.core.nats_client import nats_client
from app.core.nats_router import setup_nats_router
from app.services.logger import setup_logging
from app.services.scheduler import Scheduler
from app.tasks.employees.employees import fill_all_employee_data, fill_tutors
from app.tasks.kpi.kpi import fill_kpi
from app.tasks.premium.premium import fill_heads_premium, fill_specialists_premium
from app.tasks.sl.sl import fill_sl
from app.tasks.tests.tests import fill_current_tests
from app.tasks.tutors.tutors import fill_tutor_schedule

logger = logging.getLogger(__name__)


async def main():
    setup_logging()
    logger.info("Запуск парсера...")

    session = None

    okc_client = Client(
        settings=Settings(
            BASE_URL=settings.OKC_BASE_URL,
            USERNAME=settings.OKC_USERNAME,
            PASSWORD=settings.OKC_PASSWORD,
        )
    )
    try:
        await okc_client.connect()

        # Инициализация и настройка NATS
        try:
            await nats_client.connect()
            await setup_nats_router(okc_client=okc_client)
            await nats_client.subscribe_to_commands()
            logger.info("NATS client и router настроены")
        except Exception as e:
            logger.warning(f"Не удалось настроить NATS: {e}")

        db_url = None
        if settings.SCHEDULER_ENABLE_PERSISTENCE and settings.SCHEDULER_JOB_STORE_URL:
            db_url = settings.SCHEDULER_JOB_STORE_URL
            logger.info(f"Scheduler persistence enabled with DB: {db_url}")

        # Инициализация планировщика
        scheduler = Scheduler(
            okc_client=okc_client,
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

            if settings.ENVIRONMENT != "dev":
                # Заполнение данных при старте
                logger.info("Запуск получения данных при старте парсера...")
                await fill_all_employee_data(okc_client.dossier)
                await fill_kpi(okc_client.ure)
                await fill_heads_premium(okc_client.premium)
                await fill_specialists_premium(okc_client.premium)
                await fill_tutors(okc_client.tutors, okc_client.dossier)
                await fill_tutor_schedule(okc_client.tutors)
                await fill_sl(okc_client.sl)
                await fill_current_tests(okc_client.tests)
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
        try:
            await nats_client.disconnect()
        except Exception as e:
            logger.warning(f"Ошибка при закрытии NATS соединения: {e}")

        await okc_client.close()
        if session:
            await session.close()
            logger.info("HTTP session closed")


if __name__ == "__main__":
    asyncio.run(main())
