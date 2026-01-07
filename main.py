import asyncio
import logging

from okc_py import OKC
from okc_py.config import Settings

from src.core.config import settings
from src.core.nats_client import nats_client
from src.core.nats_router import setup_nats_router
from src.core.ws_bridge import cleanup_ws_bridges, setup_ws_bridges
from src.services.logger import setup_logging
from src.services.scheduler import Scheduler
from src.tasks.employees import fill_all_employee_data
from src.tasks.premium import fill_heads_premium, fill_specialists_premium
from src.tasks.sl import fill_sl
from src.tasks.tests import fill_assigned_tests
from src.tasks.tutors import fill_tutor_schedule
from src.tasks.ure import fill_kpi

logger = logging.getLogger(__name__)


async def main():
    setup_logging()
    logger.info("Запуск парсера...")

    okc_client = OKC(
        username=settings.OKC_USERNAME,
        password=settings.OKC_PASSWORD,
        settings=Settings(
            BASE_URL=settings.OKC_BASE_URL,
        ),
    )
    try:
        await okc_client.connect()

        # Инициализация и настройка NATS
        try:
            await nats_client.connect()
            await setup_nats_router(okc_client=okc_client)
            await nats_client.subscribe_to_commands()
            logger.info("NATS client и router настроены")

            # Setup WebSocket bridges for real-time lines data
            try:
                await setup_ws_bridges(
                    okc_client=okc_client,
                    lines=settings.WS_LINES,
                )
                logger.info(f"WebSocket bridges настроены для линий: {settings.WS_LINES}")
            except Exception as e:
                logger.warning(f"Не удалось настроить WebSocket bridges: {e}")

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
                await fill_tutor_schedule(okc_client.tutors)
                await fill_sl(okc_client.sl)
                await fill_assigned_tests(okc_client.tests)
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
            await cleanup_ws_bridges()
        except Exception as e:
            logger.warning(f"Ошибка при закрытии WebSocket bridges: {e}")

        try:
            await nats_client.disconnect()
        except Exception as e:
            logger.warning(f"Ошибка при закрытии NATS соединения: {e}")

        await okc_client.close()


if __name__ == "__main__":
    asyncio.run(main())
