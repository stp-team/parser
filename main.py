import asyncio
import logging

from okc_py import OKC
from okc_py.config import Settings
from src.core.redis import close_redis

from src.core.config import settings
from src.core.nats_client import nats_client
from src.core.ws_bridge import cleanup_ws_bridges
from src.services.logger import setup_logging
from src.services.parser_events import (
    parser_event_service,
    run_tracked_task,
)
from src.services.scheduler import Scheduler
from src.tasks.employees import fill_employees
from src.tasks.premium import (
    fill_heads_premium,
    fill_specialists_premium,
)
from src.tasks.sl import fill_sl
from src.tasks.tests import fill_assigned_tests
from src.tasks.tutors import fill_tutor_schedule
from src.tasks.ure import (
    fill_day_kpi,
    fill_month_kpi,
    fill_week_kpi,
)


try:
    from src.services.cli_dashboard import get_dashboard
    from src.services.scheduler_tracker import (
        update_scheduler_jobs,
    )

    DASHBOARD_AVAILABLE = True

except ImportError:
    DASHBOARD_AVAILABLE = False


async def run_startup_tasks(
    okc_client: OKC,
    logger: logging.Logger,
) -> None:
    """
    Первичная загрузка данных при старте parser.

    Все задачи отправляют события в Redis.
    """

    logger.info(
        "Запуск получения данных "
        "при старте парсера..."
    )

    if settings.DOSSIER_BULK_SYNC_ON_STARTUP:
        logger.info(
            "Догрузка досье при запуске включена"
        )

        await run_tracked_task(
            "employees_full_sync",
            fill_employees,
            okc_client.api.dossier,
            okc_client.api.tutors,
            task_title=(
                "Полная синхронизация сотрудников"
            ),
            source="startup",
        )

    else:
        logger.info(
            "Догрузка досье при запуске "
            "отключена"
        )

    # KPI запускаем параллельно,
    # как раньше внутри fill_kpi(),
    # но теперь получаем отдельное событие
    # для day/week/month.

    await asyncio.gather(
        run_tracked_task(
            "fill_day_kpi",
            fill_day_kpi,
            okc_client.api.ure,
            task_title="Заполнение дневных KPI",
            source="startup",
        ),
        run_tracked_task(
            "fill_week_kpi",
            fill_week_kpi,
            okc_client.api.ure,
            task_title="Заполнение недельных KPI",
            source="startup",
        ),
        run_tracked_task(
            "fill_month_kpi",
            fill_month_kpi,
            okc_client.api.ure,
            task_title="Заполнение месячных KPI",
            source="startup",
        ),
    )

    await run_tracked_task(
        "premium_heads",
        fill_heads_premium,
        okc_client.api.premium,
        task_title=(
            "Обновление премиума руководителей"
        ),
        source="startup",
    )

    await run_tracked_task(
        "premium_specialists",
        fill_specialists_premium,
        okc_client.api.premium,
        task_title=(
            "Обновление премиума специалистов"
        ),
        source="startup",
    )

    await run_tracked_task(
        "tutors",
        fill_tutor_schedule,
        okc_client.api.tutors,
        task_title=(
            "Обновление расписания наставников"
        ),
        source="startup",
    )

    await run_tracked_task(
        "sl",
        fill_sl,
        okc_client.api.sl,
        task_title=(
            "Обновление Service Level"
        ),
        source="startup",
    )

    await run_tracked_task(
        "tests_current",
        fill_assigned_tests,
        okc_client.api.tests,
        task_title=(
            "Обновление назначенных тестов"
        ),
        source="startup",
    )

    logger.info(
        "Получение данных "
        "при старте завершено"
    )


async def main():
    use_dashboard = (
        DASHBOARD_AVAILABLE
        and settings.ENABLE_DASHBOARD
    )

    dashboard = (
        get_dashboard()
        if use_dashboard
        else None
    )

    setup_logging(
        use_dashboard=use_dashboard
    )

    logger = logging.getLogger(
        __name__
    )

    okc_client = OKC(
        username=settings.OKC_USERNAME,
        password=settings.OKC_PASSWORD,
        settings=Settings(
            BASE_URL=settings.OKC_BASE_URL
        ),
    )

    parser_started = False

    if dashboard:
        logger.info(
            "Starting dashboard..."
        )

        dashboard.start()

        logger.info(
            "Dashboard started successfully"
        )

    else:
        logger.info(
            "Dashboard disabled "
            "or unavailable"
        )

    try:
        await okc_client.connect()

        await parser_event_service.service_started()

        parser_started = True

        db_url = (
            settings.SCHEDULER_JOB_STORE_URL
            if (
                settings.SCHEDULER_ENABLE_PERSISTENCE
                and settings.SCHEDULER_JOB_STORE_URL
            )
            else None
        )

        if db_url:
            logger.info(
                "Scheduler persistence "
                f"enabled with DB: {db_url}"
            )

        scheduler = Scheduler(
            okc_client=okc_client,
            db_url=db_url,
            max_workers=(
                settings.SCHEDULER_MAX_WORKERS
            ),
        )

        async with scheduler.managed_lifecycle():
            logger.info(
                "Планировщик запущен"
            )

            logger.info(
                "Dossier bulk sync config: "
                "mode=%s, "
                "custom_hours=%s, "
                "on_startup=%s",
                settings.DOSSIER_BULK_SYNC_MODE,
                settings.DOSSIER_BULK_SYNC_HOURS,
                settings.DOSSIER_BULK_SYNC_ON_STARTUP,
            )

            status = scheduler.get_job_status()

            logger.info(
                "Запланированные задачи: %s",
                len(status["jobs"]),
            )

            for job in status["jobs"]:
                logger.info(
                    "  - %s "
                    "(ID: %s) - "
                    "Next run: %s",
                    job["name"],
                    job["id"],
                    job["next_run"],
                )

            await run_startup_tasks(
                okc_client,
                logger,
            )

            try:
                while True:
                    await asyncio.sleep(2)

                    if DASHBOARD_AVAILABLE:
                        status = (
                            scheduler.get_job_status()
                        )

                        update_scheduler_jobs(
                            status["jobs"],
                            status[
                                "scheduler_running"
                            ],
                        )

                    if logger.isEnabledFor(
                        logging.DEBUG
                    ):
                        status = (
                            scheduler.get_job_status()
                        )

                        logger.debug(
                            "Scheduler stats: %s",
                            status["stats"],
                        )

            except KeyboardInterrupt:
                logger.info(
                    "Keyboard interrupt received. "
                    "Shutting down gracefully..."
                )

            except Exception as e:
                logger.error(
                    "Unexpected error "
                    f"in main loop: {e}",
                    exc_info=True,
                )

                raise

    except Exception as e:
        logger.error(
            f"Error in main: {e}",
            exc_info=True,
        )

        try:
            await parser_event_service.service_error(
                e
            )
        except Exception:
            pass

        raise

    finally:
        if dashboard:
            try:
                dashboard.stop()

                await asyncio.sleep(
                    0.2
                )

            except Exception:
                pass

        try:
            await cleanup_ws_bridges()

        except Exception as e:
            logger.warning(
                "Ошибка при закрытии "
                f"WebSocket bridges: {e}"
            )

        try:
            await nats_client.disconnect()

        except Exception as e:
            logger.warning(
                "Ошибка при закрытии "
                f"NATS соединения: {e}"
            )

        if parser_started:
            try:
                await (
                    parser_event_service
                    .service_stopped()
                )

            except Exception:
                pass

        try:
            await okc_client.close()

        except Exception as e:
            logger.warning(
                "Ошибка при закрытии "
                f"OKC: {e}"
            )

        try:
            await close_redis()

        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(
        main()
    )