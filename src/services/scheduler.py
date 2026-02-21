import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from okc_py import OKC

from src.core.config import settings
from src.tasks.employees import (
    fill_birthdays,
    fill_employee_ids,
    fill_employment_dates,
)
from src.tasks.premium import fill_heads_premium, fill_specialists_premium
from src.tasks.tests import fill_assigned_tests
from src.tasks.tutors import fill_tutor_schedule
from src.tasks.ure import (
    fill_day_kpi,
    fill_month_kpi,
    fill_week_kpi,
)

try:
    from src.services.scheduler_tracker import record_job_execution

    TRACKER_AVAILABLE = True
except ImportError:
    TRACKER_AVAILABLE = False


class Scheduler:
    def __init__(
        self,
        okc_client: OKC,
        db_url: str | None = None,
        max_workers: int = 5,
    ):
        """Инициализация планировщика.

        Args:
            okc_client: Единый клиент OKC для всех API
            db_url: URL на БД (опционально, для сохранения задач)
            max_workers: Максимальное количество одновременных задач
        """
        self.logger = logging.getLogger(__name__)
        self.okc_client = okc_client
        self.max_workers = max_workers
        self._is_running = False
        self._shutdown_event = asyncio.Event()

        jobstores = {}
        if db_url:
            jobstores["default"] = SQLAlchemyJobStore(url=db_url)

        job_defaults = {
            "coalesce": True,  # Объединить несколько ожидающих задач
            "max_instances": 2,  # Максимальных параллельных экземпляров задач
            "misfire_grace_time": 300,  # 5 минут отсрочки для неуспешных задач
        }

        self.scheduler = AsyncIOScheduler(
            jobstores=jobstores,
            job_defaults=job_defaults,
            timezone=settings.SCHEDULER_TIMEZONE,
        )

        self.scheduler.add_listener(self._job_executed, EVENT_JOB_EXECUTED)
        self.scheduler.add_listener(self._job_error, EVENT_JOB_ERROR)

        self.job_stats: dict[str, Any] = {
            "executed": 0,
            "failed": 0,
            "last_execution": None,
            "last_error": None,
        }

    async def start_scheduling(self) -> None:
        """Запуск планировщика."""
        try:
            self.logger.info("Starting scheduler...")
            await self._setup_jobs()
            self.scheduler.start()
            self._is_running = True
            self.logger.info("Scheduler started successfully")

        except Exception as e:
            self.logger.error(f"Failed to start scheduler: {e}")
            raise

    async def stop_scheduling(self, wait: bool = True) -> None:
        """Останавливает планировщик.

        Args:
            wait: Ожидать ли завершения задач
        """
        try:
            self.logger.info("Stopping scheduler...")
            self._is_running = False
            self._shutdown_event.set()

            if wait:
                # Ждем завершения всех задач (максимум 30 секунд)
                try:
                    await asyncio.wait_for(self._wait_for_jobs(), timeout=30.0)
                except asyncio.TimeoutError:
                    self.logger.warning("Timeout waiting for jobs to complete")

            # Close jobstore connections before shutdown to avoid event loop warnings
            if "default" in self.scheduler._jobstores:
                jobstore = self.scheduler._jobstores["default"]
                if hasattr(jobstore, "engine"):
                    await jobstore.engine.dispose()

            self.scheduler.shutdown(wait=wait)
            self.logger.info("Scheduler stopped successfully")

        except Exception as e:
            self.logger.error(f"Error stopping scheduler: {e}")
            raise

    async def _setup_jobs(self) -> None:
        """Настройка всех задач."""
        await self._setup_employees()
        await self._setup_kpi()
        await self._setup_premium()
        await self._setup_tutors()
        await self._setup_tests()

    async def _setup_employees(self) -> None:
        """Настройка задач, связанных с сотрудниками."""
        self.scheduler.add_job(
            self._safe_job_wrapper(fill_employee_ids, "employees_ids"),
            trigger=IntervalTrigger(hours=24),
            args=[self.okc_client.api.dossier],
            id="employees_ids",
            name="Заполнение идентификатора OKC",
            replace_existing=True,
        )

        self.scheduler.add_job(
            self._safe_job_wrapper(fill_birthdays, "employees_birthdays"),
            trigger=IntervalTrigger(hours=24),
            args=[self.okc_client.api.dossier],
            id="employees_birthdays",
            name="Заполнение дней рождений",
            replace_existing=True,
        )

        self.scheduler.add_job(
            self._safe_job_wrapper(fill_employment_dates, "employees_employment_dates"),
            trigger=IntervalTrigger(hours=24),
            args=[self.okc_client.api.dossier],
            id="employees_employment_dates",
            name="Заполнение дат трудоустройства",
            replace_existing=True,
        )

        self.logger.info("[Планировщик] Задачи сотрудников настроены")

    async def _setup_kpi(self) -> None:
        """Настройка задач, связанных с показателями KPI."""
        self.scheduler.add_job(
            self._safe_job_wrapper(fill_day_kpi, "fill_day_kpi"),
            trigger=CronTrigger(hour=10, minute=0),
            args=[self.okc_client.api.ure],
            id="fill_day_kpi",
            name="Заполнение дневных показателей KPI",
            replace_existing=True,
        )

        self.scheduler.add_job(
            self._safe_job_wrapper(fill_week_kpi, "fill_week_kpi"),
            trigger=CronTrigger(day_of_week="mon", hour=10, minute=0),
            args=[self.okc_client.api.ure],
            id="fill_week_kpi",
            name="Заполнение недельных показателей KPI",
            replace_existing=True,
        )

        self.scheduler.add_job(
            self._safe_job_wrapper(fill_month_kpi, "fill_month_kpi"),
            trigger=CronTrigger(day=4, hour=10, minute=0),
            args=[self.okc_client.api.ure],
            id="fill_month_kpi",
            name="Заполнение месячных показателей KPI",
            replace_existing=True,
        )

        self.logger.info("[Планировщик] Задачи KPI настроены")

    async def _setup_premium(self) -> None:
        """Настройка задач, связанных с премиумом."""
        self.scheduler.add_job(
            self._safe_job_wrapper(fill_specialists_premium, "premium_specialists"),
            trigger=IntervalTrigger(hours=6),
            args=[self.okc_client.api.premium],
            id="premium_specialists",
            name="Заполнение премиума специалистов",
            replace_existing=True,
        )
        self.scheduler.add_job(
            self._safe_job_wrapper(fill_heads_premium, "premium_heads"),
            trigger=IntervalTrigger(hours=6),
            args=[self.okc_client.api.premium],
            id="premium_heads",
            name="Заполнение премиума руководителей",
            replace_existing=True,
        )
        self.logger.info("[Планировщик] Задачи премиума настроены")

    async def _setup_tutors(self) -> None:
        """Настройка задач, связанных с расписанием наставников."""
        self.scheduler.add_job(
            self._safe_job_wrapper(
                lambda api: fill_tutor_schedule(api, full_update=False),
                "tutors",
            ),
            trigger=IntervalTrigger(hours=12),
            args=[self.okc_client.api.tutors],
            id="tutors",
            name="Обновление расписания наставников",
            replace_existing=True,
        )

        self.logger.info("[Планировщик] Задачи наставников настроены")

    async def _setup_tests(self) -> None:
        """Настройка задач, связанных с тестами."""
        self.scheduler.add_job(
            self._safe_job_wrapper(fill_assigned_tests, "tests_current"),
            trigger=IntervalTrigger(hours=6),
            args=[self.okc_client.api.tests],
            id="tests_current",
            name="Заполнение назначенных тестов",
            replace_existing=True,
        )

        self.logger.info("[Планировщик] Задачи тестов настроены")

    def _safe_job_wrapper(self, job_func, job_name: str):
        """Обертка для задач."""

        async def wrapper(*args, **kwargs):
            start_time = datetime.now(settings.SCHEDULER_TIMEZONE)
            self.logger.info(f"Starting job: {job_name}")

            try:
                result = await job_func(*args, **kwargs)
                duration = (
                    datetime.now(settings.SCHEDULER_TIMEZONE) - start_time
                ).total_seconds()
                self.logger.info(
                    f"Job {job_name} completed successfully in {duration:.2f}s"
                )
                return result

            except Exception as e:
                duration = (
                    datetime.now(settings.SCHEDULER_TIMEZONE) - start_time
                ).total_seconds()
                self.logger.error(
                    f"Job {job_name} failed after {duration:.2f}s: {e}", exc_info=True
                )

                # Обновление статистики
                self.job_stats["failed"] += 1
                self.job_stats["last_error"] = {
                    "job": job_name,
                    "error": str(e),
                    "time": start_time.isoformat(),
                }

                raise

        return wrapper

    def _job_executed(self, event) -> None:
        if TRACKER_AVAILABLE:
            record_job_execution(event.job_id)
        self.job_stats["executed"] += 1
        self.job_stats["last_execution"] = datetime.now(
            settings.SCHEDULER_TIMEZONE
        ).isoformat()
        self.logger.debug(f"Job {event.job_id} executed successfully")

    def _job_error(self, event) -> None:
        """Обработка ошибок при выполнении задач."""
        self.job_stats["failed"] += 1
        self.job_stats["last_error"] = {
            "job": event.job_id,
            "error": str(event.exception),
            "time": datetime.now(settings.SCHEDULER_TIMEZONE).isoformat(),
        }
        self.logger.error(f"Job {event.job_id} failed: {event.exception}")

    async def _wait_for_jobs(self) -> None:
        """Ожидание завершения всех задач."""
        while True:
            running_jobs = [
                job for job in self.scheduler.get_jobs() if job.next_run_time
            ]
            if not running_jobs:
                break
            await asyncio.sleep(1)

    def pause_job(self, job_id: str) -> bool:
        """Пауза конкретной задачи."""
        try:
            self.scheduler.pause_job(job_id)
            self.logger.info(f"Job {job_id} paused")
            return True
        except Exception as e:
            self.logger.error(f"Failed to pause job {job_id}: {e}")
            return False

    def resume_job(self, job_id: str) -> bool:
        """Запуск задачи с паузы."""
        try:
            self.scheduler.resume_job(job_id)
            self.logger.info(f"Job {job_id} resumed")
            return True
        except Exception as e:
            self.logger.error(f"Failed to resume job {job_id}: {e}")
            return False

    def get_job_status(self) -> dict[str, Any]:
        """Получает статус всех задач."""
        jobs_info = []
        for job in self.scheduler.get_jobs():
            jobs_info.append(
                {
                    "id": job.id,
                    "name": job.name,
                    "next_run": job.next_run_time.isoformat()
                    if job.next_run_time
                    else None,
                    "trigger": str(job.trigger),
                }
            )

        return {
            "scheduler_running": self.scheduler.running,
            "jobs": jobs_info,
            "stats": self.job_stats,
        }

    def run_job_now(self, job_id: str) -> bool:
        """Ручной триггер запуска задач."""
        try:
            job = self.scheduler.get_job(job_id)
            if job:
                self.scheduler.modify_job(
                    job_id, next_run_time=datetime.now(settings.SCHEDULER_TIMEZONE)
                )
                self.logger.info(f"Job {job_id} scheduled to run immediately")
                return True
            else:
                self.logger.error(f"Job {job_id} not found")
                return False
        except Exception as e:
            self.logger.error(f"Failed to run job {job_id}: {e}")
            return False

    @asynccontextmanager
    async def managed_lifecycle(self):
        shutdown_gracefully = True
        try:
            await self.start_scheduling()
            yield self
        except asyncio.CancelledError:
            # Task cancellation - still shutdown gracefully but don't re-raise
            shutdown_gracefully = True
        except Exception:
            # Other errors - shutdown immediately
            shutdown_gracefully = False
            raise
        finally:
            await self.stop_scheduling(wait=shutdown_gracefully)
