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

from app.api.employees import EmployeesAPI
from app.api.kpi import KpiAPI
from app.api.premium import PremiumAPI
from app.api.tests import TestsAPI
from app.api.tutors import TutorsAPI
from app.core.config import settings
from app.tasks.employees.employees import (
    fill_birthdays,
    fill_employee_ids,
    fill_employment_dates,
    fill_tutors,
)
from app.tasks.kpi.kpi import fill_day_kpi, fill_month_kpi, fill_week_kpi
from app.tasks.premium.premium import fill_heads_premium, fill_specialists_premium
from app.tasks.tests.tests import fill_current_tests
from app.tasks.tutors.tutors import fill_tutor_schedule


class Scheduler:
    def __init__(
        self,
        employees_api: EmployeesAPI,
        kpi_api: KpiAPI,
        premium_api: PremiumAPI,
        tutors_api: TutorsAPI,
        tests_api: TestsAPI,
        db_url: str | None = None,
        max_workers: int = 5,
    ):
        """Инициализация планировщика.

        Args:
            employees_api: Экземпляр API сотрудников
            kpi_api: Экземпляр API KPI
            premium_api: Экземпляр API премиума
            tutors_api: Экземпляр API наставников
            tests_api: Экземпляр API тестов
            db_url: URL на БД (опционально, для сохранения задач)
            max_workers: Максимальное количество одновременных задач
        """
        self.logger = logging.getLogger(__name__)
        self.employees_api = employees_api
        self.kpi_api = kpi_api
        self.premium_api = premium_api
        self.tutors_api = tutors_api
        self.tests_api = tests_api
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

            asyncio.create_task(self._health_check_loop())

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

        self.scheduler.add_job(
            self._scheduler_health_check,
            trigger=IntervalTrigger(minutes=30),
            id="scheduler_health_check",
            name="Health Check",
            replace_existing=True,
        )

    async def _setup_employees(self) -> None:
        """Настройка задач, связанных с сотрудниками."""
        self.scheduler.add_job(
            self._safe_job_wrapper(fill_employee_ids, "employees_ids"),
            trigger=IntervalTrigger(hours=2),
            args=[self.employees_api],
            id="employees_ids",
            name="Заполнение идентификатора OKC",
            replace_existing=True,
        )

        self.scheduler.add_job(
            self._safe_job_wrapper(fill_birthdays, "employees_birthdays"),
            trigger=IntervalTrigger(hours=2),
            args=[self.employees_api],
            id="employees_birthdays",
            name="Заполнение дней рождений",
            replace_existing=True,
        )

        self.scheduler.add_job(
            self._safe_job_wrapper(fill_employment_dates, "employees_employment_dates"),
            trigger=IntervalTrigger(hours=2),
            args=[self.employees_api],
            id="employees_employment_dates",
            name="Заполнение дат трудоустройства",
            replace_existing=True,
        )

        self.scheduler.add_job(
            self._safe_job_wrapper(fill_tutors, "employees_tutors"),
            trigger=IntervalTrigger(minutes=5),
            args=[self.tutors_api, self.employees_api],
            id="employees_tutors",
            name="Заполнение наставников",
            replace_existing=True,
        )

        self.logger.info("[Планировщик] Задачи сотрудников настроены")

    async def _setup_kpi(self) -> None:
        """Настройка задач, связанных с показателями KPI."""
        self.scheduler.add_job(
            self._safe_job_wrapper(fill_day_kpi, "fill_day_kpi"),
            trigger=CronTrigger(hour=10, minute=0),
            args=[self.kpi_api],
            id="fill_day_kpi",
            name="Заполнение дневных показателей KPI",
            replace_existing=True,
        )

        self.scheduler.add_job(
            self._safe_job_wrapper(fill_week_kpi, "fill_week_kpi"),
            trigger=CronTrigger(day_of_week="mon", hour=10, minute=0),
            args=[self.kpi_api],
            id="fill_week_kpi",
            name="Заполнение недельных показателей KPI",
            replace_existing=True,
        )

        self.scheduler.add_job(
            self._safe_job_wrapper(fill_month_kpi, "fill_month_kpi"),
            trigger=CronTrigger(day=4, hour=10, minute=0),
            args=[self.kpi_api],
            id="fill_month_kpi",
            name="Заполнение месячных показателей KPI",
            replace_existing=True,
        )

        self.logger.info("[Планировщик] Задачи KPI настроены")

    async def _setup_premium(self) -> None:
        """Настройка задач, связанных с премиумом."""
        self.scheduler.add_job(
            self._safe_job_wrapper(fill_specialists_premium, "premium_specialists"),
            trigger=IntervalTrigger(minutes=15),
            args=[self.premium_api],
            id="premium_specialists",
            name="Заполнение премиума специалистов",
            replace_existing=True,
        )
        self.scheduler.add_job(
            self._safe_job_wrapper(fill_heads_premium, "premium_heads"),
            trigger=IntervalTrigger(minutes=15),
            args=[self.premium_api],
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
            trigger=IntervalTrigger(minutes=5),
            args=[self.tutors_api],
            id="tutors",
            name="Обновление расписания наставников",
            replace_existing=True,
        )

        self.logger.info("[Планировщик] Задачи наставников настроены")

    async def _setup_tests(self) -> None:
        """Настройка задач, связанных с тестами."""
        self.scheduler.add_job(
            self._safe_job_wrapper(fill_current_tests, "tests_current"),
            trigger=IntervalTrigger(minutes=10),
            args=[self.tests_api],
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
        """Обработка успешно завершенных задач."""
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

    async def _health_check_loop(self) -> None:
        """Health check планировщика."""
        while self._is_running:
            try:
                await asyncio.sleep(300)  # Проверка каждые 5 минут
                if self._is_running:
                    await self._scheduler_health_check()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Health check loop error: {e}")

    async def _scheduler_health_check(self) -> None:
        """Выполняет проверку работоспособности планировщика."""
        try:
            if not self.scheduler.running:
                self.logger.warning("Scheduler is not running!")
                return

            # Проверяем зависшие задачи (работают дольше чем положено)
            current_time = datetime.now(settings.SCHEDULER_TIMEZONE)
            stuck_jobs = []

            for job in self.scheduler.get_jobs():
                if (
                    job.next_run_time
                    and (current_time - job.next_run_time).total_seconds() > 3600
                ):  # 1 час
                    stuck_jobs.append(job.id)

            if stuck_jobs:
                self.logger.warning(f"Potentially stuck jobs detected: {stuck_jobs}")

            # Логирование статистики
            self.logger.info(f"Scheduler health: {self.job_stats}")

        except Exception as e:
            self.logger.error(f"Health check failed: {e}")

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
        try:
            await self.start_scheduling()
            yield self
        finally:
            await self.stop_scheduling()
