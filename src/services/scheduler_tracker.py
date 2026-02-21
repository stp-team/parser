from datetime import datetime, timezone
from threading import Lock
from typing import Any


class SchedulerTracker:
    _instance = None
    _lock = Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._initialized = True
        self._jobs: list[dict[str, Any]] = []
        self._scheduler_running = False
        self._execution_counts: dict[str, int] = {}
        self._lock = Lock()

    def update_jobs(self, jobs: list[dict[str, Any]], scheduler_running: bool) -> None:
        with self._lock:
            self._jobs = jobs
            self._scheduler_running = scheduler_running

    def record_execution(self, job_id: str) -> None:
        with self._lock:
            self._execution_counts[job_id] = self._execution_counts.get(job_id, 0) + 1

    def get_execution_count(self, job_id: str) -> int:
        with self._lock:
            return self._execution_counts.get(job_id, 0)

    def _get_next_jobs_unlocked(self, limit: int = 5) -> list[dict[str, Any]]:
        if not self._scheduler_running or not self._jobs:
            return []

        now = datetime.now(timezone.utc)
        next_jobs = []

        for job in self._jobs:
            if not job.get("next_run"):
                continue

            try:
                next_run = datetime.fromisoformat(job["next_run"])
                seconds_until = int((next_run - now).total_seconds())

                if seconds_until > 0:
                    next_jobs.append(
                        {
                            "name": job["name"],
                            "next_run": next_run,
                            "seconds_until": seconds_until,
                            "executions": self._execution_counts.get(job["id"], 0),
                        }
                    )
            except (ValueError, TypeError):
                continue

        next_jobs.sort(key=lambda x: x["seconds_until"])
        return next_jobs[:limit]

    def get_next_jobs(self, limit: int = 5) -> list[dict[str, Any]]:
        with self._lock:
            return self._get_next_jobs_unlocked(limit)

    def get_scheduler_status(self) -> dict[str, Any]:
        with self._lock:
            next_jobs = self._get_next_jobs_unlocked(limit=1)

            return {
                "running": self._scheduler_running,
                "total_jobs": len(self._jobs),
                "next_job": next_jobs[0] if next_jobs else None,
            }


scheduler_tracker = SchedulerTracker()


def update_scheduler_jobs(jobs: list[dict[str, Any]], scheduler_running: bool) -> None:
    scheduler_tracker.update_jobs(jobs, scheduler_running)


def record_job_execution(job_id: str) -> None:
    scheduler_tracker.record_execution(job_id)


def get_scheduler_tracker() -> SchedulerTracker:
    return scheduler_tracker
