"""Scheduler job tracker for dashboard."""

import logging
from datetime import datetime, timezone
from threading import Lock
from typing import Any

logger = logging.getLogger(__name__)


class SchedulerTracker:
    """Track scheduler job information."""

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
        self._lock = Lock()

    def update_jobs(self, jobs: list[dict[str, Any]], scheduler_running: bool) -> None:
        """Update scheduler job information.

        Args:
            jobs: List of job dictionaries with 'name', 'next_run', 'trigger'
            scheduler_running: Whether the scheduler is running
        """
        with self._lock:
            self._jobs = jobs
            self._scheduler_running = scheduler_running

    def _get_next_jobs_unlocked(self, limit: int = 5) -> list[dict[str, Any]]:
        """Internal method to get next jobs (assuming lock is already held).

        Args:
            limit: Maximum number of jobs to return

        Returns:
            List of job dictionaries with countdown info
        """
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
                    next_jobs.append({
                        "name": job["name"],
                        "next_run": next_run,
                        "seconds_until": seconds_until,
                        "trigger": job.get("trigger", ""),
                    })
            except (ValueError, TypeError):
                continue

        # Sort by time until next run
        next_jobs.sort(key=lambda x: x["seconds_until"])
        return next_jobs[:limit]

    def get_next_jobs(self, limit: int = 5) -> list[dict[str, Any]]:
        """Get the next jobs to run, sorted by next_run time.

        Args:
            limit: Maximum number of jobs to return

        Returns:
            List of job dictionaries with countdown info
        """
        with self._lock:
            return self._get_next_jobs_unlocked(limit)

    def get_scheduler_status(self) -> dict[str, Any]:
        """Get current scheduler status.

        Returns:
            Dict with 'running', 'total_jobs', 'next_job' info
        """
        with self._lock:
            next_jobs = self._get_next_jobs_unlocked(limit=1)

            return {
                "running": self._scheduler_running,
                "total_jobs": len(self._jobs),
                "next_job": next_jobs[0] if next_jobs else None,
            }


# Global instance
scheduler_tracker = SchedulerTracker()


def update_scheduler_jobs(jobs: list[dict[str, Any]], scheduler_running: bool) -> None:
    """Convenience function to update scheduler job information.

    Args:
        jobs: List of job dictionaries
        scheduler_running: Whether the scheduler is running
    """
    scheduler_tracker.update_jobs(jobs, scheduler_running)


def get_scheduler_tracker() -> SchedulerTracker:
    """Get the global scheduler tracker instance.

    Returns:
        SchedulerTracker instance
    """
    return scheduler_tracker
