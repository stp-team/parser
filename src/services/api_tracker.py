"""API endpoint usage tracker."""

import logging
from collections import defaultdict
from datetime import datetime
from threading import Lock

logger = logging.getLogger(__name__)


class APITracker:
    """Track API endpoint usage statistics."""

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
        self._call_counts: defaultdict[str, int] = defaultdict(int)
        self._call_times: dict[str, datetime] = {}
        self._lock = Lock()

    def record_call(self, endpoint: str, method: str = "GET") -> None:
        """Record an API call to an endpoint.

        Args:
            endpoint: The API endpoint path (e.g., "/api/employees")
            method: HTTP method (default: "GET")
        """
        key = f"{method} {endpoint}"
        with self._lock:
            self._call_counts[key] += 1
            self._call_times[key] = datetime.now()

    def get_stats(self, limit: int = 10) -> list[tuple[str, int, str]]:
        """Get current statistics for all endpoints.

        Args:
            limit: Maximum number of endpoints to return

        Returns:
            List of tuples (endpoint, count, last_called_time)
        """
        with self._lock:
            stats = []
            for endpoint, count in sorted(
                self._call_counts.items(), key=lambda x: x[1], reverse=True
            )[:limit]:
                last_called = self._call_times.get(endpoint)
                last_called_str = (
                    last_called.strftime("%H:%M:%S") if last_called else "N/A"
                )
                stats.append((endpoint, count, last_called_str))
            return stats

    def get_total_calls(self) -> int:
        """Get total number of API calls tracked."""
        with self._lock:
            return sum(self._call_counts.values())

    def reset(self) -> None:
        """Reset all tracking statistics."""
        with self._lock:
            self._call_counts.clear()
            self._call_times.clear()


# Global instance
api_tracker = APITracker()


def track_api_call(endpoint: str, method: str = "GET") -> None:
    """Convenience function to record an API call.

    Args:
        endpoint: The API endpoint path
        method: HTTP method (default: "GET")
    """
    api_tracker.record_call(endpoint, method)


class DBTracker:
    """Track database write statistics."""

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
        self._write_counts: defaultdict[str, int] = defaultdict(int)
        self._write_times: dict[str, datetime] = {}
        self._lock = Lock()

    def record_write(self, table_name: str) -> None:
        """Record a database write to a table.

        Args:
            table_name: The database table name (e.g., "employees", "spec_day_kpi")
        """
        with self._lock:
            self._write_counts[table_name] += 1
            self._write_times[table_name] = datetime.now()

    def get_stats(self, limit: int = 10) -> list[tuple[str, int, str]]:
        """Get current statistics for all tables.

        Args:
            limit: Maximum number of tables to return

        Returns:
            List of tuples (table_name, count, last_write_time)
        """
        with self._lock:
            stats = []
            for table_name, count in sorted(
                self._write_counts.items(), key=lambda x: x[1], reverse=True
            )[:limit]:
                last_written = self._write_times.get(table_name)
                last_written_str = (
                    last_written.strftime("%H:%M:%S") if last_written else "N/A"
                )
                stats.append((table_name, count, last_written_str))
            return stats

    def get_total_writes(self) -> int:
        """Get total number of database writes tracked."""
        with self._lock:
            return sum(self._write_counts.values())

    def reset(self) -> None:
        """Reset all tracking statistics."""
        with self._lock:
            self._write_counts.clear()
            self._write_times.clear()


# Global instance
db_tracker = DBTracker()


def track_db_write(table_name: str) -> None:
    """Convenience function to record a database write.

    Args:
        table_name: The database table name
    """
    db_tracker.record_write(table_name)
