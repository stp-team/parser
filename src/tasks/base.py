import asyncio
import calendar
import logging
from collections.abc import Callable
from datetime import datetime
from time import perf_counter
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


def log_processing_time(operation_name: str):
    """Декоратор для логирования времени запуска задач."""

    def decorator(func):
        async def wrapper(*args, **kwargs):
            timer_start = perf_counter()
            logger.info(f"Starting {operation_name}")

            try:
                result = await func(*args, **kwargs)
                timer_stop = perf_counter()
                logger.info(
                    f"Completed {operation_name} in {timer_stop - timer_start:.2f}s"
                )
                return result
            except Exception as e:
                timer_stop = perf_counter()
                logger.error(
                    f"Error in {operation_name} after {timer_stop - timer_start:.2f}s: {e}"
                )
                raise

        return wrapper

    return decorator


class ConcurrentAPIFetcher:
    """Utility for parallel API requests with semaphore limiting."""

    def __init__(self, semaphore_limit: int = 10):
        self.semaphore_limit = semaphore_limit
        self.logger = logging.getLogger(self.__class__.__name__)

    async def fetch_parallel(
        self,
        tasks: list[tuple[Any, ...]],
        task_executor: Callable[..., Any],
    ) -> list[tuple[Any, ...]]:
        """
        Execute list of tasks in parallel with semaphore limiting.

        Args:
            tasks: List of tuples with parameters for task_executor
            task_executor: Function to execute each task

        Returns:
            List of results in same order as tasks
        """
        semaphore = asyncio.Semaphore(self.semaphore_limit)

        async def limited_task(task_params):
            async with semaphore:
                return await task_executor(*task_params)

        coroutines = [limited_task(task) for task in tasks]
        results = await asyncio.gather(*coroutines, return_exceptions=True)

        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.error(f"Task {i} failed: {result}")
                processed_results.append((tasks[i], None))
            else:
                processed_results.append((tasks[i], result))

        return processed_results


class BatchDBOperator:
    """Utility for bulk database operations."""

    def __init__(self, session: AsyncSession):
        self.session = session
        self.logger = logging.getLogger(self.__class__.__name__)

    async def bulk_insert_with_cleanup(
        self,
        data_list: list[Any],
        delete_func: Callable[[], Any] | None,
        operation_name: str,
    ) -> int:
        """
        Bulk insert with optional cleanup.

        Args:
            data_list: List of objects to insert
            delete_func: Optional function to delete old data
            operation_name: Operation name for logging

        Returns:
            Number of records inserted
        """
        if not data_list:
            self.logger.warning(f"[{operation_name}] No data to insert")
            return 0

        try:
            if delete_func:
                await delete_func()

            self.session.add_all(data_list)
            await self.session.commit()

            self.logger.info(f"[{operation_name}] Inserted {len(data_list)} records")
            return len(data_list)

        except Exception as e:
            self.logger.error(f"[{operation_name}] Bulk insert failed: {e}")
            await self.session.rollback()
            return 0

    async def bulk_update_with_transaction(
        self,
        updates: list[dict[str, Any]],
        update_func: Callable[[dict[str, Any]], Any],
        operation_name: str,
    ) -> int:
        """
        Execute bulk updates in a single transaction.

        Args:
            updates: List of dictionaries with update data
            update_func: Function to perform single update
            operation_name: Operation name for logging

        Returns:
            Number of records updated
        """
        if not updates:
            self.logger.info(f"[{operation_name}] No data to update")
            return 0

        try:
            updated_count = 0

            for update_data in updates:
                await update_func(update_data)
                updated_count += 1

            await self.session.commit()
            self.logger.info(
                f"[{operation_name}] Updated {updated_count} records in one transaction"
            )
            return updated_count

        except Exception as e:
            self.logger.error(f"[{operation_name}] Bulk update failed: {e}")
            await self.session.rollback()
            return 0


class PeriodHelper:
    """Utility for date and period operations."""

    @staticmethod
    def add_months(date: datetime, months: int) -> datetime:
        """Add months to date using only standard library."""
        year = date.year
        month = date.month + months

        while month > 12:
            year += 1
            month -= 12
        while month < 1:
            year -= 1
            month += 12

        day = min(date.day, calendar.monthrange(year, month)[1])
        return date.replace(year=year, month=month, day=day)

    @staticmethod
    def parse_period_string(period: str) -> tuple[datetime, datetime]:
        """
        Parse period string and return start/end dates.

        Args:
            period: Period in "YYYY-MM" or "YYYY-MM-DD" format

        Returns:
            Tuple (start_date, end_date)

        Examples:
            >>> start, end = PeriodHelper.parse_period_string("2024-12")
            >>> start  # 2024-12-01 00:00:00
            >>> end    # 2024-12-31 23:59:59
        """
        if "-" in period and len(period.split("-")) == 2:
            year, month = map(int, period.split("-"))
            start_date = datetime(year, month, 1)
            last_day = calendar.monthrange(year, month)[1]
            end_date = datetime(year, month, last_day, 23, 59, 59)
        elif "-" in period and len(period.split("-")) == 3:
            start_date = datetime.strptime(period, "%Y-%m-%d")
            end_date = start_date.replace(hour=23, minute=59, second=59)
        else:
            raise ValueError(f"Unsupported period format: {period}")

        return start_date, end_date

    @staticmethod
    def get_previous_months(
        months_count: int = 2, from_date: datetime = None
    ) -> list[str]:
        """
        Get list of previous months in YYYY-MM format.

        Args:
            months_count: Number of months to get
            from_date: Base date (default: current date)

        Returns:
            List of month strings

        Examples:
            >>> PeriodHelper.get_previous_months(2)  # if current is 2024-12
            ["2024-10", "2024-11"]
        """
        base_date = from_date or datetime.now()
        periods = []

        for i in range(months_count, 0, -1):
            target_date = PeriodHelper.add_months(base_date, -i)
            periods.append(target_date.strftime("%Y-%m"))

        return periods

    @staticmethod
    def get_date_range_for_period(period: str) -> tuple[str, str]:
        """
        Get date range for period.

        Args:
            period: Period in "YYYY-MM" format

        Returns:
            Tuple (start_date_str, end_date_str) in "DD.MM.YYYY" format

        Examples:
            >>> PeriodHelper.get_date_range_for_period("2024-12")
            ("01.12.2024", "31.12.2024")
        """
        start_date, end_date = PeriodHelper.parse_period_string(period)
        return start_date.strftime("%d.%m.%Y"), end_date.strftime("%d.%m.%Y")

    @staticmethod
    def format_date_for_api(date_obj: datetime, api_format: str = "DD.MM.YYYY") -> str:
        """
        Format date for API requests.

        Args:
            date_obj: datetime object
            api_format: Format for API ("DD.MM.YYYY" or "YYYY-MM-DD")

        Returns:
            Formatted date string

        Examples:
            >>> dt = datetime(2024, 12, 15)
            >>> PeriodHelper.format_date_for_api(dt, "DD.MM.YYYY")
            "15.12.2024"
        """
        if api_format == "DD.MM.YYYY":
            return date_obj.strftime("%d.%m.%Y")
        elif api_format == "YYYY-MM-DD":
            return date_obj.strftime("%Y-%m-%d")
        else:
            raise ValueError(f"Unsupported API format: {api_format}")
