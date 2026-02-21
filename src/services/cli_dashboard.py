"""CLI Dashboard with live updating table and logs."""

import logging
import queue
import threading
import time
from datetime import datetime, timezone

from rich.columns import Columns
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from src.services.api_tracker import api_tracker, db_tracker

# Optional scheduler tracker import
try:
    from src.services.scheduler_tracker import get_scheduler_tracker
    SCHEDULER_TRACKER_AVAILABLE = True
except ImportError:
    SCHEDULER_TRACKER_AVAILABLE = False


class LogHandler(logging.Handler):
    """Custom logging handler that sends logs to a queue."""

    def __init__(self, log_queue: queue.Queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record: logging.LogRecord) -> None:
        """Send log record to queue."""
        try:
            self.log_queue.put(record)
        except Exception:
            self.handleError(record)


class CLIDashboard:
    """CLI Dashboard with live updating table and logs."""

    def __init__(self, refresh_per_second: int = 2):
        """Initialize the CLI dashboard.

        Args:
            refresh_per_second: Number of times per second to refresh the display
        """
        self.console = Console()
        self.log_queue: queue.Queue[logging.LogRecord] = queue.Queue(maxsize=1000)
        self.log_messages: list[Text] = []
        self.max_logs = 10  # Number of logs to display
        self.refresh_per_second = refresh_per_second
        self._running = False
        self._live: Live | None = None
        self._update_thread: threading.Thread | None = None

    def get_log_handler(self) -> logging.Handler:
        """Get a log handler that feeds into this dashboard.

        Returns:
            A logging.Handler instance
        """
        return LogHandler(self.log_queue)

    def _process_logs(self) -> None:
        """Process pending log records from the queue."""
        while not self.log_queue.empty():
            try:
                record = self.log_queue.get_nowait()
                log_line = self._format_log_record(record)
                self.log_messages.append(log_line)
                # Keep only the most recent logs
                if len(self.log_messages) > self.max_logs:
                    self.log_messages.pop(0)
            except queue.Empty:
                break

    def _format_log_record(self, record: logging.LogRecord) -> Text:
        """Format a log record for display.

        Args:
            record: The log record to format

        Returns:
            Formatted log Text object
        """
        timestamp = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        level = record.levelname
        name = record.name.split(".")[-1]  # Get last part of module name
        message = record.getMessage()

        # Color coding based on level
        level_colors = {
            "DEBUG": "dim white",
            "INFO": "bright_blue",
            "WARNING": "bright_yellow",
            "ERROR": "bright_red",
            "CRITICAL": "bold red",
        }
        level_style = level_colors.get(level, "white")

        text = Text()
        text.append(f"[{timestamp}] ", style="dim")
        text.append(f"{level:8}", style=level_style)
        text.append(f" [{name}] ", style="dim")
        text.append(message)

        return text

    def _format_countdown(self, seconds: int) -> str:
        """Format seconds into a readable countdown string.

        Args:
            seconds: Number of seconds

        Returns:
            Formatted string like "2h 30m 15s"
        """
        if seconds < 0:
            return "Now"

        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60

        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        elif minutes > 0:
            return f"{minutes}m {secs}s"
        else:
            return f"{secs}s"

    def _create_scheduler_table(self) -> Table:
        """Create the scheduler countdown table.

        Returns:
            Rich Table object
        """
        table = Table(
            title="⏰ Следующие задачи",
            show_header=True,
            header_style="bold yellow",
            border_style="bright_black",
        )
        table.add_column("Задача", style="cyan", width=25)
        table.add_column("Через", justify="right", style="green", width=10)
        table.add_column("В", justify="right", style="yellow", width=8)

        if not SCHEDULER_TRACKER_AVAILABLE:
            table.add_row("[dim]Scheduler tracking unavailable[/dim]", "", "")
            return table

        tracker = get_scheduler_tracker()
        next_jobs = tracker.get_next_jobs(limit=5)

        if not next_jobs:
            table.add_row("[dim]Нет запланированных задач[/dim]", "", "")
            return table

        now = datetime.now(timezone.utc)
        for job in next_jobs:
            # Format job name - remove common prefix
            name = job["name"].replace("fill_", "").replace("_data", "").replace("_", " ").title()
            countdown = self._format_countdown(job["seconds_until"])
            next_run_time = job["next_run"].strftime("%H:%M:%S")
            table.add_row(name, countdown, next_run_time)

        return table

    def _create_api_table(self) -> Table:
        """Create the API endpoint statistics table.

        Returns:
            Rich Table object
        """
        table = Table(
            title="📊 Вызовы API",
            show_header=True,
            header_style="bold magenta",
            border_style="bright_black",
        )
        table.add_column("Endpoint", style="cyan", width=25)
        table.add_column("Вызовов", justify="right", style="green", width=10)
        table.add_column("Последний", justify="right", style="yellow", width=10)

        stats = api_tracker.get_stats(limit=6)
        if not stats:
            table.add_row("[dim]API вызовов не было[/dim]", "", "")
        else:
            for endpoint, count, last_called in stats:
                # Shorten endpoint names
                short_endpoint = endpoint.replace("GET /api/", "").replace("POST /api/", "").replace("/", " ")
                table.add_row(short_endpoint, str(count), last_called)

        return table

    def _create_db_table(self) -> Table:
        """Create the database write statistics table.

        Returns:
            Rich Table object
        """
        table = Table(
            title="💾 Записи в БД",
            show_header=True,
            header_style="bold cyan",
            border_style="bright_black",
        )
        table.add_column("Таблица", style="cyan", width=20)
        table.add_column("Записей", justify="right", style="green", width=10)
        table.add_column("Последний", justify="right", style="yellow", width=10)

        stats = db_tracker.get_stats(limit=6)
        if not stats:
            table.add_row("[dim]Записей в БД не было[/dim]", "", "")
        else:
            for table_name, count, last_written in stats:
                # Format table name nicely
                nice_name = table_name.replace("_", " ").title()
                table.add_row(nice_name, str(count), last_written)

        return table

    def _create_logs_panel(self) -> Panel:
        """Create the logs panel.

        Returns:
            Rich Panel object
        """
        if not self.log_messages:
            logs_text = Text("Ожидаем логи...", style="dim")
        else:
            logs_text = Text()
            for msg in self.log_messages:
                logs_text.append(msg)
                logs_text.append("\n")

        return Panel(
            logs_text,
            title=Text("📝 Логи", style="bold blue"),
            border_style="bright_black",
            height=self.max_logs + 2,  # +2 for panel borders
        )

    def _generate_layout(self) -> Layout:
        """Generate the dashboard layout.

        Returns:
            Rich Layout object
        """
        layout = Layout()

        # Split into header, stats area, and logs
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="stats", size=13),
            Layout(name="logs", size=14),
        )

        # Header with title, stats, and next task
        total_api_calls = api_tracker.get_total_calls()
        total_db_writes = db_tracker.get_total_writes()

        header_text = Text()
        header_text.append("🚀 ", style="bold red")
        header_text.append("Парсер STP", style="bold white")
        header_text.append("  |  ", style="dim")
        header_text.append("API: ", style="dim")
        header_text.append(str(total_api_calls), style="bold green")
        header_text.append("  |  ", style="dim")
        header_text.append("БД: ", style="dim")
        header_text.append(str(total_db_writes), style="bold cyan")
        header_text.append(f"  |  {datetime.now().strftime('%H:%M:%S')}", style="dim")

        # Add next task info if available
        if SCHEDULER_TRACKER_AVAILABLE:
            tracker = get_scheduler_tracker()
            status = tracker.get_scheduler_status()

            if status["next_job"]:
                next_job = status["next_job"]
                countdown = self._format_countdown(next_job["seconds_until"])
                task_name = next_job["name"].replace("fill_", "").replace("_data", "").replace("_", " ").title()
                header_text.append("  |  ", style="dim")
                header_text.append("Следующая задача: ", style="dim")
                header_text.append(f"{task_name} in {countdown}", style="bold yellow")

        layout["header"].update(
            Panel(header_text, border_style="bright_black", padding=(0, 1))
        )

        # Stats area with three tables side by side
        scheduler_table = self._create_scheduler_table()
        api_table = self._create_api_table()
        db_table = self._create_db_table()
        stats_columns = Columns([scheduler_table, api_table, db_table], equal=True)
        layout["stats"].update(stats_columns)

        # Logs panel
        layout["logs"].update(self._create_logs_panel())

        return layout

    def _update_loop(self) -> None:
        """Background thread that continuously updates the display."""
        try:
            while self._running:
                try:
                    self._process_logs()
                    layout = self._generate_layout()
                    self._live.update(layout)
                    time.sleep(1.0 / self.refresh_per_second)
                except Exception as e:
                    # Live display might have been stopped or interrupted
                    if self._running:
                        break
        except (Exception, KeyboardInterrupt):
            # Thread interrupted - exit cleanly
            pass
        finally:
            # Stop Live display when thread exits
            if self._live:
                try:
                    self._live.stop()
                except (Exception, KeyboardInterrupt):
                    # Ignore errors during shutdown
                    pass

    def start(self) -> None:
        """Start the live dashboard display."""
        if self._running:
            return

        self._running = True

        # Create Live display with a simple initial layout
        from rich.text import Text
        initial_layout = Text("Запускаем дашборд...", style="dim")

        self._live = Live(
            initial_layout,
            console=self.console,
            refresh_per_second=self.refresh_per_second,
        )

        # Start Live display in the main thread
        try:
            self._live.start()
        except Exception as e:
            self.console.print(f"Failed to start Live display: {e}")
            self._running = False
            return

        # Start background update thread to keep the display updated
        self._update_thread = threading.Thread(target=self._update_loop, daemon=True)
        self._update_thread.start()

    def stop(self) -> None:
        """Stop the live dashboard display."""
        if not self._running:
            return

        # Signal the update thread to stop
        self._running = False

        # The daemon thread will clean up itself in its finally block

    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()


# Global dashboard instance
_dashboard: CLIDashboard | None = None
_dashboard_lock = threading.Lock()


def get_dashboard() -> CLIDashboard:
    """Get the global dashboard instance.

    Returns:
        CLIDashboard instance
    """
    global _dashboard

    with _dashboard_lock:
        if _dashboard is None:
            _dashboard = CLIDashboard()
        return _dashboard
