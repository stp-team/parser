"""CLI Dashboard with live updating table and logs."""

import logging
import queue
import threading
import time
from datetime import datetime

from rich.columns import Columns
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from src.services.api_tracker import api_tracker, db_tracker


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
        self.max_logs = 12  # Number of logs to display
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

    def _create_api_table(self) -> Table:
        """Create the API endpoint statistics table.

        Returns:
            Rich Table object
        """
        table = Table(
            title="📊 API Calls",
            show_header=True,
            header_style="bold magenta",
            border_style="bright_black",
        )
        table.add_column("Endpoint", style="cyan", width=35)
        table.add_column("Calls", justify="right", style="green", width=6)
        table.add_column("Last", justify="right", style="yellow", width=8)

        stats = api_tracker.get_stats(limit=8)
        if not stats:
            table.add_row("[dim]No API calls yet[/dim]", "", "")
        else:
            for endpoint, count, last_called in stats:
                # Shorten endpoint names
                short_endpoint = endpoint.replace("/api/", "").replace("/", " ")
                table.add_row(short_endpoint, str(count), last_called)

        return table

    def _create_db_table(self) -> Table:
        """Create the database write statistics table.

        Returns:
            Rich Table object
        """
        table = Table(
            title="💾 DB Writes",
            show_header=True,
            header_style="bold cyan",
            border_style="bright_black",
        )
        table.add_column("Table", style="cyan", width=25)
        table.add_column("Writes", justify="right", style="green", width=6)
        table.add_column("Last", justify="right", style="yellow", width=8)

        stats = db_tracker.get_stats(limit=8)
        if not stats:
            table.add_row("[dim]No DB writes yet[/dim]", "", "")
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
            logs_text = Text("Waiting for logs...", style="dim")
        else:
            logs_text = Text()
            for msg in self.log_messages:
                logs_text.append(msg)
                logs_text.append("\n")

        return Panel(
            logs_text,
            title=Text("📝 Logs", style="bold blue"),
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
            Layout(name="stats", size=14),
            Layout(name="logs", size=16),
        )

        # Header with title and stats
        total_api_calls = api_tracker.get_total_calls()
        total_db_writes = db_tracker.get_total_writes()
        header_text = Text()
        header_text.append("🚀 ", style="bold red")
        header_text.append("Parser Dashboard", style="bold white")
        header_text.append("  |  ", style="dim")
        header_text.append("API: ", style="dim")
        header_text.append(str(total_api_calls), style="bold green")
        header_text.append("  |  ", style="dim")
        header_text.append("DB: ", style="dim")
        header_text.append(str(total_db_writes), style="bold cyan")
        header_text.append(f"  |  {datetime.now().strftime('%H:%M:%S')}", style="dim")

        layout["header"].update(
            Panel(header_text, border_style="bright_black", padding=(0, 1))
        )

        # Stats area with two tables side by side
        api_table = self._create_api_table()
        db_table = self._create_db_table()
        stats_columns = Columns([api_table, db_table], equal=True)
        layout["stats"].update(stats_columns)

        # Logs panel
        layout["logs"].update(self._create_logs_panel())

        return layout

    def _update_loop(self) -> None:
        """Background thread that continuously updates the display."""
        while self._running and self._live:
            try:
                self._process_logs()
                layout = self._generate_layout()
                self._live.update(layout)
                time.sleep(1.0 / self.refresh_per_second)
            except Exception:
                # Live display might have been stopped
                break

    def start(self) -> None:
        """Start the live dashboard display."""
        if self._running:
            return

        self._running = True

        # Create Live display
        self._live = Live(
            self._generate_layout(),
            console=self.console,
            refresh_per_second=self.refresh_per_second,
        )
        self._live.start()

        # Start background update thread
        self._update_thread = threading.Thread(target=self._update_loop, daemon=True)
        self._update_thread.start()

    def stop(self) -> None:
        """Stop the live dashboard display."""
        if not self._running:
            return

        self._running = False

        if self._live:
            self._live.stop()
            self._live = None

        if self._update_thread:
            self._update_thread.join(timeout=1.0)
            self._update_thread = None

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
