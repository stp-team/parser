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

try:
    from src.services.scheduler_tracker import get_scheduler_tracker

    SCHEDULER_TRACKER_AVAILABLE = True
except ImportError:
    SCHEDULER_TRACKER_AVAILABLE = False


class LogHandler(logging.Handler):
    def __init__(self, log_queue: queue.Queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.log_queue.put(record)
        except Exception:
            self.handleError(record)


class CLIDashboard:
    def __init__(self, refresh_per_second: int = 2):
        self.console = Console()
        self.log_queue: queue.Queue[logging.LogRecord] = queue.Queue(maxsize=1000)
        self.log_messages: list[Text] = []
        self.max_logs = 10
        self.refresh_per_second = refresh_per_second
        self._running = False
        self._live: Live | None = None
        self._update_thread: threading.Thread | None = None

    def get_log_handler(self) -> logging.Handler:
        return LogHandler(self.log_queue)

    def _process_logs(self) -> None:
        while not self.log_queue.empty():
            try:
                record = self.log_queue.get_nowait()
                self.log_messages.append(self._format_log_record(record))
                if len(self.log_messages) > self.max_logs:
                    self.log_messages.pop(0)
            except queue.Empty:
                break

    def _format_log_record(self, record: logging.LogRecord) -> Text:
        timestamp = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        level = record.levelname
        name = record.name.split(".")[-1]
        message = record.getMessage()

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

    @staticmethod
    def _format_job_name(name: str) -> str:
        return name.replace("fill_", "").replace("_data", "").replace("_", " ").title()

    @staticmethod
    def _format_countdown(seconds: int) -> str:
        if seconds < 0:
            return "Now"

        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60

        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        if minutes > 0:
            return f"{minutes}m {secs}s"
        return f"{secs}s"

    def _create_scheduler_table(self) -> Table:
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

        for job in next_jobs:
            name = self._format_job_name(job["name"])
            countdown = self._format_countdown(job["seconds_until"])
            next_run_time = job["next_run"].strftime("%H:%M:%S")
            table.add_row(name, countdown, next_run_time)

        return table

    def _create_stats_table(
        self,
        title: str,
        header_style: str,
        columns: list[tuple[str, int, str]],
        stats_getter,
        empty_msg: str,
        name_formatter: str | None = None,
    ) -> Table:
        table = Table(
            title=title,
            show_header=True,
            header_style=header_style,
            border_style="bright_black",
        )

        for col_name, width, style in columns:
            table.add_column(col_name, style=style, width=width)

        stats = stats_getter(limit=6)
        if not stats:
            table.add_row(f"[dim]{empty_msg}[/dim]", "", "")
            return table

        for *values, last_time in stats:
            name = values[0]
            if name_formatter:
                name = name.replace(name_formatter, "").replace("/", " ")
            table.add_row(name, str(values[1]), last_time)

        return table

    def _create_api_table(self) -> Table:
        return self._create_stats_table(
            title="📊 Вызовы API",
            header_style="bold magenta",
            columns=[
                ("Endpoint", 25, "cyan"),
                ("Вызовов", 10, "green"),
                ("Последний", 10, "yellow"),
            ],
            stats_getter=api_tracker.get_stats,
            empty_msg="API вызовов не было",
            name_formatter="api/",
        )

    def _create_db_table(self) -> Table:
        return self._create_stats_table(
            title="💾 Записи в БД",
            header_style="bold cyan",
            columns=[
                ("Таблица", 20, "cyan"),
                ("Записей", 10, "green"),
                ("Последний", 10, "yellow"),
            ],
            stats_getter=db_tracker.get_stats,
            empty_msg="Записей в БД не было",
            name_formatter="_",
        )

    def _create_logs_panel(self) -> Panel:
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
            height=self.max_logs + 2,
        )

    def _generate_layout(self) -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="stats", size=13),
            Layout(name="logs", size=14),
        )

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

        if SCHEDULER_TRACKER_AVAILABLE:
            tracker = get_scheduler_tracker()
            status = tracker.get_scheduler_status()

            if status["next_job"]:
                next_job = status["next_job"]
                countdown = self._format_countdown(next_job["seconds_until"])
                task_name = self._format_job_name(next_job["name"])
                header_text.append("  |  ", style="dim")
                header_text.append("Следующая задача: ", style="dim")
                header_text.append(f"{task_name} in {countdown}", style="bold yellow")

        layout["header"].update(
            Panel(header_text, border_style="bright_black", padding=(0, 1))
        )

        scheduler_table = self._create_scheduler_table()
        api_table = self._create_api_table()
        db_table = self._create_db_table()
        layout["stats"].update(
            Columns([scheduler_table, api_table, db_table], equal=True)
        )
        layout["logs"].update(self._create_logs_panel())

        return layout

    def _update_loop(self) -> None:
        try:
            while self._running:
                try:
                    self._process_logs()
                    self._live.update(self._generate_layout())
                    time.sleep(1.0 / self.refresh_per_second)
                except Exception:
                    if not self._running:
                        break
        except (Exception, KeyboardInterrupt):
            pass
        finally:
            if self._live:
                try:
                    self._live.stop()
                except (Exception, KeyboardInterrupt):
                    pass

    def start(self) -> None:
        if self._running:
            return

        self._running = True

        self._live = Live(
            Text("Запускаем дашборд...", style="dim"),
            console=self.console,
            refresh_per_second=self.refresh_per_second,
        )

        try:
            self._live.start()
        except Exception as e:
            self.console.print(f"Failed to start Live display: {e}")
            self._running = False
            return

        self._update_thread = threading.Thread(target=self._update_loop, daemon=True)
        self._update_thread.start()

    def stop(self) -> None:
        if not self._running:
            return
        self._running = False

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


_dashboard: CLIDashboard | None = None
_dashboard_lock = threading.Lock()


def get_dashboard() -> CLIDashboard:
    global _dashboard

    with _dashboard_lock:
        if _dashboard is None:
            _dashboard = CLIDashboard()
        return _dashboard
