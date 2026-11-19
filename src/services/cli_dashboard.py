
"""Терминальный мониторинг STP Parser."""

import logging
import os
import queue
import threading
import time
import traceback
from collections import deque
from datetime import datetime
from math import ceil

from rich import box
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from src.core.config import settings
from src.services.monitor_schedule import next_report_run
from src.services.monitor_state import monitor


LOG_MODE = settings.PARSER_LOG_MODE

class LogHandler(logging.Handler):
    """Передаёт записи logging в очередь Rich."""

    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        try:
            self.log_queue.put_nowait(record)
        except queue.Full:
            # Переполненный экранный буфер
            # не должен тормозить парсер.
            pass
        except Exception:
            self.handleError(record)


class CLIDashboard:
    def __init__(self, refresh_per_second=2):
        self.console = Console()
        self.refresh_per_second = refresh_per_second

        self.log_queue = queue.Queue(maxsize=1000)
        self.log_messages = deque(maxlen=1000)

        self._running = False
        self._live = None
        self._update_thread = None

    def get_log_handler(self):
        return LogHandler(self.log_queue)

    @staticmethod
    def _visible(record):
        if LOG_MODE == "debug":
            return True

        if record.levelno >= logging.WARNING:
            return True

        message = record.getMessage()
        name = record.name

        if name.endswith("dynamic_reports"):
            if "[PARSER] Прогон завершён" in message:
                return True

            if LOG_MODE == "normal":
                return (
                    "[PARSER]" in message
                    or "[PREMIUM]" in message
                )

        if name.endswith("ws_bridge"):
            important = (
                "OKC WebSocket connected",
                "OKC WebSocket disconnected",
                "OKC message published",
                "WebSocket bridge stopped",
            )
            return any(x in message for x in important)

        if name == "__main__" or name.endswith(".main"):
            return "WebSocket-мониторинг НТП" in message

        return False

    @staticmethod
    def _format_record(record):
        timestamp = datetime.fromtimestamp(
            record.created,
            settings.SCHEDULER_TIMEZONE,
        ).strftime("%H:%M:%S")

        level = record.levelname

        styles = {
            "DEBUG": "dim",
            "INFO": "cyan",
            "WARNING": "yellow",
            "ERROR": "bright_red",
            "CRITICAL": "bold red",
        }

        result = Text()
        result.append(f"{timestamp} ", style="dim")
        result.append(
            f"{level:<7}",
            style=styles.get(level, "white"),
        )

        message = record.getMessage()

        limit = 2000 if LOG_MODE == "debug" else 180
        if len(message) > limit:
            message = message[:limit] + "..."

        result.append(" " + message)

        if record.exc_info and LOG_MODE == "debug":
            details = "".join(
                traceback.format_exception(
                    *record.exc_info
                )
            )
            result.append(
                "\n" + details[-2000:],
                style="dim",
            )

        return result

    def _process_logs(self):
        while True:
            try:
                record = self.log_queue.get_nowait()
            except queue.Empty:
                break

            if self._visible(record):
                self.log_messages.append(
                    self._format_record(record)
                )

    @staticmethod
    def _format_date(value):
        if not value:
            return "—"

        try:
            return datetime.fromisoformat(
                value
            ).strftime("%d.%m.%Y")
        except ValueError:
            return str(value)

    def _header(self, state):
        reports = state["reports"]

        good = sum(
            item["ok"] for item in reports
        )
        errors = sum(
            item["errors"] for item in reports
        )

        bad_sources = sum(
            item["last_status"] == "ERROR"
            for item in reports
        )

        ws = state["ws"]["status"]

        now = datetime.now(
            settings.SCHEDULER_TIMEZONE
        ).strftime("%H:%M:%S")

        label = Text()
        label.append("STP PARSER", style="bold cyan")
        label.append("  |  ", style="dim")
        label.append(
            "RUNNING" if state["enabled"] else "PAUSED",
            style="green" if state["enabled"] else "yellow",
        )

        label.append(
            f"  |  Отчётов: {len(reports)}"
            f"  |  OK: {good}"
            f"  |  ERR: {errors}",
        )

        if bad_sources:
            label.append(
                f"  |  Проблем: {bad_sources}",
                style="bold yellow",
            )

            db_state = (
                "OK"
                if state["db_ok"] is True
                else "ERR"
                if state["db_ok"] is False
                else "—"
            )

            label.append(
                f"  |  БД: {db_state}",
                style=(
                    "green"
                    if db_state == "OK"
                    else "yellow"
                ),
            )

            label.append(
                f"  |  OKC: {state['okc_status']}",
                style=(
                    "green"
                    if state["okc_status"] == "OK"
                    else "yellow"
                ),
            )

        label.append(
            f"  |  WS НТП: {ws}",
            style="green" if ws == "ONLINE" else "yellow",
        )

        label.append(
            f"  |  {now}",
            style="dim",
        )

        return Panel(
            label,
            border_style="bright_black",
            padding=(0, 1),
        )

    def _reports_panel(self, state, visible_rows):
        reports = list(state["reports"])

        reports.sort(
            key=lambda item: (
                item["last_status"] != "ERROR",
                not item["enabled"],
                item["description"].casefold(),
            )
        )

        total = len(reports)
        page_count = max(
            1,
            ceil(total / visible_rows),
        )

        # Автопереключение страницы раз в 12 секунд.
        page = int(
            time.monotonic() // 12
        ) % page_count

        offset = page * visible_rows
        current = reports[
            offset:offset + visible_rows
        ]

        table = Table(
            box=box.SIMPLE_HEAD,
            expand=True,
            show_edge=False,
            padding=(0, 1),
            header_style="bold bright_cyan",
        )

        table.add_column(
            "Отчёт",
            ratio=4,
            overflow="ellipsis",
            no_wrap=True,
        )
        table.add_column(
            "Последняя",
            justify="center",
            width=11,
        )
        table.add_column(
            "OK",
            justify="right",
            width=5,
            style="green",
        )
        table.add_column(
            "ERR",
            justify="right",
            width=5,
            style="yellow",
        )
        table.add_column(
            "Статус",
            justify="center",
            width=10,
        )

        for item in current:
            if not item["enabled"]:
                status = Text(
                    "ВЫКЛ.",
                    style="dim",
                )
            elif item["last_status"] == "ERROR":
                status = Text(
                    "ОШИБКА",
                    style="bold red",
                )
            elif item["last_status"] == "OK":
                status = Text(
                    "УСПЕШНО",
                    style="green",
                )
            elif item["last_success"]:
                status = Text(
                    "ОЖИД.",
                    style="yellow",
                )
            else:
                status = Text(
                    "НЕТ ДАННЫХ",
                    style="dim",
                )

            table.add_row(
                Text(
                    item["description"],
                    style="cyan",
                    overflow="ellipsis",
                    no_wrap=True,
                ),
                self._format_date(
                    item["last_success"]
                ),
                str(item["ok"]),
                str(item["errors"]),
                status,
            )

        if not current:
            table.add_row(
                "Нет настроенных отчётов",
                "—", "0", "0", "—",
            )

        title = (
            f"Отчёты OKC  "
            f"[{total} источников · "
            f"страница {page + 1}/{page_count}]"
        )

        return Panel(
            table,
            title=title,
            border_style="bright_black",
            padding=(0, 1),
        )

    def _ws_panel(self, state, compact=False):
        ws = state["ws"]
        status = ws["status"]

        color = {
            "ONLINE": "green",
            "CONNECTING": "yellow",
            "OFFLINE": "red",
        }.get(status, "white")

        result = Text()
        result.append(
            f"НТП: {status}",
            style=f"bold {color}",
        )

        result.append(
            f"  |  Последнее сообщение: "
            f"{ws['last_message_at'] or '—'}"
            f"  |  Опубликовано: {ws['published']}"
            f"  |  Ошибок: {ws['publish_errors']}"
        )

        if not compact:
            result.append(
                "\n"
                f"Подключён с: {ws['connected_at'] or '—'}"
                f"  |  Переподключений: {ws['reconnects']}"
            )

            if status == "OFFLINE" and ws["reason"]:
                result.append(
                    f"  |  Причина: {ws['reason'][:65]}",
                    style="yellow",
                )

        return Panel(
            result,
            title="WebSocket · Линия НТП",
            border_style="bright_black",
            padding=(0, 1),
        )

    def _next_panel(self, state, compact=False):
        now = datetime.now(
            settings.SCHEDULER_TIMEZONE
        )

        upcoming = []

        for item in state["reports"]:
            due = next_report_run(
                item, now
            )

            if due is not None:
                upcoming.append((
                    due,
                    item["description"],
                ))

        upcoming.sort(key=lambda x: x[0])

        count = 1 if compact else 2
        lines = Text()

        for index, (due, description) in enumerate(
            upcoming[:count]
        ):
            if index:
                lines.append("\n")

            if due <= now.replace(tzinfo=None):
                when = "ожидает запуска"
            else:
                when = due.strftime(
                    "%d.%m %H:%M"
                )

            lines.append(
                f"{when}  ",
                style="yellow",
            )
            lines.append(
                description[:80],
                style="cyan",
            )

        if not upcoming:
            lines.append(
                "Активных выгрузок нет",
                style="dim",
            )

        return Panel(
            lines,
            title="Ближайшие выгрузки",
            border_style="bright_black",
            padding=(0, 1),
        )

    def _logs_panel(self, height):
        available = max(1, height - 2)
        recent = list(
            self.log_messages
        )[-available:]

        output = Text()

        if not recent:
            output.append(
                "Новых событий нет",
                style="dim",
            )

        for index, record in enumerate(recent):
            if index:
                output.append("\n")
            output.append(record)

        return Panel(
            output,
            title=f"События · {LOG_MODE}",
            border_style="bright_black",
            padding=(0, 1),
        )

    def _generate_layout(self):
        state = monitor.snapshot()

        height = max(
            20,
            self.console.size.height,
        )

        compact = height < 28

        ws_size = 3 if compact else 4
        next_size = 3 if compact else 4
        min_logs = 5 if compact else 7

        visible_rows = max(
            2,
            min(
                9,
                height - (
                    3 + ws_size + next_size
                    + min_logs + 4
                ),
            ),
        )

        report_size = visible_rows + 4

        layout = Layout()

        layout.split_column(
            Layout(
                name="header",
                size=3,
            ),
            Layout(
                name="reports",
                size=report_size,
            ),
            Layout(
                name="ws",
                size=ws_size,
            ),
            Layout(
                name="next",
                size=next_size,
            ),
            Layout(
                name="logs",
                ratio=1,
                minimum_size=min_logs,
            ),
        )

        layout["header"].update(
            self._header(state)
        )
        layout["reports"].update(
            self._reports_panel(
                state, visible_rows
            )
        )
        layout["ws"].update(
            self._ws_panel(
                state, compact
            )
        )
        layout["next"].update(
            self._next_panel(
                state, compact
            )
        )

        remaining = height - (
            3 + report_size
            + ws_size + next_size
        )

        layout["logs"].update(
            self._logs_panel(remaining)
        )

        return layout

    def _update_loop(self):
        while self._running:
            try:
                self._process_logs()

                if self._live:
                    self._live.update(
                        self._generate_layout()
                    )

            except Exception:
                # Ошибка интерфейса не должна
                # останавливать сбор отчётов.
                pass

            time.sleep(
                1.0 / self.refresh_per_second
            )

    def start(self):
        if self._running:
            return

        self._running = True

        self._live = Live(
            Text("Запуск мониторинга..."),
            console=self.console,
            refresh_per_second=self.refresh_per_second,
        )

        try:
            self._live.start()
        except Exception:
            self._running = False
            raise

        self._update_thread = threading.Thread(
            target=self._update_loop,
            daemon=True,
        )
        self._update_thread.start()

    def stop(self):
        if not self._running:
            return

        self._running = False

        if self._update_thread:
            self._update_thread.join(timeout=2)

        if self._live:
            self._live.stop()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


_dashboard = None
_dashboard_lock = threading.Lock()


def get_dashboard():
    global _dashboard

    with _dashboard_lock:
        if _dashboard is None:
            _dashboard = CLIDashboard()

        return _dashboard
