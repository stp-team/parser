"""Терминальный мониторинг STP Parser."""

import sys
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

class NavigationInput:
    """Неблокирующий ввод для Rich Dashboard."""

    def __init__(self):
        self._fd = None
        self._buffer = b""
        self._old_terminal = None
        self._termios = None

    def start(self):
        try:
            self._fd = sys.stdin.fileno()
        except (AttributeError, OSError, ValueError):
            return

        if os.name == "nt":
            # Windows: msvcrt используется
            # непосредственно при опросе.
            return

        if not sys.stdin.isatty():
            return

        try:
            import termios
            import tty

            self._old_terminal = (
                termios.tcgetattr(self._fd)
            )
            self._termios = termios

            # Без ожидания Enter, без эха символов.
            # Ctrl+C остаётся доступным.
            tty.setcbreak(self._fd)

        except (OSError, ValueError):
            self._old_terminal = None
            self._termios = None

    def stop(self):
        if (
            self._termios is not None
            and self._old_terminal is not None
            and self._fd is not None
        ):
            self._termios.tcsetattr(
                self._fd,
                self._termios.TCSADRAIN,
                self._old_terminal,
            )

        self._old_terminal = None
        self._termios = None

    def read(self) -> list[str]:
        commands = []

        if self._fd is None:
            return commands

        if os.name == "nt":
            import msvcrt

            try:
                while msvcrt.kbhit():
                    char = msvcrt.getwch()

                    # Специальные клавиши Windows.
                    if char in ("\x00", "\xe0"):
                        special = msvcrt.getwch()

                        if special == "K":
                            commands.append("prev")
                        elif special == "M":
                            commands.append("next")

                    else:
                        self._buffer += char.encode(
                            "utf-8",
                            errors="ignore",
                        )

            except OSError:
                pass

            # Дополнительная поддержка терминалов,
            # где stdin предоставлен как pipe.
            # Например, некоторых конфигураций
            # Git Bash / MinGW.
            try:
                import ctypes

                handle = msvcrt.get_osfhandle(
                    self._fd
                )

                available = ctypes.c_ulong(0)

                success = (
                    ctypes.windll.kernel32.PeekNamedPipe(
                        ctypes.c_void_p(handle),
                        None,
                        0,
                        None,
                        ctypes.byref(available),
                        None,
                    )
                )

                if success and available.value:
                    self._buffer += os.read(
                        self._fd,
                        min(available.value, 128),
                    )

            except (OSError, ValueError, AttributeError):
                pass

        else:
            import select

            try:
                readable, _, _ = select.select(
                    [self._fd],
                    [],
                    [],
                    0,
                )

                if readable:
                    self._buffer += os.read(
                        self._fd,
                        128,
                    )

            except (OSError, ValueError):
                pass

        while self._buffer:
            # Последовательности стрелок ANSI:
            # ESC [ D = влево
            # ESC [ C = вправо
            if self._buffer[0] == 27:
                if len(self._buffer) < 3:
                    break

                sequence = self._buffer[:3]
                self._buffer = self._buffer[3:]

                if sequence in (
                    b"\x1b[D",
                    b"\x1bOD",
                ):
                    commands.append("prev")

                elif sequence in (
                    b"\x1b[C",
                    b"\x1bOC",
                ):
                    commands.append("next")

                continue

            char = chr(self._buffer[0]).lower()
            self._buffer = self._buffer[1:]

            if char == "p":
                commands.append("prev")
            elif char == "n":
                commands.append("next")
            elif char == "a":
                commands.append("toggle_auto")

        return commands

class CLIDashboard:
    def __init__(self, refresh_per_second=2):
        self.console = Console()
        self.refresh_per_second = refresh_per_second

        self.log_queue = queue.Queue(maxsize=1000)
        self.log_messages = deque(maxlen=1000)

        self._running = False
        self._live = None
        self._update_thread = None
        self._page_index = 0
        self._auto_page = settings.PARSER_AUTO_PAGE

        self._page_interval = max(
            3,
            settings.PARSER_PAGE_INTERVAL,
        )

        self._last_page_change = time.monotonic()

        self._navigation = NavigationInput()

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
        label.append("STP PARSER: ", style="bold cyan")
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

        # Состояние БД показываем всегда.
        db_state = (
            "OK"
            if state["db_ok"] is True
            else "ERR"
            if state["db_ok"] is False
            else "—"
        )

        label.append(
            "  |  "
        )
        label.append(
            f"БД: {db_state}",
            style=(
                "green"
                if db_state == "OK"
                else "yellow"
            ),
        )

        # Последнее известное состояние запросов OKC.
        okc_status = state["okc_status"]

        label.append(
            f"  |  "
        )
        label.append(
            f"OKC API: {okc_status}",
            style=(
                "green"
                if okc_status == "OK"
                else "yellow"
            ),
        )

        label.append(
            f"  |  "
        )
        label.append(
            f"WS НТП: {ws}",
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

    def _handle_navigation(self, total_pages: int):
        """Ручная/автоматическая смена страниц."""

        total_pages = max(1, total_pages)

        self._page_index %= total_pages

        now = time.monotonic()

        for command in self._navigation.read():
            if command == "prev":
                self._page_index = (
                                           self._page_index - 1
                                   ) % total_pages

                self._last_page_change = now

            elif command == "next":
                self._page_index = (
                                           self._page_index + 1
                                   ) % total_pages

                self._last_page_change = now

            elif command == "toggle_auto":
                self._auto_page = not self._auto_page
                self._last_page_change = now

        if (
                self._auto_page
                and total_pages > 1
                and now - self._last_page_change
                >= self._page_interval
        ):
            self._page_index = (
                                       self._page_index + 1
                               ) % total_pages

            self._last_page_change = now

    @staticmethod
    def _report_status(item, state, now):
        """Определить понятный статус источника."""

        if not state["enabled"] or not item["enabled"]:
            return "ВЫКЛ.", "dim", "disabled"

        if item["last_status"] == "ERROR":
            return "ОШИБКА", "bold red", "error"

        next_run = next_report_run(item, now)

        due = (
                next_run is not None
                and next_run <= now.replace(tzinfo=None)
        )

        # Время планового опроса уже наступило,
        # но успешной выгрузки за текущий цикл нет.
        if due:
            return "НЕТ ЗАПРОСОВ", "yellow", "waiting"

        last_success = item.get("last_success")

        if last_success == now.date().isoformat():
            return "УСПЕШНО", "green", "success"

        if last_success:
            return "ПО ПЛАНУ", "green", "scheduled"

        return "ОШИБКА", "dim", "empty"

    def _reports_panel(self, state, visible_rows):
        """Группировка отчётов с ручной пагинацией."""

        now = datetime.now(
            settings.SCHEDULER_TIMEZONE
        )

        group_titles = {
            "eos": "ЕОС",
            "premium": "ПРЕМИУМ",
            "tutors": "НАСТАВНИКИ",
        }

        # Каждый тип отчёта получает свой раздел.
        groups = {
            "eos": [],
            "premium": [],
            "tutors": [],
        }

        for item in state["reports"]:
            kind = item.get(
                "resource_type", "eos"
            )

            groups.setdefault(kind, []).append(item)

        status_priority = {
            "error": 0,
            "waiting": 1,
            "success": 2,
            "scheduled": 3,
            "empty": 4,
            "disabled": 5,
        }

        # Внутри раздела сначала проблемные
        # источники, затем остальные по названию.
        for items in groups.values():
            items.sort(
                key=lambda item: (
                    status_priority.get(
                        self._report_status(
                            item, state, now
                        )[2],
                        99,
                    ),
                    item["description"].casefold(),
                )
            )

        pages = []
        page_capacity = max(1, visible_rows)

        for kind, items in groups.items():
            if not items:
                continue

            group_pages = ceil(
                len(items) / page_capacity
            )

            for local_page in range(group_pages):
                start = local_page * page_capacity
                end = start + page_capacity

                pages.append({
                    "kind": kind,
                    "group_total": len(items),
                    "group_page": local_page + 1,
                    "group_pages": group_pages,
                    "items": items[start:end],
                })

        if not pages:
            pages = [{
                "kind": "eos",
                "group_total": 0,
                "group_page": 1,
                "group_pages": 1,
                "items": [],
            }]

        # Читаем стрелки и обрабатываем автопрокрутку.
        self._handle_navigation(len(pages))

        page = pages[self._page_index]

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
            width=12,
        )

        for item in page["items"]:
            status_text, status_style, _ = (
                self._report_status(
                    item, state, now
                )
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
                Text(
                    status_text,
                    style=status_style,
                ),
            )

        if not page["items"]:
            table.add_row(
                "Нет настроенных отчётов",
                "—", "0", "0", "—",
            )

        group_title = group_titles.get(
            page["kind"],
            page["kind"].upper(),
        )

        mode = (
            "АВТО"
            if self._auto_page
            else "РУЧНОЙ"
        )

        title = (
            f"Отчёты OKC · {group_title} "
            f"({page['group_total']})"
            f" · {self._page_index + 1}/{len(pages)}"
            f" · {mode}"
            f" · ←/→ страницы, A авто"
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
        if not state["enabled"]:
            return Panel(
                Text(
                    "Парсер выключен — "
                    "выгрузки приостановлены",
                    style="yellow",
                ),
                title="Ближайшие выгрузки",
                border_style="bright_black",
                padding=(0, 1),
            )

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
            self._navigation.start()
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

        try:
            if self._live:
                self._live.stop()
        finally:
            self._navigation.stop()

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
