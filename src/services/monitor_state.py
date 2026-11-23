"""Состояние терминального мониторинга STP Parser."""

from copy import deepcopy
from datetime import datetime
from threading import RLock

from src.core.config import settings


class ParserMonitor:
    def __init__(self):
        self._lock = RLock()
        self._links = {}
        self._reports = {}
        self._history_loaded = set()
        self._enabled = False
        self._db_ok = None
        self._okc_status = "ОЖИДАНИЕ"

        self._ws = {
            "status": "OFFLINE",
            "connected_at": None,
            "last_message_at": None,
            "published": 0,
            "publish_errors": 0,
            "reconnects": 0,
            "was_online": False,
            "reason": None,
            "error_kind": None,
        }

    @staticmethod
    def _now():
        return datetime.now(
            settings.SCHEDULER_TIMEZONE
        )

    def set_links(self, links, enabled: bool):
        """Обновить каталог из parser.links."""

        with self._lock:
            self._enabled = enabled

            self._links = {
                link["name"]: {
                    "name": link["name"],
                    "description": (
                        link["description"]
                        or link["name"]
                    ),
                    "period": link["period"],
                    "resource_type": link.get(
                        "resource_type", "eos"
                    ),
                    "enabled": link["enabled"],
                }
                for link in links
            }

            for name in self._links:
                self._reports.setdefault(
                    name,
                    {
                        "ok": 0,
                        "errors": 0,
                        "last_success": None,
                        "last_status": None,
                        "last_error": None,
                    },
                )

            return [
                name
                for name in self._links
                if name not in self._history_loaded
            ]

    def set_history(self, names, rows):
        """История последней выгрузки из БД."""

        with self._lock:
            for row in rows:
                name = row["link_name"]
                last_at = row["last_at"]

                if name not in self._reports:
                    continue

                if (
                    last_at is not None
                    and self._reports[name]["last_success"]
                    is None
                ):
                    self._reports[name]["last_success"] = (
                        last_at.date().isoformat()
                    )

            self._history_loaded.update(names)
            self._db_ok = True

    def db_error(self):
        with self._lock:
            self._db_ok = False

    def report_success(self, name):
        with self._lock:
            row = self._reports.get(name)
            if row is None:
                return

            row["ok"] += 1
            row["last_status"] = "OK"
            row["last_success"] = (
                self._now().date().isoformat()
            )
            row["last_error"] = None
            self._okc_status = "OK"
            self._db_ok = True

    def report_error(self, name, error):
        with self._lock:
            row = self._reports.get(name)
            if row is None:
                return

            row["errors"] += 1
            row["last_status"] = "ERROR"
            row["last_error"] = str(error)[:300]
            self._okc_status = "ERROR"

    def ws_connecting(self):
        with self._lock:
            if self._ws["was_online"]:
                self._ws["reconnects"] += 1

            self._ws["status"] = "CONNECTING"

    def ws_connected(self):
        with self._lock:
            self._ws["status"] = "ONLINE"

            # Формат: DD.MM HH:MM
            self._ws["connected_at"] = (
                self._now().strftime("%d.%m %H:%M")
            )

            self._ws["was_online"] = True
            self._ws["reason"] = None
            self._ws["error_kind"] = None

    def ws_disconnected(
            self,
            reason=None,
            kind=None,
    ):
        with self._lock:
            self._ws["status"] = "OFFLINE"
            self._ws["reason"] = (
                str(reason)[:200]
                if reason is not None
                else None
            )
            self._ws["error_kind"] = kind

    def ws_published(self):
        with self._lock:
            self._ws["published"] += 1
            self._ws["last_message_at"] = (
                self._now().strftime("%H:%M:%S")
            )

    def ws_publish_error(self):
        with self._lock:
            self._ws["publish_errors"] += 1

    def snapshot(self):
        """Безопасная копия состояния для Rich."""

        with self._lock:
            reports = []

            for name, link in self._links.items():
                reports.append({
                    **link,
                    **self._reports[name],
                })

            return {
                "enabled": self._enabled,
                "reports": deepcopy(reports),
                "db_ok": self._db_ok,
                "okc_status": self._okc_status,
                "ws": deepcopy(self._ws),
            }


monitor = ParserMonitor()
