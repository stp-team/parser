"""OKC Line WebSocket -> Redis notifications bridge."""

import asyncio
import html
import logging
import re
import json
from typing import Any

from aiohttp import (
    ClientConnectionError,
    ClientResponseError,
)

from okc_py.exceptions import (
    AuthenticationError,
    CSRFError,
    InvalidCredentialsError,
)

from src.services.monitor_state import monitor
from okc_py import OKC
from stp_redis import (
    NotificationEvent,
    NotificationRecipients,
    NotificationServiceInfo,
)

from src.core.config import settings
from src.core.redis import get_redis


logger = logging.getLogger(__name__)

def classify_ws_error(exc: Exception) -> str:
    """
    Определяет понятную категорию
    ошибки подключения OKC.
    """

    # Ошибки входа в OKC.
    if isinstance(
        exc,
        (
            AuthenticationError,
            InvalidCredentialsError,
            CSRFError,
        ),
    ):
        return "Ошибка авторизации"

    # HTTP/WebSocket ответ с кодом ошибки.
    if isinstance(exc, ClientResponseError):
        if exc.status in (401, 403):
            return "Ошибка авторизации"

        return "Ошибка подключения"

    # Ошибки подключения к серверу,
    # обрывы TCP и тайм-ауты.
    if isinstance(
        exc,
        (
            ClientConnectionError,
            ConnectionError,
            TimeoutError,
            OSError,
        ),
    ):
        return "Ошибка сети"

    # Локальная сессия не была создана
    # или недоступна.
    if isinstance(exc, RuntimeError):
        message = str(exc).lower()

        if (
            "session not initialized" in message
            or "session not connected" in message
        ):
            return "Ошибка сессии"

    return "Ошибка WebSocket"

# Уведомления OKC отправляются ТОЛЬКО этим пользователям.
OKC_NOTIFICATION_RECIPIENT_IDS = [
    7920,
    7585,
    8429,
]

LINE_DISPLAY_NAMES = {
    "ntp": "НТП",
}

class WebSocketBridge:
    """
    Слушает WebSocket конкретной линии OKC.

    Используем только событие:
        message

    Полученное сообщение отправляется
    в стандартный Redis notification stream.
    """

    def __init__(
        self,
        okc_client: OKC,
        line_name: str,
    ) -> None:
        self.okc_client = okc_client
        self.line_name = line_name
        self.line_title = LINE_DISPLAY_NAMES.get(
            line_name,
            line_name,
        )

        self.line = None

        self.is_running = False

        self._reconnect_delay = 5

        # Плановая повторная авторизация
        # и подключение каждые 12 часов.
        self._session_renewal_seconds = 12 * 60 * 60

        # Тот же RedisNotificationService,
        # который используется API.
        #
        # stream_name специально НЕ указываем:
        # будет использован стандартный
        # REDIS_NOTIFICATION_STREAM.

    async def start(self) -> None:
        """
        Постоянное WebSocket-соединение OKC.

        - Повторная авторизация каждые 12 часов.
        - Восстановление при обрыве.
        - Повторные попытки каждые 5 секунд.
        - Независимость от выгрузки отчётов.
        """

        logger.info(
            "[%s] Starting OKC WebSocket bridge",
            self.line_name,
        )

        try:
            self.line = getattr(
                self.okc_client.ws.lines,
                self.line_name,
            )

        except AttributeError:
            logger.exception(
                "[%s] Unknown OKC line",
                self.line_name,
            )
            raise

        # Подписываемся на события только
        # один раз, без дублирования
        # обработчиков при переподключениях.
        self.line.on(
            "message",
            self._on_message,
        )

        self.line.on(
            "rawData",
            self._on_raw_data,
        )

        self.line.on(
            "rawIncidents",
            self._on_raw_incidents,
        )

        self.is_running = True
        loop = asyncio.get_running_loop()

        while self.is_running:
            planned_rotation = False

            try:
                monitor.ws_connecting()

                logger.info(
                    "[%s] Authorizing OKC session...",
                    self.line_name,
                )

                # Закрываем предыдущую HTTP-сессию,
                # если она существует.
                await self.okc_client.close()

                # Создаём новую сессию и
                # повторно авторизуемся в OKC.
                await self.okc_client.connect()

                logger.info(
                    "[%s] OKC session authenticated",
                    self.line_name,
                )

                logger.info(
                    "[%s] Connecting to OKC WebSocket...",
                    self.line_name,
                )

                await self.line.connect()

                monitor.ws_connected()

                connected_at = loop.time()

                # Следующее плановое обновление
                # через 12 часов от успешного
                # подключения WebSocket.
                renewal_deadline = (
                        connected_at
                        + self._session_renewal_seconds
                )

                logger.info(
                    "[%s] OKC WebSocket connected. "
                    "Next renewal in 12 hours.",
                    self.line_name,
                )

                while (
                        self.is_running
                        and self.line.is_connected
                ):
                    remaining = (
                            renewal_deadline - loop.time()
                    )

                    if remaining <= 0:
                        planned_rotation = True

                        logger.info(
                            "[%s] Scheduled 12-hour "
                            "session renewal",
                            self.line_name,
                        )

                        monitor.ws_disconnected(
                            reason=(
                                "Плановое обновление "
                                "сессии OKC"
                            ),
                            kind="Плановое обновление",
                        )

                        break

                    await asyncio.sleep(
                        min(5.0, remaining)
                    )

                if not self.is_running:
                    break

                if not planned_rotation:
                    # WebSocket перестал быть
                    # подключённым раньше срока.
                    logger.warning(
                        "[%s] WebSocket connection closed",
                        self.line_name,
                    )

                    monitor.ws_disconnected(
                        reason=(
                            "Соединение с OKC "
                            "закрылось раньше срока"
                        ),
                        kind="Соединение закрыто",
                    )

            except asyncio.CancelledError:
                monitor.ws_disconnected(
                    reason="Задача мониторинга остановлена",
                    kind="Остановлено",
                )

                logger.info(
                    "[%s] WebSocket bridge cancelled",
                    self.line_name,
                )

                raise

            except Exception as exc:
                error_kind = classify_ws_error(exc)

                monitor.ws_disconnected(
                    reason=str(exc),
                    kind=error_kind,
                )

                # Ожидаемые сетевые сбои
                # не засоряем длинными traceback
                # при каждой повторной попытке.
                if error_kind in (
                        "Ошибка сети",
                        "Ошибка авторизации",
                        "Ошибка сессии",
                        "Ошибка подключения",
                ):
                    logger.warning(
                        "[%s] %s: %s",
                        self.line_name,
                        error_kind,
                        exc,
                    )
                else:
                    logger.exception(
                        "[%s] %s",
                        self.line_name,
                        error_kind,
                    )

            finally:
                # Отключаем старый WebSocket.
                # При следующем проходе цикла
                # будет новая авторизация.
                if self.line is not None:
                    try:
                        await self.line.disconnect()

                    except Exception:
                        logger.exception(
                            "[%s] Failed to disconnect "
                            "old WebSocket",
                            self.line_name,
                        )

            if not self.is_running:
                break

            if planned_rotation:
                # Плановое переподключение
                # выполняем без дополнительной
                # пятисекундной паузы.
                continue

            logger.info(
                "[%s] Reconnecting in %s seconds...",
                self.line_name,
                self._reconnect_delay,
            )

            await asyncio.sleep(
                self._reconnect_delay
            )

    @staticmethod
    def _html_to_text(
        value: str,
    ) -> str:
        """
        OKC присылает messageText в HTML.

        Например:
            <p>Делей, оперативнее</p>

        В notification.body отправляем обычный текст.
        """

        if not value:
            return ""

        value = re.sub(
            r"<br\s*/?>",
            "\n",
            value,
            flags=re.IGNORECASE,
        )

        value = re.sub(
            r"</p\s*>",
            "\n",
            value,
            flags=re.IGNORECASE,
        )

        value = re.sub(
            r"<[^>]+>",
            "",
            value,
        )

        value = html.unescape(
            value
        )

        lines = [
            line.strip()
            for line in value.splitlines()
            if line.strip()
        ]

        return "\n".join(lines)

    async def _on_raw_incidents(self, data: Any) -> None:
        """
        Обновляет список актуальных аварий OKC.

        Берём только объект "new" и только поля:
        - cities
        - description
        - incId
        - startDate
        """

        if not isinstance(data, dict):
            return

        raw_incidents = data.get("new")

        if not isinstance(raw_incidents, list):
            raw_incidents = []

        incidents = []

        for item in raw_incidents:
            if not isinstance(item, dict):
                continue

            incidents.append(
                {
                    "cities": item.get("cities"),
                    "description": item.get("description"),
                    "incId": item.get("incId"),
                    "startDate": item.get("startDate"),
                }
            )

        # Чтобы перестановка элементов в массиве
        # не создавала ложное "обновление".
        incidents.sort(
            key=lambda item: str(
                item.get("incId") or ""
            )
        )

        payload = {
            "line": self.line_title,
            "incidents": incidents,
            "silent": True,
        }

        payload_json = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
        )

        try:
            redis = await get_redis()

            old_payload = await redis.get(
                "okc:ntp:incidents"
            )
            await redis.set(
                "okc:ntp:incidents",
                payload_json,
            )
            # Состояние аварий не изменилось —
            # WS-событие повторно не отправляем.
            if old_payload == payload_json:
                return

            event = NotificationEvent(
                event_type="okc.line.incidents",
                event_service=NotificationServiceInfo(
                    title="okc_service",
                ),
                channels_type=["ws"],
                title="Аварии НТП",
                body="Список актуальных аварий обновлён",
                payload=payload,
                recipients=NotificationRecipients(
                    include_ids=(
                        OKC_NOTIFICATION_RECIPIENT_IDS
                    ),
                ),
            )

            notification_id = await redis.xadd(
                name=settings.REDIS_NOTIFICATION_STREAM,
                fields={
                    "data": event.model_dump_json(),
                },
                maxlen=1000,
                approximate=True,
            )

            logger.info(
                "[%s] OKC incidents updated: "
                "notification_id=%s "
                "count=%s",
                self.line_name,
                notification_id,
                len(incidents),
            )

        except Exception as e:
            logger.error(
                "[%s] Failed to update "
                "OKC incidents: %s",
                self.line_name,
                e,
                exc_info=True,
            )

    async def _on_raw_data(
            self,
            data: Any,
    ) -> None:
        if not isinstance(data, dict):
            return

        waiting_queue = data.get(
            "waitingQueue"
        )

        talking_queue = data.get(
            "talkingQueue"
        )

        if not isinstance(waiting_queue, list):
            waiting_queue = []

        if not isinstance(talking_queue, list):
            talking_queue = []

        payload = {
            "line": self.line_title,
            "waitingQueue": waiting_queue,
            "talkingQueue": talking_queue,
            "silent": True,
        }

        payload_json = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
        )

        try:
            redis = await get_redis()

            old_payload = await redis.get(
                "okc:ntp:call_queues"
            )

            # Всегда обновляем актуальное состояние.
            await redis.set(
                "okc:ntp:call_queues",
                payload_json,
                ex=30,
            )

            # Ничего не поменялось — фронт не дёргаем.
            if old_payload == payload_json:
                return

            event = NotificationEvent(
                event_type="okc.line.call_queues",
                event_service=NotificationServiceInfo(title="okc_service"),
                channels_type=["ws"],
                title="Очередь НТП",
                body="Состояние очереди обновлено",
                payload=payload,
                recipients=NotificationRecipients(
                    include_ids=[7920, 7585, 8429],
                ),
            )

            await redis.xadd(
                name=settings.REDIS_NOTIFICATION_STREAM,
                fields={
                    "data": event.model_dump_json(),
                },
                maxlen=1000,
                approximate=True,
            )

        except Exception as e:
            logger.error(
                "[%s] Failed to update "
                "OKC call queues: %s",
                self.line_name,
                e,
                exc_info=True,
            )

    async def _on_message(
        self,
        data: Any,
    ) -> None:
        """
        Обработать событие OKC:

        ["message", {
            "messageText": "<p>...</p>",
            "authorName": "...",
            "from": "line-ntp2"
        }]
        """

        if not isinstance(data, dict):
            logger.warning(
                "[%s] Invalid OKC message: %r",
                self.line_name,
                data,
            )
            return

        message_html = str(
            data.get("messageText")
            or ""
        )

        message_text = self._html_to_text(
            message_html
        )

        author_name = str(
            data.get("authorName")
            or "ОКС"
        ).strip()

        source = str(
            data.get("from")
            or self.line_name
        ).strip()

        if not message_text:
            logger.warning(
                "[%s] Empty OKC message received",
                self.line_name,
            )
            return

        event = NotificationEvent(
            event_type="okc.line.message",
            event_service=NotificationServiceInfo(
                title="okc_service",
            ),
            channels_type=["ws", "fcm"],
            title=f"{self.line_title} • {author_name}",
            body=message_text,
            payload={
                "line": self.line_name,
                "from": source,
                "authorName": author_name,
            },
            recipients=NotificationRecipients(
                include_ids=(
                    OKC_NOTIFICATION_RECIPIENT_IDS
                ),
            ),
        )

        try:
            redis = await get_redis()

            notification_id = await redis.xadd(
                name=settings.REDIS_NOTIFICATION_STREAM,
                fields={
                    "data": event.model_dump_json(),
                },
                maxlen=1000,
                approximate=True,
            )

            monitor.ws_published()

            logger.info(
                "[%s] OKC message published: "
                "notification_id=%s "
                "recipients=%s "
                "author=%r",
                self.line_name,
                notification_id,
                OKC_NOTIFICATION_RECIPIENT_IDS,
                author_name,
            )

        except Exception as e:
            # Redis/notificator не должен ронять
            # WebSocket OKC.
            monitor.ws_publish_error()
            logger.error(
                "[%s] Failed to publish "
                "OKC notification: %s",
                self.line_name,
                e,
                exc_info=True,
            )

    async def stop(self) -> None:
        """Остановить WebSocket bridge."""

        self.is_running = False
        monitor.ws_disconnected(
            "WebSocket остановлен"
        )
        
        if self.line:
            try:
                await self.line.disconnect()

            except Exception as e:
                logger.warning(
                    "[%s] Error disconnecting "
                    "OKC WebSocket: %s",
                    self.line_name,
                    e,
                )

        logger.info(
            "[%s] OKC WebSocket bridge stopped",
            self.line_name,
        )


class WebSocketBridgeManager:
    """Менеджер WebSocket подключений OKC."""

    def __init__(
        self,
        okc_client: OKC,
        lines: list[str] | None = None,
    ) -> None:
        self.okc_client = okc_client

        # Только НТП1 и НТП2.
        self.lines = lines or [
            "ntp",
        ]

        self.bridges: list[
            WebSocketBridge
        ] = []

        self.tasks: list[
            asyncio.Task
        ] = []

    async def start_all(self) -> None:
        """Запустить все line WebSocket."""

        logger.info(
            "Starting OKC WebSocket bridges: %s",
            ", ".join(self.lines),
        )

        for line_name in self.lines:
            bridge = WebSocketBridge(
                okc_client=self.okc_client,
                line_name=line_name,
            )

            self.bridges.append(
                bridge
            )

            task = asyncio.create_task(
                bridge.start(),
                name=f"okc-ws-{line_name}",
            )

            self.tasks.append(
                task
            )

        logger.info(
            "OKC WebSocket bridge tasks started"
        )

    async def stop_all(self) -> None:
        """Остановить все line WebSocket."""

        logger.info(
            "Stopping OKC WebSocket bridges..."
        )

        for bridge in self.bridges:
            try:
                await bridge.stop()

            except Exception as e:
                logger.warning(
                    "Failed to stop bridge %s: %s",
                    bridge.line_name,
                    e,
                )

        for task in self.tasks:
            if not task.done():
                task.cancel()

        if self.tasks:
            await asyncio.gather(
                *self.tasks,
                return_exceptions=True,
            )

        self.tasks.clear()
        self.bridges.clear()

        logger.info(
            "All OKC WebSocket bridges stopped"
        )


ws_bridge_manager: (
    WebSocketBridgeManager | None
) = None


async def setup_ws_bridges(
    okc_client: OKC,
    lines: list[str] | None = None,
) -> WebSocketBridgeManager:
    """Запустить OKC WebSocket bridges."""

    global ws_bridge_manager

    if ws_bridge_manager is not None:
        return ws_bridge_manager

    ws_bridge_manager = (
        WebSocketBridgeManager(
            okc_client=okc_client,
            lines=lines,
        )
    )

    await ws_bridge_manager.start_all()

    return ws_bridge_manager


async def cleanup_ws_bridges() -> None:
    """Остановить OKC WebSocket bridges."""

    global ws_bridge_manager

    if ws_bridge_manager is None:
        return

    await ws_bridge_manager.stop_all()

    ws_bridge_manager = None