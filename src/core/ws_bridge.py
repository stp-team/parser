"""OKC Line WebSocket -> Redis notifications bridge."""

import asyncio
import html
import logging
import re
from typing import Any

from okc_py import OKC
from stp_redis import (
    NotificationEvent,
    NotificationRecipients,
    NotificationServiceInfo,
    RedisNotificationService,
)


logger = logging.getLogger(__name__)


# Уведомления OKC отправляются ТОЛЬКО этим пользователям.
OKC_NOTIFICATION_RECIPIENT_IDS = [
    7920,
    7585,
]


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

        self.line = None

        self.is_running = False

        self._reconnect_delay = 5

        # Тот же RedisNotificationService,
        # который используется API.
        #
        # stream_name специально НЕ указываем:
        # будет использован стандартный
        # REDIS_NOTIFICATION_STREAM.
        self.notificator = RedisNotificationService()

    async def start(self) -> None:
        """Запустить WebSocket линии и поддерживать соединение."""

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
            logger.error(
                "[%s] Unknown OKC line",
                self.line_name,
            )
            raise

        # Обработчик регистрируем ДО connect().
        #
        # OKC может прислать message сразу
        # после установки соединения.
        self.line.on(
            "message",
            self._on_message,
        )

        self.is_running = True

        while self.is_running:
            try:
                logger.info(
                    "[%s] Connecting to OKC WebSocket...",
                    self.line_name,
                )

                await self.line.connect()

                logger.info(
                    "[%s] OKC WebSocket connected",
                    self.line_name,
                )

                while (
                    self.is_running
                    and self.line.is_connected
                ):
                    await asyncio.sleep(5)

                if not self.is_running:
                    break

                logger.warning(
                    "[%s] OKC WebSocket disconnected",
                    self.line_name,
                )

            except asyncio.CancelledError:
                logger.info(
                    "[%s] WebSocket bridge cancelled",
                    self.line_name,
                )
                raise

            except Exception as e:
                logger.error(
                    "[%s] WebSocket error: %s",
                    self.line_name,
                    e,
                    exc_info=True,
                )

            try:
                if self.line:
                    await self.line.disconnect()

            except Exception:
                pass

            if self.is_running:
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

            channels_type=[
                "ws",
                "fcm",
            ],

            title=f"ОКС • {author_name}",

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
            notification_id = (
                await self.notificator.publish_notification(
                    event
                )
            )

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
            "ntp1",
            "ntp2",
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