"""WebSocket to NATS bridge for OKC lines data."""

import asyncio
import json
import logging
from typing import Any

from okc_py import OKC
from okc_py.sockets.models import RawData, RawIncidents

from src.core.config import settings
from src.core.nats_client import nats_client

logger = logging.getLogger(__name__)


class WebSocketBridge:
    """Bridge for OKC WebSocket lines to NATS messaging."""

    def __init__(self, okc_client: OKC, line_name: str = "nck"):
        """
        Initialize WebSocket bridge.

        Args:
            okc_client: OKC client instance
            line_name: Line to connect to (nck, ntp1, ntp2)
        """
        self.okc_client = okc_client
        self.line_name = line_name
        self.line = None
        self.is_running = False
        self._reconnect_delay = 5
        self._max_reconnect_attempts = 10

    async def start(self) -> None:
        """Start the WebSocket bridge connection."""
        if not settings.NATS_HOST:
            logger.warning("NATS_HOST is not configured, skipping WebSocket bridge")
            return

        if not nats_client.nc:
            logger.warning("NATS client is not connected, skipping WebSocket bridge")
            return

        logger.info(f"Starting WebSocket bridge for line: {self.line_name}")

        # Get the line object from okc_client
        try:
            self.line = getattr(self.okc_client.ws.lines, self.line_name)
        except AttributeError:
            logger.error(
                f"Invalid line name: {self.line_name}. Available: nck, ntp1, ntp2"
            )
            raise

        await self._connect_with_retry()

    async def _connect_with_retry(self) -> None:
        """Connect to WebSocket with retry logic."""
        reconnect_attempt = 0

        while reconnect_attempt < self._max_reconnect_attempts and self.is_running:
            try:
                await self.line.connect()
                logger.info(f"WebSocket connected to line: {self.line_name}")

                # Register event handlers
                self.line.on("rawData", self._on_raw_data)
                self.line.on("rawIncidents", self._on_raw_incidents)

                self.is_running = True
                reconnect_attempt = 0  # Reset counter on successful connection

                # Start monitoring connection
                await self._monitor_connection()

            except asyncio.CancelledError:
                # Shutdown requested - clean up and re-raise
                logger.info("WebSocket bridge shutdown requested")
                self.is_running = False
                if self.line and self.line.is_connected:
                    await self.line.disconnect()
                    logger.info(f"WebSocket disconnected from line: {self.line_name}")
                raise  # Re-raise to properly cancel the task
            except Exception as e:
                reconnect_attempt += 1
                logger.error(
                    f"WebSocket connection error (attempt {reconnect_attempt}/{self._max_reconnect_attempts}): {e}"
                )

                if reconnect_attempt >= self._max_reconnect_attempts:
                    logger.error(
                        "Max reconnection attempts reached. Stopping WebSocket bridge."
                    )
                    self.is_running = False
                    break

                # Only reconnect if we're still supposed to be running
                if self.is_running:
                    logger.info(f"Reconnecting in {self._reconnect_delay} seconds...")
                    await asyncio.sleep(self._reconnect_delay)

    async def _monitor_connection(self) -> None:
        """Monitor WebSocket connection and handle disconnections."""
        while self.is_running:
            try:
                await asyncio.sleep(5)

                if not self.line.is_connected:
                    logger.warning(
                        "WebSocket connection lost, attempting to reconnect..."
                    )
                    self.is_running = False
                    break

            except asyncio.CancelledError:
                logger.info("WebSocket monitor cancelled")
                self.is_running = False
                break
            except Exception as e:
                logger.error(f"Error monitoring WebSocket connection: {e}")
                self.is_running = False
                break

    async def _on_raw_data(self, data: dict) -> None:
        """Handle rawData events from WebSocket."""
        try:
            # Validate data using Pydantic model
            raw_data = RawData(**data)

            # Prepare message for NATS
            message = {
                "type": "rawData",
                "line": self.line_name,
                "timestamp": asyncio.get_event_loop().time(),
                "data": self._serialize_raw_data(raw_data),
            }

            # Publish to NATS with line-specific subject
            await self._publish_to_nats(message, f"ws_line_{self.line_name}")

            logger.debug(f"Published rawData event from {self.line_name}")

        except Exception as e:
            logger.error(f"Error processing rawData event: {e}")

    async def _on_raw_incidents(self, data: dict) -> None:
        """Handle rawIncidents events from WebSocket."""
        try:
            # Validate data using Pydantic model
            incidents = RawIncidents(**data)

            # Prepare message for NATS
            message = {
                "type": "rawIncidents",
                "line": self.line_name,
                "timestamp": asyncio.get_event_loop().time(),
                "data": self._serialize_raw_incidents(incidents),
            }

            # Publish to NATS with line-specific subject
            await self._publish_to_nats(message, f"ws_line_{self.line_name}")

            logger.debug(f"Published rawIncidents event from {self.line_name}")

        except Exception as e:
            logger.error(f"Error processing rawIncidents event: {e}")

    async def _publish_to_nats(self, message: dict[str, Any], subject: str) -> None:
        """Publish message to NATS with specified subject."""
        try:
            if nats_client.nc:
                await nats_client.nc.publish(
                    subject,
                    json.dumps(message, ensure_ascii=False, default=str).encode(
                        "utf-8"
                    ),
                )
            else:
                logger.warning("NATS client not connected, cannot publish message")

        except Exception as e:
            logger.error(f"Error publishing to NATS: {e}")

    def _serialize_raw_data(self, raw_data: RawData) -> dict[str, Any]:
        """Serialize RawData object to dictionary using Pydantic model_dump."""
        return raw_data.model_dump(mode="json", exclude_none=True)

    def _serialize_raw_incidents(self, incidents: RawIncidents) -> dict[str, Any]:
        """Serialize RawIncidents object to dictionary using Pydantic model_dump."""
        return incidents.model_dump(mode="json", exclude_none=True)

    async def stop(self) -> None:
        """Stop the WebSocket bridge."""
        logger.info("Stopping WebSocket bridge...")
        self.is_running = False

        # Ensure WebSocket is disconnected if still connected
        if self.line and self.line.is_connected:
            await self.line.disconnect()
            logger.info(f"WebSocket disconnected from line: {self.line_name}")


class WebSocketBridgeManager:
    """Manager for multiple WebSocket bridge connections."""

    def __init__(self, okc_client: OKC, lines: list[str] | None = None):
        """
        Initialize WebSocket bridge manager.

        Args:
            okc_client: OKC client instance
            lines: List of line names to connect to (default: ["nck"])
        """
        self.okc_client = okc_client
        self.lines = lines or ["nck"]
        self.bridges: list[WebSocketBridge] = []
        self.tasks: list[asyncio.Task] = []

    async def start_all(self) -> None:
        """Start all WebSocket bridges."""
        logger.info(f"Starting {len(self.lines)} WebSocket bridge(s)...")

        for line_name in self.lines:
            bridge = WebSocketBridge(self.okc_client, line_name)
            self.bridges.append(bridge)

            # Create task for each bridge
            task = asyncio.create_task(bridge.start())
            self.tasks.append(task)

        logger.info(f"All {len(self.lines)} WebSocket bridge(s) started")

    async def stop_all(self) -> None:
        """Stop all WebSocket bridges."""
        logger.info("Stopping all WebSocket bridges...")

        # Cancel all tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()

        # Wait for tasks to complete
        await asyncio.gather(*self.tasks, return_exceptions=True)

        # Stop all bridges
        for bridge in self.bridges:
            await bridge.stop()

        self.bridges.clear()
        self.tasks.clear()

        logger.info("All WebSocket bridges stopped")

    def is_running(self) -> bool:
        """Check if any bridge is running."""
        return any(bridge.is_running for bridge in self.bridges)


# Global WebSocket bridge manager instance
ws_bridge_manager: WebSocketBridgeManager | None = None


async def setup_ws_bridges(
    okc_client: OKC, lines: list[str] | None = None
) -> WebSocketBridgeManager | None:
    """
    Setup and start WebSocket bridge manager.

    Args:
        okc_client: OKC client instance
        lines: List of line names to connect to (default: ["nck"])

    Returns:
        WebSocketBridgeManager instance
    """
    global ws_bridge_manager

    if not settings.NATS_HOST:
        logger.warning("NATS_HOST is not configured, skipping WebSocket bridges setup")
        return None

    if not nats_client.nc:
        logger.warning("NATS client is not connected, skipping WebSocket bridges setup")
        return None

    ws_bridge_manager = WebSocketBridgeManager(okc_client, lines)
    await ws_bridge_manager.start_all()

    return ws_bridge_manager


async def cleanup_ws_bridges() -> None:
    """Cleanup WebSocket bridges."""
    global ws_bridge_manager

    if ws_bridge_manager:
        await ws_bridge_manager.stop_all()
        ws_bridge_manager = None
