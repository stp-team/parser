import asyncio
import json
import logging
from collections.abc import Callable
from typing import Any

import nats
from nats.aio.client import Client as NATS

from app.core.config import settings

logger = logging.getLogger(__name__)


class NATSClient:
    """NATS client for handling API command messages"""

    def __init__(self):
        self.nc: NATS | None = None
        self.command_handlers: dict[str, Callable] = {}

    async def connect(self) -> None:
        """Connect to NATS server"""
        if not settings.NATS_HOST:
            logger.warning("NATS_HOST is not configured, skipping NATS connection")
            return

        try:
            self.nc = await nats.connect(
                servers=[f"nats://{settings.NATS_HOST}:{settings.NATS_PORT}"],
                token=settings.NATS_TOKEN if settings.NATS_TOKEN else None,
                connect_timeout=10,
                reconnect_time_wait=2,
                max_reconnect_attempts=5,
            )
            logger.info(
                f"Connected to NATS server at {settings.NATS_HOST}:{settings.NATS_PORT}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to NATS server: {e}")
            raise

    async def disconnect(self) -> None:
        """Disconnect from NATS server"""
        if self.nc:
            await self.nc.close()
            logger.info("Disconnected from NATS server")

    def register_handler(self, command: str, handler: Callable) -> None:
        """Register a command handler"""
        self.command_handlers[command] = handler
        logger.info(f"Registered handler for command: {command}")

    async def subscribe_to_commands(self) -> None:
        """Subscribe to command messages"""
        if not self.nc:
            logger.warning("NATS client not connected, cannot subscribe")
            return

        try:
            await self.nc.subscribe(settings.NATS_SUBJECT, cb=self._message_handler)
            logger.info(f"Subscribed to subject: {settings.NATS_SUBJECT}")
        except Exception as e:
            logger.error(f"Failed to subscribe to NATS subject: {e}")
            raise

    async def _message_handler(self, msg) -> None:
        """Handle incoming NATS messages"""
        try:
            # Parse message data - handle both JSON and plain string formats
            message_text = msg.data.decode()

            try:
                # Try to parse as JSON first
                data = json.loads(message_text)
                command = data.get("command")
                params = data.get("params", {})
            except json.JSONDecodeError:
                # If JSON parsing fails, treat as simple string command
                command = message_text.strip()
                params = {}
                logger.info(f"Received simple string command: {command}")

            logger.info(f"Received command: {command} with params: {params}")

            # Find and execute handler
            if command in self.command_handlers:
                handler = self.command_handlers[command]
                try:
                    result = await handler(**params)

                    # Send response back if reply subject exists
                    if msg.reply:
                        response = {
                            "status": "success",
                            "data": result,
                            "command": command,
                        }
                        await self.nc.publish(msg.reply, json.dumps(response, ensure_ascii=False).encode('utf-8'))
                        logger.debug(f"Sent response for command {command}")

                except Exception as e:
                    logger.error(f"Error executing handler for command {command}: {e}")

                    # Send error response if reply subject exists
                    if msg.reply:
                        error_response = {
                            "status": "error",
                            "error": str(e),
                            "command": command,
                        }
                        await self.nc.publish(
                            msg.reply, json.dumps(error_response, ensure_ascii=False).encode('utf-8')
                        )
            else:
                logger.warning(f"No handler found for command: {command}")

                # Send error response if reply subject exists
                if msg.reply:
                    error_response = {
                        "status": "error",
                        "error": f"Unknown command: {command}",
                        "command": command,
                    }
                    await self.nc.publish(
                        msg.reply, json.dumps(error_response, ensure_ascii=False).encode('utf-8')
                    )

        except Exception as e:
            logger.error(f"Error processing NATS message: {e}")

    async def publish_command(
        self, command: str, params: dict[str, Any] | None = None, timeout: float = 10.0
    ) -> Any:
        """Publish a command and wait for response"""
        if not self.nc:
            raise RuntimeError("NATS client not connected")

        if params is None:
            params = {}

        message = {"command": command, "params": params}

        try:
            response = await self.nc.request(
                settings.NATS_SUBJECT, json.dumps(message).encode(), timeout=timeout
            )

            result = json.loads(response.data.decode())

            if result.get("status") == "error":
                raise Exception(result.get("error", "Unknown error"))

            return result.get("data")

        except asyncio.TimeoutError:
            raise Exception(f"Timeout waiting for response to command: {command}")
        except Exception as e:
            logger.error(f"Error publishing command {command}: {e}")
            raise


# Global NATS client instance
nats_client = NATSClient()
