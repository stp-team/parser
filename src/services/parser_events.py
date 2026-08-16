import logging
import traceback
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from time import perf_counter
from typing import Any

from stp_redis.models.services_event import (
    ServicesEvent,
    ServicesInfo,
)

from src.core.config import settings
from src.core.redis import get_redis


logger = logging.getLogger(__name__)


class ParserEventService:
    """
    Отправка структурированных событий parser в Redis.
    """

    def __init__(self) -> None:
        self.stream_name = settings.REDIS_PARSER_STREAM

        self.service_info = ServicesInfo(
            title="parser",
        )

    async def publish(
        self,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        event = ServicesEvent(
            event_type=event_type,
            event_service=self.service_info,
            to_services=[],
            payload=payload or {},
        )

        try:
            redis = await get_redis()

            await redis.xadd(
                name=self.stream_name,
                fields={
                    "data": event.model_dump_json(),
                },
                maxlen=1000,
                approximate=True,
            )

        except Exception as exc:
            logger.warning(
                "Не удалось отправить событие parser в Redis: %s",
                exc,
            )

    async def service_started(self) -> None:
        await self.publish(
            event_type="parser.service.started",
            payload={
                "status": "started",
                "message": "Парсер запущен",
                "timestamp": datetime.now(UTC).isoformat(),
            },
        )

    async def service_stopped(self) -> None:
        await self.publish(
            event_type="parser.service.stopped",
            payload={
                "status": "stopped",
                "message": "Парсер остановлен",
                "timestamp": datetime.now(UTC).isoformat(),
            },
        )

    async def service_error(
        self,
        error: BaseException,
    ) -> None:
        await self.publish(
            event_type="parser.service.error",
            payload={
                "status": "error",
                "message": "Критическая ошибка parser",
                **self._get_error_info(error),
            },
        )

    async def task_started(
        self,
        task_name: str,
        task_title: str | None = None,
        source: str | None = None,
    ) -> None:
        await self.publish(
            event_type="parser.task.started",
            payload={
                "task": task_name,
                "title": task_title or task_name,
                "status": "started",
                "source": source,
                "message": "Задача запущена",
                "started_at": datetime.now(UTC).isoformat(),
            },
        )

    async def task_success(
        self,
        task_name: str,
        duration: float,
        result: Any = None,
        task_title: str | None = None,
        source: str | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "task": task_name,
            "title": task_title or task_name,
            "status": "success",
            "source": source,
            "message": "Задача успешно выполнена",
            "duration": round(duration, 3),
            "finished_at": datetime.now(UTC).isoformat(),
        }

        result_data = self._serialize_result(result)

        if result_data is not None:
            payload["result"] = result_data

        await self.publish(
            event_type="parser.task.success",
            payload=payload,
        )

    async def task_error(
        self,
        task_name: str,
        duration: float,
        error: BaseException,
        task_title: str | None = None,
        source: str | None = None,
    ) -> None:
        await self.publish(
            event_type="parser.task.error",
            payload={
                "task": task_name,
                "title": task_title or task_name,
                "status": "error",
                "source": source,
                "message": "Ошибка выполнения задачи",
                "duration": round(duration, 3),
                "finished_at": datetime.now(UTC).isoformat(),
                **self._get_error_info(error),
            },
        )

    @staticmethod
    def _get_error_info(
        error: BaseException,
    ) -> dict[str, Any]:
        tb = traceback.extract_tb(
            error.__traceback__,
        )

        error_file: str | None = None
        error_line: int | None = None
        error_function: str | None = None

        if tb:
            last_frame = tb[-1]

            error_file = last_frame.filename
            error_line = last_frame.lineno
            error_function = last_frame.name

        traceback_text = "".join(
            traceback.format_exception(
                type(error),
                error,
                error.__traceback__,
            )
        )

        return {
            "error": str(error),
            "error_type": type(error).__name__,
            "error_file": error_file,
            "error_line": error_line,
            "error_function": error_function,
            "traceback": traceback_text,
        }

    @staticmethod
    def _serialize_result(
        result: Any,
    ) -> Any:
        if result is None:
            return None

        if isinstance(
            result,
            (str, int, float, bool),
        ):
            return result

        if isinstance(
            result,
            (list, tuple),
        ):
            return {
                "count": len(result),
            }

        if isinstance(result, dict):
            return result

        if hasattr(result, "model_dump"):
            try:
                return result.model_dump(
                    mode="json",
                )
            except Exception:
                pass

        return str(result)


parser_event_service = ParserEventService()


async def run_tracked_task(
    task_name: str,
    task_func: Callable[..., Awaitable[Any]],
    *args,
    task_title: str | None = None,
    source: str | None = None,
    **kwargs,
) -> Any:
    await parser_event_service.task_started(
        task_name=task_name,
        task_title=task_title,
        source=source,
    )

    started = perf_counter()

    try:
        result = await task_func(
            *args,
            **kwargs,
        )

        duration = perf_counter() - started

        await parser_event_service.task_success(
            task_name=task_name,
            task_title=task_title,
            duration=duration,
            result=result,
            source=source,
        )

        return result

    except Exception as error:
        duration = perf_counter() - started

        await parser_event_service.task_error(
            task_name=task_name,
            task_title=task_title,
            duration=duration,
            error=error,
            source=source,
        )

        raise