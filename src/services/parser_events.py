import logging
import traceback
from collections import Counter
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from datetime import UTC, datetime
from threading import Lock
from time import perf_counter
from typing import Any
from uuid import uuid4

from stp_redis.models.services_event import (
    ServicesEvent,
    ServicesInfo,
)

from src.core.config import settings
from src.core.redis import get_redis


logger = logging.getLogger(__name__)


_current_task_run_id: ContextVar[str | None] = ContextVar(
    "parser_task_run_id",
    default=None,
)


class TaskExecutionLogHandler(logging.Handler):
    """
    Собирает logging-сообщения только внутри конкретного
    запуска run_tracked_task().

    ContextVar позволяет корректно работать даже когда
    несколько задач parser выполняются параллельно.
    """

    def __init__(
        self,
        level: int = logging.INFO,
        max_records: int = 300,
        max_message_length: int = 4000,
    ) -> None:
        super().__init__(level)

        self.max_records = max_records
        self.max_message_length = max_message_length

        self._buffers: dict[str, dict[str, Any]] = {}
        self._lock = Lock()

    def start_task(
        self,
        task_run_id: str,
    ) -> None:
        with self._lock:
            self._buffers[task_run_id] = {
                "records": [],
                "dropped": 0,
            }

    def finish_task(
        self,
        task_run_id: str,
    ) -> dict[str, Any]:
        with self._lock:
            buffer = self._buffers.pop(
                task_run_id,
                {
                    "records": [],
                    "dropped": 0,
                },
            )

        records: list[dict[str, Any]] = buffer["records"]
        dropped: int = buffer["dropped"]

        levels = Counter(
            record["level"]
            for record in records
        )

        error_count = (
            levels.get("ERROR", 0)
            + levels.get("CRITICAL", 0)
        )

        warning_count = levels.get(
            "WARNING",
            0,
        )

        return {
            "records": records,
            "stored_records": len(records),
            "dropped_records": dropped,
            "total_records": len(records) + dropped,
            "info_count": levels.get("INFO", 0),
            "warning_count": warning_count,
            "error_count": error_count,
            "critical_count": levels.get(
                "CRITICAL",
                0,
            ),
            "has_errors": error_count > 0,
            "has_warnings": warning_count > 0,
        }

    def emit(
        self,
        record: logging.LogRecord,
    ) -> None:
        task_run_id = _current_task_run_id.get()

        if not task_run_id:
            return

        try:
            log_record = self._serialize_record(
                record
            )

            with self._lock:
                buffer = self._buffers.get(
                    task_run_id
                )

                if buffer is None:
                    return

                if (
                    len(buffer["records"])
                    >= self.max_records
                ):
                    buffer["dropped"] += 1
                    return

                buffer["records"].append(
                    log_record
                )

        except Exception:
            # Сбор логов никогда не должен
            # ломать основную задачу.
            pass

    def _serialize_record(
        self,
        record: logging.LogRecord,
    ) -> dict[str, Any]:
        message = record.getMessage()

        if (
            len(message)
            > self.max_message_length
        ):
            message = (
                message[
                    : self.max_message_length
                ]
                + "... [truncated]"
            )

        exception: str | None = None

        if record.exc_info:
            exception = "".join(
                traceback.format_exception(
                    *record.exc_info,
                )
            )

            if (
                len(exception)
                > self.max_message_length
            ):
                exception = (
                    exception[
                        : self.max_message_length
                    ]
                    + "... [truncated]"
                )

        elif record.exc_text:
            exception = record.exc_text

        result: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created,
                tz=UTC,
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": message,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        if exception:
            result["exception"] = exception

        return result


def _get_log_level(
    level_name: str,
) -> int:
    return getattr(
        logging,
        level_name.upper(),
        logging.INFO,
    )


task_execution_log_handler = (
    TaskExecutionLogHandler(
        level=_get_log_level(
            settings.REDIS_PARSER_LOG_LEVEL
        ),
        max_records=(
            settings.REDIS_PARSER_LOG_MAX_RECORDS
        ),
        max_message_length=(
            settings.REDIS_PARSER_LOG_MAX_MESSAGE_LENGTH
        ),
    )
)


def ensure_task_log_handler() -> None:
    """
    Подключает handler к root logger.

    setup_logging() очищает root handlers,
    поэтому проверяем наличие перед запуском
    каждой tracked-задачи.
    """

    root_logger = logging.getLogger()

    if (
        task_execution_log_handler
        not in root_logger.handlers
    ):
        root_logger.addHandler(
            task_execution_log_handler
        )


class ParserEventService:
    """
    Отправка структурированных событий parser
    в Redis Stream parser:events.

    Ошибка Redis не должна останавливать parser.
    """

    def __init__(self) -> None:
        self.stream_name = (
            settings.REDIS_PARSER_STREAM
        )

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
                "Не удалось отправить событие parser "
                "в Redis: %s",
                exc,
            )

    async def service_started(
        self,
    ) -> None:
        await self.publish(
            event_type="parser.service.started",
            payload={
                "status": "started",
                "message": "Парсер запущен",
                "timestamp": datetime.now(
                    UTC
                ).isoformat(),
            },
        )

    async def service_stopped(
        self,
    ) -> None:
        await self.publish(
            event_type="parser.service.stopped",
            payload={
                "status": "stopped",
                "message": "Парсер остановлен",
                "timestamp": datetime.now(
                    UTC
                ).isoformat(),
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
                "message": (
                    "Критическая ошибка parser"
                ),
                **self._get_error_info(
                    error
                ),
            },
        )

    async def task_started(
        self,
        task_run_id: str,
        task_name: str,
        task_title: str | None = None,
        source: str | None = None,
    ) -> None:
        await self.publish(
            event_type="parser.task.started",
            payload={
                "task_run_id": task_run_id,
                "task": task_name,
                "title": (
                    task_title
                    or task_name
                ),
                "status": "started",
                "source": source,
                "message": "Задача запущена",
                "started_at": datetime.now(
                    UTC
                ).isoformat(),
            },
        )

    async def task_success(
        self,
        task_run_id: str,
        task_name: str,
        duration: float,
        execution_log: dict[str, Any],
        result: Any = None,
        task_title: str | None = None,
        source: str | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "task_run_id": task_run_id,
            "task": task_name,
            "title": (
                task_title
                or task_name
            ),
            "status": "success",
            "source": source,
            "message": (
                "Задача успешно выполнена"
            ),
            "duration": round(
                duration,
                3,
            ),
            "finished_at": datetime.now(
                UTC
            ).isoformat(),
            "execution_log": execution_log,
        }

        result_data = (
            self._serialize_result(
                result
            )
        )

        if result_data is not None:
            payload["result"] = result_data

        await self.publish(
            event_type="parser.task.success",
            payload=payload,
        )

    async def task_completed_with_errors(
        self,
        task_run_id: str,
        task_name: str,
        duration: float,
        execution_log: dict[str, Any],
        result: Any = None,
        task_title: str | None = None,
        source: str | None = None,
    ) -> None:
        """
        Функция Python завершилась без exception,
        но внутри выполнения были ERROR/CRITICAL.
        """

        payload: dict[str, Any] = {
            "task_run_id": task_run_id,
            "task": task_name,
            "title": (
                task_title
                or task_name
            ),
            "status": "completed_with_errors",
            "source": source,
            "message": (
                "Задача завершена, но во время "
                "выполнения зафиксированы ошибки"
            ),
            "duration": round(
                duration,
                3,
            ),
            "finished_at": datetime.now(
                UTC
            ).isoformat(),
            "execution_log": execution_log,
        }

        result_data = (
            self._serialize_result(
                result
            )
        )

        if result_data is not None:
            payload["result"] = result_data

        await self.publish(
            event_type=(
                "parser.task.completed_with_errors"
            ),
            payload=payload,
        )

    async def task_error(
        self,
        task_run_id: str,
        task_name: str,
        duration: float,
        error: BaseException,
        execution_log: dict[str, Any],
        task_title: str | None = None,
        source: str | None = None,
    ) -> None:
        await self.publish(
            event_type="parser.task.error",
            payload={
                "task_run_id": task_run_id,
                "task": task_name,
                "title": (
                    task_title
                    or task_name
                ),
                "status": "error",
                "source": source,
                "message": (
                    "Задача завершилась ошибкой"
                ),
                "duration": round(
                    duration,
                    3,
                ),
                "finished_at": datetime.now(
                    UTC
                ).isoformat(),
                "execution_log": execution_log,
                **self._get_error_info(
                    error
                ),
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

            error_file = (
                last_frame.filename
            )

            error_line = (
                last_frame.lineno
            )

            error_function = (
                last_frame.name
            )

        traceback_text = "".join(
            traceback.format_exception(
                type(error),
                error,
                error.__traceback__,
            )
        )

        return {
            "error": str(error),
            "error_type": (
                type(error).__name__
            ),
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
            (
                str,
                int,
                float,
                bool,
            ),
        ):
            return result

        if isinstance(
            result,
            (
                list,
                tuple,
                set,
            ),
        ):
            return {
                "count": len(result),
            }

        if isinstance(
            result,
            dict,
        ):
            return result

        if hasattr(
            result,
            "model_dump",
        ):
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
    task_func: Callable[
        ...,
        Awaitable[Any],
    ],
    *args,
    task_title: str | None = None,
    source: str | None = None,
    **kwargs,
) -> Any:
    """
    Выполнить задачу parser с трекингом.

    Возможные итоговые события:

    parser.task.success
        Нет исключения и внутри не было ERROR.

    parser.task.completed_with_errors
        Исключение наружу не вышло,
        но внутри logger.error/logger.critical были.

    parser.task.error
        Из задачи наружу вышло исключение.
    """

    ensure_task_log_handler()

    task_run_id = str(
        uuid4()
    )

    await parser_event_service.task_started(
        task_run_id=task_run_id,
        task_name=task_name,
        task_title=task_title,
        source=source,
    )

    task_execution_log_handler.start_task(
        task_run_id
    )

    context_token = (
        _current_task_run_id.set(
            task_run_id
        )
    )

    started = perf_counter()

    try:
        result = await task_func(
            *args,
            **kwargs,
        )

    except Exception as error:
        duration = (
            perf_counter()
            - started
        )

        _current_task_run_id.reset(
            context_token
        )

        execution_log = (
            task_execution_log_handler
            .finish_task(
                task_run_id
            )
        )

        await parser_event_service.task_error(
            task_run_id=task_run_id,
            task_name=task_name,
            task_title=task_title,
            duration=duration,
            error=error,
            execution_log=execution_log,
            source=source,
        )

        raise

    else:
        duration = (
            perf_counter()
            - started
        )

        _current_task_run_id.reset(
            context_token
        )

        execution_log = (
            task_execution_log_handler
            .finish_task(
                task_run_id
            )
        )

        if execution_log[
            "has_errors"
        ]:
            await (
                parser_event_service
                .task_completed_with_errors(
                    task_run_id=task_run_id,
                    task_name=task_name,
                    task_title=task_title,
                    duration=duration,
                    result=result,
                    execution_log=execution_log,
                    source=source,
                )
            )

        else:
            await parser_event_service.task_success(
                task_run_id=task_run_id,
                task_name=task_name,
                task_title=task_title,
                duration=duration,
                result=result,
                execution_log=execution_log,
                source=source,
            )

        return result