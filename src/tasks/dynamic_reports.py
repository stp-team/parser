
import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

from aiohttp import ClientTimeout
from okc_py import OKC
from sqlalchemy import text
from stp_database.repo.Backend.requests import BackendRequestsRepo

from src.core.config import settings
from src.core.db import get_backend_session, get_stats_session


logger = logging.getLogger(__name__)

POLL_SECONDS = 60
MAX_RESPONSE_BYTES = 64 * 1024 * 1024


def get_period_dates(period: str) -> tuple[str, str, str]:
    """
    Рассчитывает даты для OKC.

    startDate — начало отчётного периода.
    stopDate — следующий день после окончания периода.
    period_label — фактический период для БД.
    """

    today = datetime.now(
        settings.SCHEDULER_TIMEZONE
    ).date()

    if period == "d":
        start = today - timedelta(days=1)
        end = start

    elif period == "w":
        end = today - timedelta(
            days=today.weekday() + 1
        )
        start = end - timedelta(days=6)

    elif period == "m":
        end = today.replace(day=1) - timedelta(days=1)
        start = end.replace(day=1)

    else:
        raise ValueError(
            f"Неизвестный период: {period}"
        )

    # Конечная дата в OKC не включается в отчёт.
    stop = end + timedelta(days=1)

    start_date = start.strftime("%d.%m.%Y")
    stop_date = stop.strftime("%d.%m.%Y")

    if period == "d":
        period_label = start_date
    else:
        period_label = (
            f"{start.strftime('%d.%m.%Y')}-"
            f"{end.strftime('%d.%m.%Y')}"
        )

    return start_date, stop_date, period_label

async def read_parser_settings() -> tuple[bool, list[dict[str, Any]]]:
    """Читает parser.on и parser.links из Backend."""

    async with get_backend_session() as session:
        repo = BackendRequestsRepo(session).services_parametrs

        flag = await repo.get_param(
            service="parser",
            name="on",
        )

        if flag is None:
            return False, []

        if flag.type != "int" or str(flag.value).strip() not in {"0", "1"}:
            raise ValueError("parser.on должен быть int со значением 0 или 1")

        if str(flag.value).strip() == "0":
            return False, []

        links_param = await repo.get_param(
            service="parser",
            name="links",
        )

    if links_param is None or links_param.type != "json":
        raise ValueError("Не найден JSON-параметр parser.links")

    links = json.loads(links_param.value or "[]")

    if not isinstance(links, list):
        raise ValueError("parser.links должен содержать JSON-массив")

    seen_names = set()

    for link in links:
        if not isinstance(link, dict):
            raise ValueError("Каждая ссылка должна быть JSON-объектом")

        required = {
            "name", "description", "url",
            "period", "payload", "enabled",
        }

        if not required.issubset(link):
            raise ValueError("У ссылки отсутствуют обязательные поля")

        if not isinstance(link["name"], str) or not link["name"].strip():
            raise ValueError("name должен быть непустой строкой")

        if link["name"] in seen_names:
            raise ValueError(f"Повторяется name: {link['name']}")

        seen_names.add(link["name"])

        if not isinstance(link["description"], str):
            raise ValueError(f"Некорректный description: {link['name']}")

        if not isinstance(link["url"], str) or not link["url"].strip():
            raise ValueError(f"Некорректный URL: {link['name']}")

        if link["period"] not in {"d", "w", "m"}:
            raise ValueError(f"Некорректный period: {link['name']}")

        if not isinstance(link["payload"], dict):
            raise ValueError(f"Некорректный payload: {link['name']}")

        if type(link["enabled"]) is not bool:
            raise ValueError(f"Некорректный enabled: {link['name']}")

    return True, links

async def write_parser_log(
    level: str,
    message: str,
) -> None:
    """Записывает событие в Stats.ParserLog."""

    if level not in {"error", "warn", "alert", "info"}:
        raise ValueError(
            f"Недопустимый тип лога: {level}"
        )

    try:
        async with get_stats_session() as session:
            await session.execute(
                text("""
                    INSERT INTO `ParserLog` (
                        uuid,
                        type,
                        value
                    )
                    VALUES (
                        :uuid,
                        :type,
                        :value
                    )
                """),
                {
                    "uuid": str(uuid4()),
                    "type": level,
                    "value": message,
                },
            )

            await session.commit()

    except Exception:
        logger.exception(
            "Не удалось записать Stats.ParserLog"
        )

async def report_exists(
    name: str,
    period: str,
    period_label: str,
) -> bool:

    async with get_stats_session() as session:
        result = await session.execute(
            text("""
                SELECT uuid
                FROM `ParserReports`
                WHERE link_name = :name
                  AND period_type = :period
                  AND period_of_dates = :period_label
                LIMIT 1
            """),
            {
                "name": name,
                "period": period,
                "period_label": period_label,
            },
        )

        return result.first() is not None

def prepare_url(base_url: str, raw_url: str) -> str:
    """Разрешает запросы только к настроенному серверу OKC."""

    base = urlsplit(base_url)
    raw_url = raw_url.strip()

    if raw_url.startswith("//"):
        raise ValueError("URL с внешним хостом запрещён")

    if raw_url.startswith("/"):
        url = f"{base.scheme}://{base.netloc}{raw_url}"
    elif "://" not in raw_url:
        url = f"{base.scheme}://{base.netloc}/{raw_url}"
    else:
        url = raw_url

    target = urlsplit(url)

    if (
        target.scheme != base.scheme
        or target.netloc.lower() != base.netloc.lower()
        or target.username is not None
        or target.password is not None
    ):
        raise ValueError("Разрешены ссылки только на сервер OKC")

    return url


async def fetch_raw_report(
    okc: OKC,
    link: dict[str, Any],
    start_date: str,
    stop_date: str,
) -> str:
    """Отправляет POST JSON и возвращает исходный ответ."""

    url = prepare_url(
        okc.client.settings.BASE_URL,
        link["url"],
    )

    payload = dict(link["payload"])
    payload["startDate"] = start_date
    payload["stopDate"] = stop_date

    session = okc.client.get_session()

    if session is None or session.closed:
        raise RuntimeError("Сессия OKC не подключена")

    async with session.post(
        url,
        json=payload,
        timeout=ClientTimeout(total=120),
    ) as response:
        response.raise_for_status()

        if response.url.path.rstrip("/") == "/site/login":
            raise RuntimeError("OKC перенаправил запрос на авторизацию")

        result = bytearray()

        async for chunk in response.content.iter_chunked(256 * 1024):
            result.extend(chunk)

            if len(result) > MAX_RESPONSE_BYTES:
                raise ValueError("Ответ превышает допустимый размер")

        if not result:
            raise ValueError("OKC вернул пустой ответ")

        return bytes(result).decode(
            response.charset or "utf-8"
        )

async def save_report(
    name: str,
    period: str,
    period_label: str,
    raw_result: str,
) -> None:
    """Сохраняет текстовый ответ OKC в Stats.ParserReports."""

    async with get_stats_session() as session:
        await session.execute(
            text("""
                INSERT INTO `ParserReports` (
                    uuid,
                    link_name,
                    period_type,
                    period_of_dates,
                    link_result
                )
                VALUES (
                    :uuid,
                    :name,
                    :period,
                    :period_label,
                    :raw_result
                )
            """),
            {
                "uuid": str(uuid4()),
                "name": name,
                "period": period,
                "period_label": period_label,
                "raw_result": raw_result,
            },
        )

        await session.commit()

async def parser_is_enabled() -> bool:
    """Перепроверяет выключатель между запросами."""

    async with get_backend_session() as session:
        repo = BackendRequestsRepo(session).services_parametrs
        flag = await repo.get_param("parser", "on")

    return (
        flag is not None
        and flag.type == "int"
        and str(flag.value).strip() == "1"
    )


async def collect_reports(okc: OKC) -> None:
    is_on, links = await read_parser_settings()

    if not is_on:
        return

    pending = []

    for link in links:
        if not link["enabled"]:
            continue

        start_date, stop_date, label = get_period_dates(
            link["period"]
        )

        if await report_exists(
            link["name"],
            link["period"],
            label,
        ):
            continue

        pending.append((link, start_date, stop_date, label))

    if not pending:
        return

    successful = 0
    failed = 0

    try:
        await okc.connect()

        for link, start_date, stop_date, label in pending:
            if not await parser_is_enabled():
                break

            try:
                raw_result = await fetch_raw_report(
                    okc,
                    link,
                    start_date,
                    stop_date,
                )

                await save_report(
                    link["name"],
                    link["period"],
                    label,
                    raw_result,
                )

                successful += 1

            except Exception as exc:
                failed += 1

                logger.warning(
                    "Ошибка выгрузки %s: %s",
                    link["name"],
                    exc,
                )

                await write_parser_log(
                    "warn",
                    f"{link['name']} [{label}]: "
                    f"{type(exc).__name__}: {exc}",
                )

    finally:
        await okc.close()

    await write_parser_log(
        "info",
        f"Прогон завершён. "
        f"Запланировано: {len(pending)}; "
        f"успешно: {successful}; "
        f"ошибок: {failed}.",
    )


async def run_dynamic_reports_loop(okc: OKC) -> None:
    """Основной цикл управляемого парсера."""

    while True:
        try:
            await collect_reports(okc)

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            logger.exception("Ошибка выполнения парсера")

            await write_parser_log(
                "error",
                f"{type(exc).__name__}: {exc}",
            )

        await asyncio.sleep(POLL_SECONDS)
