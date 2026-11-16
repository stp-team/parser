
import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
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

POLL_SECONDS = 300
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
        resource_type = link.get("resource_type", "eos")

        if resource_type not in {"eos", "premium", "tutors"}:
            raise ValueError(f"Некорректный resource_type: {link['name']}")
        link["resource_type"] = resource_type

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

# Для premium/tutors хранится одна актуальная
# запись на каждый link_name.
REFRESH_LABEL = "__latest__"

async def get_last_refresh(
    name: str,
) -> datetime | None:
    """Последняя успешная выгрузка источника."""

    async with get_stats_session() as session:
        result = await session.execute(
            text("""
                SELECT writed_at
                FROM `ParserReports`
                WHERE link_name = :name
                  AND period_of_dates = :label
                ORDER BY writed_at DESC
                LIMIT 1
            """),
            {
                "name": name,
                "label": REFRESH_LABEL,
            },
        )

        return result.scalar_one_or_none()


def refresh_is_due(
    period: str,
    last_refresh: datetime | None,
    now: datetime,
) -> bool:
    """Нужно ли обновить premium/tutors."""

    # Общий запрет выгрузки раньше 10:00.
    if now.hour < 10:
        return False

    # Месячные обновления начинаются
    # с 4-го числа, как в прежнем расписании.
    if period == "m" and now.day < 4:
        return False

    # Источник ещё никогда не выгружался.
    if last_refresh is None:
        return True

    # Новые записи хранят writed_at в UTC.
    # MySQL DATETIME не содержит часовой пояс.
    if last_refresh.tzinfo is None:
        last_refresh = last_refresh.replace(
            tzinfo=timezone.utc
        )

    last_date = last_refresh.astimezone(
        settings.SCHEDULER_TIMEZONE
    ).date()

    today = now.date()

    if period == "d":
        # Одно успешное обновление за день.
        return last_date < today

    if period == "w":
        # Одно обновление за календарную
        # неделю, начиная с понедельника.
        week_start = today - timedelta(
            days=today.weekday()
        )

        return last_date < week_start

    if period == "m":
        # Одно обновление за календарный месяц.
        return (
            last_date.year,
            last_date.month,
        ) < (
            today.year,
            today.month,
        )

    raise ValueError(
        f"Неизвестная периодичность: {period}"
    )

def get_premium_period(now: datetime) -> str:
    """
    Возвращает месяц премиума в формате MM.YYYY.

    С 1 по 3 число:
        предыдущий календарный месяц.

    С 4 числа:
        текущий календарный месяц.
    """

    month_start = now.date().replace(day=1)

    if now.day < 4:
        target_month = (
            month_start - timedelta(days=1)
        )
    else:
        target_month = month_start

    return target_month.strftime("%m.%Y")

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
    start_date: str | None = None,
    stop_date: str | None = None,
) -> str:
    """POST JSON в OKC, получение сырого ответа."""

    url = prepare_url(
        okc.client.settings.BASE_URL,
        link["url"],
    )

    payload = dict(link["payload"])

    resource_type = link.get(
        "resource_type",
        "eos",
    )

    if resource_type == "eos":
        # Прежнее поведение:
        # period отвечает за даты отчёта.
        if start_date is None or stop_date is None:
            raise ValueError(
                "Для eos необходимы startDate и stopDate"
            )

        payload["startDate"] = start_date
        payload["stopDate"] = stop_date

    elif resource_type == "premium":
        # period в parser.links отвечает
        # только за частоту опроса.
        #
        # payload.period — месяц премиума.
        # Формат: MM.YYYY.
        now = datetime.now(
            settings.SCHEDULER_TIMEZONE
        )

        premium_period = get_premium_period(now)

        payload["period"] = premium_period

        logger.info(
            "[PREMIUM] %s | "
            "месяц=%s | частота=%s",
            link["name"],
            premium_period,
            link["period"],
        )

    elif resource_type == "tutors":
        # Для наставников ничего автоматически
        # в payload не добавляем.
        pass

    else:
        raise ValueError(
            f"Неизвестный resource_type: {resource_type}"
        )

    # Для premium/tutors payload не меняем.
    # Все параметры передаются из parser.links.

    session = okc.client.get_session()

    if session is None or session.closed:
        raise RuntimeError("Сессия OKC не подключена")

    logger.info(
        "OKC REQUEST: url=%s, payload=%s",
        url,
        json.dumps(payload, ensure_ascii=False),
    )

    async with session.post(
        url,
        json=payload,
        timeout=ClientTimeout(total=120),
    ) as response:
        if response.status >= 400:
            error_body = await response.text()
            logger.error(
                "OKC ERROR: status=%s, body=%s",
                response.status,
                error_body[:3000],
            )

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

async def save_refresh_report(
    name: str,
    period: str,
    raw_result: str,
) -> None:
    """
    Сохраняет актуальный premium/tutors отчёт.

    При первом получении создаёт строку.
    При последующих — обновляет её,
    сохраняя прежний UUID.
    """

    async with get_stats_session() as session:
        async with session.begin():

            result = await session.execute(
                text("""
                    SELECT uuid
                    FROM `ParserReports`
                    WHERE link_name = :name
                      AND period_of_dates = :label
                    ORDER BY writed_at DESC
                    LIMIT 1
                    FOR UPDATE
                """),
                {
                    "name": name,
                    "label": REFRESH_LABEL,
                },
            )

            report_uuid = result.scalar_one_or_none()

            if report_uuid is None:
                # Первая успешная выгрузка.
                await session.execute(
                    text("""
                        INSERT INTO `ParserReports` (
                            uuid,
                            link_name,
                            period_type,
                            period_of_dates,
                            link_result,
                            writed_at,
                            checked_at,
                            check_status,
                            check_result
                        )
                        VALUES (
                            :uuid,
                            :name,
                            :period,
                            :label,
                            :raw_result,
                            UTC_TIMESTAMP(),
                            NULL,
                            'ready',
                            NULL
                        )
                    """),
                    {
                        "uuid": str(uuid4()),
                        "name": name,
                        "period": period,
                        "label": REFRESH_LABEL,
                        "raw_result": raw_result,
                    },
                )

            else:
                # Последующие выгрузки обновляют
                # существующую строку.
                await session.execute(
                    text("""
                        UPDATE `ParserReports`
                        SET
                            period_type = :period,
                            link_result = :raw_result,
                            writed_at = UTC_TIMESTAMP(),
                            checked_at = NULL,
                            check_status = 'ready',
                            check_result = NULL
                        WHERE uuid = :uuid
                    """),
                    {
                        "uuid": report_uuid,
                        "period": period,
                        "raw_result": raw_result,
                    },
                )

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
    """Универсальная выгрузка eos/premium/tutors."""

    now = datetime.now(
        settings.SCHEDULER_TIMEZONE
    )

    # Сохраняем действующее ограничение.
    if now.hour < 10:
        return

    is_on, links = await read_parser_settings()

    if not is_on:
        return

    pending = []

    for link in links:
        if not link["enabled"]:
            continue

        resource_type = link["resource_type"]
        period = link["period"]

        if resource_type == "eos":
            # Старое поведение eos:
            # period определяет даты отчёта.

            if period == "m" and now.day < 4:
                continue

            start_date, stop_date, label = (
                get_period_dates(period)
            )

            # Уже сохранённый период
            # повторно не скачиваем.
            if await report_exists(
                link["name"],
                period,
                label,
            ):
                continue

        else:
            # premium/tutors:
            # period определяет частоту
            # обновления данных.

            last_refresh = await get_last_refresh(
                link["name"]
            )

            if not refresh_is_due(
                period,
                last_refresh,
                now,
            ):
                continue

            # Даты автоматически не формируем.
            start_date = None
            stop_date = None

            label = REFRESH_LABEL

        pending.append(
            (
                link,
                start_date,
                stop_date,
                label,
            )
        )

    if not pending:
        return

    successful = 0
    failed = 0

    try:
        await okc.connect()

        for (
            link,
            start_date,
            stop_date,
            label,
        ) in pending:

            if not await parser_is_enabled():
                logger.info(
                    "Парсер выключен во время прогона"
                )
                break

            resource_type = link["resource_type"]

            try:
                # Всегда POST.
                raw_result = await fetch_raw_report(
                    okc,
                    link,
                    start_date,
                    stop_date,
                )

                if resource_type == "eos":
                    # Отдельная историческая запись
                    # для каждого отчётного периода.
                    await save_report(
                        link["name"],
                        link["period"],
                        label,
                        raw_result,
                    )

                else:
                    # premium/tutors:
                    # обновление существующей записи.
                    await save_refresh_report(
                        link["name"],
                        link["period"],
                        raw_result,
                    )

                successful += 1

                logger.info(
                    "[PARSER] %s | type=%s | "
                    "period=%s | сохранено",
                    link["name"],
                    resource_type,
                    link["period"],
                )

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                failed += 1

                logger.warning(
                    "[PARSER] Ошибка %s (%s): %s",
                    link["name"],
                    resource_type,
                    exc,
                )

                await write_parser_log(
                    "warn",
                    (
                        f"{link['name']} "
                        f"[{resource_type}, {label}]: "
                        f"{type(exc).__name__}: {exc}"
                    ),
                )

    finally:
        await okc.close()

    logger.info(
        "[PARSER] Прогон завершён | "
        "источников=%s | успешно=%s | ошибок=%s",
        len(pending),
        successful,
        failed,
    )

    await write_parser_log(
        "info",
        (
            f"Прогон завершён. "
            f"Запланировано: {len(pending)}; "
            f"успешно: {successful}; "
            f"ошибок: {failed}."
        ),
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
