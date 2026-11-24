import asyncio
import json
import logging

from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4
from urllib.parse import urlencode

from src.services.monitor_state import monitor

from aiohttp import ClientTimeout
from okc_py import OKC
from sqlalchemy import text, bindparam
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

        parser_enabled = (
                str(flag.value).strip() == "1"
        )

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

        if resource_type == "tutors":
            if link["period"] != "m":
                raise ValueError(
                    f"{link['name']}: tutors требует period=m"
                )

            first_period = link.get("first_period")

            try:
                parsed = datetime.strptime(
                    first_period, "%m.%Y"
                )
                if parsed.strftime("%m.%Y") != first_period:
                    raise ValueError()
            except (TypeError, ValueError):
                raise ValueError(
                    f"{link['name']}: first_period "
                    "должен иметь формат MM.YYYY"
                )

            if link["url"].split("?")[0].rstrip(
                    "/"
            ).endswith(
                "/tutor-graph/tutor-api/get-full-graph"
            ) is False:
                raise ValueError(
                    f"{link['name']}: неверный endpoint tutors"
                )

        if not isinstance(link["payload"], dict):
            raise ValueError(f"Некорректный payload: {link['name']}")

        if type(link["enabled"]) is not bool:
            raise ValueError(f"Некорректный enabled: {link['name']}")

    return parser_enabled, links

async def load_monitor_history(
    names: list[str],
) -> None:
    """Последняя успешная выгрузка каждого источника."""

    if not names:
        return

    query = text("""
        SELECT
            link_name,
            MAX(writed_at) AS last_at
        FROM ParserReports
        WHERE link_name IN :names
        GROUP BY link_name
    """).bindparams(
        bindparam("names", expanding=True)
    )

    try:
        async with get_stats_session() as session:
            result = await session.execute(
                query,
                {"names": names},
            )

            rows = [
                dict(row)
                for row in result.mappings()
            ]

        monitor.set_history(names, rows)

    except Exception:
        monitor.db_error()
        logger.exception(
            "[MONITOR] Не удалось получить "
            "историю выгрузок"
        )

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

def validate_premium_response(
    raw_result: str,
    link: dict[str, Any],
    requested_period: str,
) -> tuple[str, int, int]:
    """
    Проверяет ответы премиума специалистов и РГ.

    Возвращает:
        (тип_премиума, количество_строк,
         количество_notEligible)

    Исходный JSON не изменяет.
    """

    try:
        response = json.loads(raw_result)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"{link['name']}: премиум вернул невалидный JSON"
        ) from exc

    # Специалисты: корневой JSON-массив.
    if isinstance(response, list):
        premium_type = "specialists"
        rows = response
        not_eligible = {}

    # РГ: объект с premium и notEligible.
    elif (
        isinstance(response, dict)
        and isinstance(response.get("premium"), list)
    ):
        premium_type = "heads"
        rows = response["premium"]

        not_eligible = response.get("notEligible", {})

        # OKC иногда возвращает пустой массив.
        if not_eligible == []:
            not_eligible = {}

        if not isinstance(not_eligible, dict):
            raise ValueError(
                f"{link['name']}: некорректный notEligible"
            )

    else:
        raise ValueError(
            f"{link['name']}: неизвестный формат премиума. "
            "Ожидался массив специалистов либо "
            "объект с полем premium"
        )

    # Контроль соответствия endpoint и ответа.
    endpoint = link["url"].split("?")[0].rstrip("/")

    if endpoint.endswith("get-premium-head-month"):
        expected_type = "heads"
    elif endpoint.endswith("get-premium-spec-month"):
        expected_type = "specialists"
    else:
        raise ValueError(
            f"{link['name']}: неизвестный endpoint премиума"
        )

    if premium_type != expected_type:
        raise ValueError(
            f"{link['name']}: получен {premium_type}, "
            f"ожидался {expected_type}"
        )

    if not rows:
        raise ValueError(
            f"{link['name']}: премиум вернул пустой список"
        )

    # Проверяем реальный месяц каждой строки.
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(
                f"{link['name']}: строка {index} "
                "не является объектом"
            )

        row_period = row.get("PERIOD")

        try:
            parsed_period = datetime.strptime(
                row_period,
                "%d.%m.%Y",
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"{link['name']}: некорректный PERIOD "
                f"в строке {index}: {row_period}"
            ) from exc

        if parsed_period.strftime("%m.%Y") != requested_period:
            raise ValueError(
                f"{link['name']}: период ответа "
                f"{row_period} не соответствует "
                f"запросу {requested_period}"
            )

        if row.get("EMPLOYEE_ID") is None:
            raise ValueError(
                f"{link['name']}: отсутствует EMPLOYEE_ID "
                f"в строке {index}"
            )

    return (
        premium_type,
        len(rows),
        len(not_eligible),
    )

def validate_tutors_response(
    raw_result: str,
    expected_month: str,
) -> None:
    """Структурная проверка графика наставников."""

    import calendar

    try:
        data = json.loads(raw_result)
    except json.JSONDecodeError as exc:
        raise ValueError(
            "TUTORS: OKC вернул некорректный JSON"
        ) from exc

    if not isinstance(data, dict):
        raise ValueError(
            "TUTORS: ожидался JSON-объект"
        )

    tutors = data.get("tutors")
    days = data.get("days")

    if (
        not isinstance(tutors, list)
        or not tutors
        or not isinstance(days, list)
    ):
        raise ValueError(
            "TUTORS: отсутствуют tutors/days "
            "или пустой график"
        )

    month = datetime.strptime(
        expected_month, "%m.%Y"
    ).date()

    count = calendar.monthrange(
        month.year, month.month
    )[1]

    expected = {
        date.strftime("%d.%m.%Y")
        for date in (
            month + timedelta(days=offset)
            for offset in range(count)
        )
    }

    actual = {
        day.get("day")
        for day in days
        if isinstance(day, dict)
    }

    if (
        len(days) != count
        or actual != expected
    ):
        raise ValueError(
            "TUTORS: OKC вернул неполный "
            "или неверный месяц"
        )

    for index, tutor in enumerate(tutors):
        if not isinstance(tutor, dict):
            raise ValueError(
                f"TUTORS: запись {index} не объект"
            )

        info = tutor.get("tutorInfo")

        if (
            not isinstance(info, dict)
            or not info.get("employeeId")
            or not isinstance(tutor.get("shifts"), list)
            or not isinstance(tutor.get("trainees"), list)
        ):
            raise ValueError(
                f"TUTORS: повреждена запись {index}"
            )

        if (
            len(tutor["shifts"]) != count
            or len(tutor["trainees"]) != count
        ):
            raise ValueError(
                f"TUTORS: неполный график "
                f"employeeId={info['employeeId']}"
            )

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
        now = datetime.now(settings.SCHEDULER_TIMEZONE)

        # Наставники: предыдущий полный месяц.
        month_start = now.date().replace(day=1)
        previous_month_end = month_start - timedelta(days=1)
        previous_month_start = (previous_month_end.replace(day=1))
        payload["startDate"] = (previous_month_start.strftime("%d.%m.%Y"))
        payload["stopDate"] = (month_start.strftime("%d.%m.%Y"))

        logger.info(
            "[TUTORS] %s | месяц=%s",
            link["name"],
            previous_month_start.strftime("%m.%Y"),
        )


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

    request_kwargs = {
        "timeout": ClientTimeout(total=120),
    }

    if resource_type == "tutors":
        form_fields = []

        for key, value in payload.items():
            if isinstance(value, list):
                for item in value:
                    form_fields.append(
                        (f"{key}[]", str(item))
                    )

            elif value is not None:
                form_fields.append(
                    (key, str(value))
                )

        request_kwargs["data"] = urlencode(
            form_fields
        ).encode("utf-8")

        request_kwargs["headers"] = {
            "Content-Type": (
                "application/x-www-form-urlencoded; "
                "charset=UTF-8"
            ),
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json",
        }

    else:
        request_kwargs["json"] = payload

    async with session.post(
            url,
            **request_kwargs,
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

        raw_result = bytes(result).decode(
            response.charset or "utf-8"
        )

        if resource_type == "premium":
            premium_type, rows_count, excluded_count = (
                validate_premium_response(
                    raw_result=raw_result,
                    link=link,
                    requested_period=payload["period"],
                )
            )

            logger.info(
                "[PREMIUM] %s | тип=%s | месяц=%s | "
                "сотрудников=%s | notEligible=%s | OK",
                link["name"],
                premium_type,
                payload["period"],
                rows_count,
                excluded_count,
            )

        return raw_result


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
    monthly_eos_names: list[str] | None = None,
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
                    SELECT uuid, check_status, check_result 
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

            existing_report = result.mappings().first()

            report_uuid = (
                existing_report["uuid"]
                if existing_report
                else None
            )

            if (
                    existing_report
                    and existing_report["check_status"] != "done"
            ):
                raise RuntimeError(
                    f"{name}: предыдущий отчёт "
                    "ещё не обработан"
                )

            if existing_report and monthly_eos_names is not None:
                old_result = existing_report["check_result"]

                if isinstance(old_result, str):
                    old_result = json.loads(old_result)

                if not isinstance(old_result, dict):
                    raise RuntimeError(
                        "TUTORS: нет метаданных предыдущего отчёта"
                    )

                old_month_raw = old_result.get(
                    "extraction_period"
                )

                if not old_month_raw:
                    raise RuntimeError(
                        "TUTORS: неизвестен предыдущий месяц"
                    )

                old_month = datetime.strptime(
                    old_month_raw,
                    "%Y-%m-%d",
                ).date()

                # Проверяем, что старый месяц действительно
                # был обработан AchievementService.
                from calendar import monthrange

                last_day = monthrange(
                    old_month.year,
                    old_month.month,
                )[1]

                label = (
                    f"{old_month:%d.%m.%Y}-"
                    f"{old_month.replace(day=last_day):%d.%m.%Y}"
                )

                if not monthly_eos_names:
                    raise RuntimeError(
                        "TUTORS: не настроены месячные EOS"
                    )

                result = await session.execute(
                    text("""
                         SELECT link_name,
                                check_status,
                                check_result
                         FROM ParserReports
                         WHERE link_name IN :names
                           AND period_type = 'm'
                           AND period_of_dates = :period_label
                         """).bindparams(
                        bindparam("names", expanding=True)
                    ),
                    {
                        "names": monthly_eos_names,
                        "period_label": label,
                    },
                )

                checked_names = set()

                for eos_row in result.mappings():
                    meta = eos_row["check_result"]

                    if isinstance(meta, str):
                        try:
                            meta = json.loads(meta)
                        except ValueError:
                            meta = None

                    if not isinstance(meta, dict):
                        continue

                    if (
                            eos_row["check_status"] == "done"
                            and meta.get("extraction_period")
                            == old_month.isoformat()
                            and meta.get("achievements_checked_at")
                    ):
                        checked_names.add(eos_row["link_name"])

                pending = (
                        set(monthly_eos_names) - checked_names
                )

                if pending:
                    raise RuntimeError(
                        "TUTORS: предыдущий месяц ещё "
                        "не прошёл начисление достижений. "
                        f"Ожидаются EOS: {sorted(pending)}"
                    )

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

    is_on, links = await read_parser_settings()

    # Интерфейс обновляется независимо от того,
    # разрешены ли запросы к OKC прямо сейчас.
    history_names = monitor.set_links(
        links,
        enabled=is_on,
    )

    if history_names:
        await load_monitor_history(history_names)

    # Сам механизм получения отчётов не меняем.
    if not is_on or now.hour < 10:
        return

    pending = []

    for link in links:
        if not link["enabled"]:
            continue

        resource_type = link["resource_type"]
        period = link["period"]

        if resource_type == "tutors":
            first = datetime.strptime(
                link["first_period"], "%m.%Y"
            ).date().replace(day=1)

            # За какой законченный месяц сейчас
            # должна выполняться выгрузка.
            month_start = now.date().replace(day=1)
            requested_month = (
                    month_start - timedelta(days=1)
            ).replace(day=1)

            if requested_month < first:
                continue

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

        monthly_eos_names = [
            item["name"]
            for item in links
            if (
                    item["enabled"]
                    and item["resource_type"] == "eos"
                    and item["period"] == "m"
                    and item.get("distribution_rules") is not None
            )
        ]

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

                if resource_type == "tutors":
                    expected_month = (
                            now.date().replace(day=1)
                            - timedelta(days=1)
                    ).strftime("%m.%Y")

                    validate_tutors_response(
                        raw_result,
                        expected_month,
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
                        monthly_eos_names=(
                            monthly_eos_names
                            if resource_type == "tutors"
                            else None
                        ),
                    )

                successful += 1

                monitor.report_success(
                    link["name"]
                )

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

                monitor.report_error(
                    link["name"],
                    exc,
                )

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
