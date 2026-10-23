import logging
from datetime import datetime, timedelta

from okc_py import UreAPI
from sqlalchemy import delete, func, select
from sqlalchemy.dialects.mysql import insert as mysql_insert
from stp_database.models.Stats import SalesRg

from src.core.config import settings
from src.core.db import get_stats_session


logger = logging.getLogger(__name__)


FACT_FIELDS = (
    "router_fact",
    "tv_fact",
    "video_fact",
    "dmf_fact",
    "ps_fact",
    "fullduplex_fact",
    "vas_fact",
    "services_fact",
    "general_fact",
)

def month_start(value: datetime) -> datetime:
    return value.replace(
        day=1,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
        tzinfo=None,
    )

def shift_month(value: datetime, offset: int) -> datetime:
    absolute = (
        value.year * 12
        + value.month - 1
        + offset
    )
    return datetime(
        absolute // 12,
        absolute % 12 + 1,
        1,
    )

def local_now() -> datetime:
    return datetime.now(settings.SCHEDULER_TIMEZONE).replace(tzinfo=None)

def resolve_division(value: str) -> str:
    name = " ".join(value.casefold().split())

    if name == "нтп":
        return "НТП"

    if name == "нтп нцк":
        return "НЦК"

    raise RuntimeError(f"Неизвестное направление ЕОС: {value!r}")

async def sync_sales_month(
    api: UreAPI,
    period: datetime,
) -> int:
    """
    Полностью обновляет накопленный факт
    за один календарный месяц.

    Другие месяцы и планы не изменяет.
    """

    period = month_start(period)
    now = local_now()
    current_month = month_start(now)

    if period > current_month:
        raise ValueError(
            "Нельзя получить факт будущего месяца"
        )

    if period == current_month:
        # Только полностью завершённые дни.
        end_date = now.replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
    else:
        # Завершённый месяц целиком.
        end_date = shift_month(period, 1)

    if end_date <= period:
        logger.info(
            "[SalesRg] В месяце %s ещё нет "
            "завершённых дней",
            period.strftime("%m.%Y"),
        )
        return 0

    # Сначала получаем и проверяем весь отчёт.
    # До этого момента БД не изменяем.
    response = await api.get_rg_sales_report(
        start_date=period,
        end_date=end_date,
    )

    prepared = {}
    divisions = set()

    for row in response.data:
        employee_id = row.employee_id

        if employee_id in prepared:
            raise RuntimeError(
                "Дублирующийся ID в ЕОС: "
                f"{employee_id}"
            )

        division = resolve_division(row.unit_name)
        divisions.add(division)

        prepared[employee_id] = {
            "employee_id": employee_id,
            "extraction_period": period,
            "division": division,

            "router_fact": row.materials_int,
            "tv_fact": row.materials_tv,
            "video_fact": row.materials_vn,
            "dmf_fact": row.materials_dmf,

            "ps_fact": row.paid_service,
            "fullduplex_fact": row.hundred_plus,
            "vas_fact": row.vas,

            "general_fact": (
                row.materials_int
                + row.materials_tv
                + row.materials_dmf
                + row.materials_vn
            ),

            "services_fact": (
                row.agr_int
                + row.agr_tv
                + row.agr_gs
                + row.agr_dmf
                + row.agr_vn
            ),
        }

    # Не принимаем частичную выгрузку,
    # в которой пропало целое направление.
    if divisions != {"НТП", "НЦК"}:
        raise RuntimeError(
            "Неполный отчёт ЕОС, "
            f"получены направления: {divisions}"
        )

    async with get_stats_session() as session:
        async with session.begin():

            existing_result = await session.execute(
                select(
                    SalesRg.employee_id,
                    SalesRg.division,
                ).where(
                    SalesRg.extraction_period == period,
                    SalesRg.employee_id.in_(prepared),
                )
            )

            # Если Excel и ЕОС относят одного РГ
            # к разным направлениям,
            # автоматически не перекидываем его.
            for employee_id, stored_division in (
                existing_result.all()
            ):
                incoming = prepared[employee_id]["division"]

                if (
                    stored_division is not None
                    and stored_division != incoming
                ):
                    raise RuntimeError(
                        f"РГ {employee_id}: направление "
                        f"в БД {stored_division}, "
                        f"в ЕОС {incoming}"
                    )

            for values in prepared.values():
                statement = mysql_insert(
                    SalesRg
                ).values(
                    **values,
                    updated_at=func.now(),
                )

                updates = {
                    field: getattr(
                        statement.inserted,
                        field,
                    )
                    for field in FACT_FIELDS
                }

                # Для новой строки направление
                # берётся из ЕОС, для старой
                # сохраняем уже заполненное.
                updates["division"] = func.coalesce(
                    SalesRg.division,
                    statement.inserted.division,
                )
                updates["updated_at"] = func.now()

                await session.execute(
                    statement.on_duplicate_key_update(
                        **updates
                    )
                )

    logger.info(
        "[SalesRg] Факт за %s сохранён. "
        "РГ=%s, НТП/НЦК получены",
        period.strftime("%m.%Y"),
        len(prepared),
    )

    return len(prepared)


async def fill_rg_sales(api: UreAPI) -> int:
    """
    Ежедневный запуск в 10:00.

    1–3 числа дополнительно обновляем
    завершённый предыдущий месяц.
    Текущий месяц обновляем ежедневно.
    """

    now = local_now()
    current = month_start(now)

    total = 0

    # Закрываем прошлый месяц, включая
    # последний календарный день.
    #
    # Повторяем с 1-го по 3-е:
    # - на случай сбоя 1-го числа;
    # - для поздних корректировок ЕОС.
    if now.day <= 3:
        previous = shift_month(current, -1)

        total += await sync_sales_month(
            api,
            previous,
        )

    # В первый день месяца завершённых
    # суток текущего месяца ещё нет.
    if now.day > 1:
        total += await sync_sales_month(
            api,
            current,
        )

    # Очистка выполняется только после
    # успешного получения запрошенных фактов.
    oldest_allowed = shift_month(
        current,
        -2,
    )

    async with get_stats_session() as session:
        async with session.begin():
            result = await session.execute(
                delete(SalesRg).where(
                    SalesRg.extraction_period
                    < oldest_allowed
                )
            )

    logger.info(
        "[SalesRg] Обновление завершено. "
        "Строк факта=%s, "
        "очищено старых записей=%s",
        total,
        result.rowcount,
    )

    return total
