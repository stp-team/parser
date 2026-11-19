
"""Планируемые окна выгрузки parser.links."""

from datetime import date, datetime, time, timedelta


def _next_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 4)

    return date(value.year, value.month + 1, 4)


def next_report_run(
    report: dict,
    now: datetime,
) -> datetime | None:
    """
    Ближайшее плановое окно.

    Это не APScheduler job, а расчёт времени,
    когда источник должен стать доступен
    для очередного опроса.
    """

    if not report["enabled"]:
        return None

    today = now.date()
    period = report["period"]

    last = report.get("last_success")
    last_date = (
        date.fromisoformat(last)
        if last
        else None
    )

    if period == "d":
        if last_date == today:
            target = today + timedelta(days=1)
        else:
            target = today

    elif period == "w":
        monday = today - timedelta(
            days=today.weekday()
        )

        if last_date and last_date >= monday:
            target = monday + timedelta(days=7)
        else:
            target = monday

    elif period == "m":
        fourth = today.replace(day=4)

        if last_date and last_date >= fourth:
            target = _next_month(today)
        else:
            target = fourth

    else:
        return None

    if target < today:
        target = today

    candidate = datetime.combine(
        target,
        time(hour=10),
    )

    # Внутри допустимого окна источник
    # может быть обработан следующим циклом
    # динамического парсера.
    if candidate < now.replace(tzinfo=None):
        return now.replace(tzinfo=None)

    return candidate
