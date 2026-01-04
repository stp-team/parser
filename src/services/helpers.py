from datetime import datetime, timedelta


def get_yesterday_date() -> datetime:
    """Возвращает вчерашнюю дату как datetime объект.

    Returns:
        datetime объект с вчерашней датой
    """
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.replace(hour=0, minute=0, second=0, microsecond=0)


def get_current_month_first_day() -> datetime:
    """Возвращает первое число текущего месяца как datetime объект.

    Returns:
        datetime объект с первым числом текущего месяца
    """
    current_date = datetime.now()
    return current_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def get_week_start_date() -> datetime:
    """
    Возвращает начало недели (понедельник) для парсинга KPI.

    Логика:
    - Если сегодня понедельник, парсим с этого понедельника
    - Если любой другой день недели, парсим с понедельника текущей недели

    Returns:
        datetime объект с началом недели (понедельник)
    """
    current_date = datetime.now()
    current_weekday = current_date.weekday()  # 0 = Понедельник, 6 = Воскресенье

    # Находим понедельник текущей недели
    days_since_monday = current_weekday
    monday_date = current_date - timedelta(days=days_since_monday)

    return monday_date.replace(hour=0, minute=0, second=0, microsecond=0)


def get_month_period_for_kpi() -> datetime:
    """
    Возвращает период месяца для парсинга KPI.

    Логика:
    - Парсить предыдущий месяц ПОКА не наступит 4-е число следующего месяца
    - Если сегодня 1, 2, 3 число месяца, парсим предыдущий месяц
    - С 4-го числа и далее парсим текущий месяц

    Returns:
        datetime объект с первым числом месяца для парсинга
    """
    current_date = datetime.now()
    current_day = current_date.day

    if current_day <= 3:
        # Если сегодня 1, 2 или 3 число, парсим предыдущий месяц
        if current_date.month == 1:
            # Если текущий месяц январь, предыдущий - декабрь прошлого года
            return datetime(current_date.year - 1, 12, 1, 0, 0, 0)
        else:
            # Возвращаем первое число предыдущего месяца
            return current_date.replace(
                month=current_date.month - 1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )
    else:
        # С 4-го числа парсим текущий месяц
        return get_current_month_first_day()
