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


def get_week_end_date() -> datetime:
    """
    Возвращает конец недели (воскресенье) для парсинга KPI.

    Returns:
        datetime объект с концом недели (воскресенье)
    """
    monday = get_week_start_date()
    sunday = monday + timedelta(days=6)
    return sunday.replace(hour=23, minute=59, second=59, microsecond=999999)


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
                microsecond=0
            )
    else:
        # С 4-го числа парсим текущий месяц
        return get_current_month_first_day()


def calculate_days_in_week_period() -> int:
    """
    Вычисляет количество дней для парсинга недельного периода.

    Returns:
        Количество дней для недельного периода (всегда 7 для полной недели)
    """
    # Для недельных KPI всегда используем 7 дней
    return 7


def calculate_days_in_month_period() -> int:
    """
    Вычисляет количество дней для парсинга месячного периода.

    Returns:
        Количество дней в месяце для парсинга
    """
    current_date = datetime.now()
    month_period = get_month_period_for_kpi()

    if current_date.day <= 3:
        # Если парсим предыдущий месяц, берем все дни предыдущего месяца
        if month_period.month == 12:
            # Декабрь
            return 31
        elif month_period.month in [1, 3, 5, 7, 8, 10]:
            # Месяцы с 31 днем
            return 31
        elif month_period.month in [4, 6, 9, 11]:
            # Месяцы с 30 днями
            return 30
        else:
            # Февраль
            year = month_period.year
            if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
                return 29  # Високосный год
            else:
                return 28
    else:
        # Парсим текущий месяц до текущей даты
        return current_date.day
