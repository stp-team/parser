from datetime import datetime


def get_current_month_first_day() -> datetime:
    """Возвращает первое число текущего месяца как datetime объект.

    Returns:
        datetime объект с первым числом текущего месяца
    """
    current_date = datetime.now()
    return current_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
