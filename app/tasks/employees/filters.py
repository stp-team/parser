def filter_missing_birthday(emp) -> bool:
    """Фильтр для сотрудников без дня рождения."""
    return not emp.birthday and emp.user_id


def filter_missing_employment_date(emp) -> bool:
    """Фильтр для сотрудников без даты трудоустройства."""
    return not emp.employment_date and emp.user_id


def filter_missing_employee_id(emp) -> bool:
    """Фильтр для сотрудников без employee_id."""
    return not emp.employee_id and emp.user_id


def filter_missing_any_data(emp) -> bool:
    """Фильтр для сотрудников с любыми недостающими данными."""
    return (
        not emp.birthday or not emp.employee_id or not emp.employment_date
    ) and emp.user_id
