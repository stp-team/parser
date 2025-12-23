"""
Утилиты для обработки данных тестов.
Предоставляет функции для работы с данными о назначенных тестах.
"""

import logging
from datetime import datetime
from typing import Any

from stp_database.models.Stats import AssignedTest

from app.tasks.base import safe_get_attr

logger = logging.getLogger(__name__)


def parse_test_date(
    date_str: str, format_pattern: str = "%d.%m.%Y %H:%M:%S"
) -> datetime | None:
    """
    Безопасный парсинг даты из строки.

    Args:
        date_str: Строка с датой
        format_pattern: Формат для парсинга

    Returns:
        Объект datetime или None при ошибке
    """
    try:
        if not date_str:
            return None
        return datetime.strptime(date_str, format_pattern)
    except (ValueError, TypeError) as e:
        logger.warning(f"Не удалось распарсить дату '{date_str}': {e}")
        return None


def create_assigned_test_from_api_data(test_data: Any) -> AssignedTest | None:
    """
    Создает объект AssignedTest из данных API.

    Args:
        test_data: Данные теста из API

    Returns:
        Объект AssignedTest или None при ошибке
    """
    try:
        # Извлекаем данные с использованием безопасных методов
        # Конвертируем None в пустые строки для совместимости с базой данных
        test_name = safe_get_attr(test_data, "test_name", "") or ""
        user_name = safe_get_attr(test_data, "user_name", "") or ""
        head_name = safe_get_attr(test_data, "head_name", "") or ""
        creator_name = safe_get_attr(test_data, "creator_name", "") or ""
        status_name = safe_get_attr(test_data, "status_name", "") or ""
        active_from_str = safe_get_attr(test_data, "active_from", "") or ""

        # Парсим дату активации
        active_from = parse_test_date(active_from_str)
        if not active_from:
            logger.warning(
                f"Пропущен тест '{test_name}' из-за некорректной даты активации"
            )
            return None

        # Создаем объект AssignedTest
        return AssignedTest(
            test_name=test_name,
            employee_fullname=user_name,
            head_fullname=head_name,
            creator_fullname=creator_name,
            status=status_name,
            active_from=active_from,
        )

    except Exception as e:
        logger.error(f"Ошибка создания AssignedTest: {e}")
        return None


def validate_test_data(test_data: Any) -> bool:
    """
    Валидация данных теста.

    Args:
        test_data: Данные теста для проверки

    Returns:
        True если данные валидны, False иначе
    """
    required_fields = ["test_name", "user_name", "status_name", "active_from"]

    for field in required_fields:
        if not safe_get_attr(test_data, field):
            logger.warning(f"Отсутствует обязательное поле '{field}' в данных теста")
            return False

    # Проверяем, что можно распарсить дату
    active_from_str = safe_get_attr(test_data, "active_from", "")
    if not parse_test_date(active_from_str):
        return False

    return True


def filter_valid_tests(test_data_list: list[Any]) -> list[Any]:
    """
    Фильтрует список тестов, оставляя только валидные.

    Args:
        test_data_list: Список данных тестов

    Returns:
        Список валидных данных тестов
    """
    valid_tests = []
    invalid_count = 0

    for test_data in test_data_list:
        if validate_test_data(test_data):
            valid_tests.append(test_data)
        else:
            invalid_count += 1

    if invalid_count > 0:
        logger.info(
            f"Отфильтровано {invalid_count} невалидных тестов из {len(test_data_list)}"
        )

    return valid_tests


def get_default_subdivisions() -> list[int]:
    """
    Возвращает список подразделений по умолчанию для выгрузки тестов.

    Returns:
        Список ID подразделений
    """
    return [16231]


def format_date_range_for_tests(start_date: str, end_date: str) -> tuple[str, str]:
    """
    Форматирует диапазон дат для API запроса тестов.

    Args:
        start_date: Начальная дата в формате DD.MM.YYYY
        end_date: Конечная дата в формате DD.MM.YYYY

    Returns:
        Кортеж отформатированных дат
    """
    return start_date, end_date
