import asyncio
import calendar
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from time import perf_counter
from typing import Any, Generic, TypeVar, Union

from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

# Type variables for generic classes
T = TypeVar("T")  # Type for data models
ConfigT = TypeVar("ConfigT")  # Type for configuration


@dataclass
class ProcessingConfig(ABC):
    """Базовая конфигурация для обработки данных."""

    update_type: str

    @abstractmethod
    def get_delete_func(self) -> Callable[[AsyncSession, ...], Any]:
        """Возвращает функцию удаления старых данных."""
        pass


class APIProcessor(Generic[T, ConfigT], ABC):
    """Базовый класс для обработки API данных."""

    def __init__(self, api: Any):
        self.api = api
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    async def fetch_data(self, config: ConfigT, **kwargs) -> list[tuple[Any, ...]]:
        """Получает данные из API."""
        pass

    @abstractmethod
    def process_results(
        self, results: list[tuple[Any, ...]], config: ConfigT, **kwargs
    ) -> list[T]:
        """Обрабатывает результаты API и конвертирует в модели данных."""
        pass

    @abstractmethod
    async def save_data(self, data: list[T], config: ConfigT, **kwargs) -> int:
        """Сохраняет данные в БД."""
        pass

    async def process_with_config(self, config: ConfigT, **kwargs) -> int:
        """Универсальная обработка данных с конфигурацией."""
        timer_start = perf_counter()
        self.logger.info(f"Начинаем обработку {config.update_type}")

        try:
            # Получаем данные из API
            results = await self.fetch_data(config, **kwargs)

            # Обрабатываем результаты
            processed_data = self.process_results(results, config, **kwargs)

            if not processed_data:
                self.logger.warning(f"Нет данных для обработки в {config.update_type}")
                return 0

            # Сохраняем в БД
            updated_count = await self.save_data(processed_data, config, **kwargs)

            timer_stop = perf_counter()
            self.logger.info(
                f"Завершена обработка {config.update_type}: "
                f"обработано {updated_count} записей за {timer_stop - timer_start:.2f} секунд"
            )
            return updated_count

        except Exception as e:
            timer_stop = perf_counter()
            self.logger.error(
                f"Ошибка обработки {config.update_type} за {timer_stop - timer_start:.2f} секунд: {e}"
            )
            raise


class ConcurrentAPIFetcher:
    """Утилита для параллельных API запросов с ограничением семафора."""

    def __init__(self, semaphore_limit: int = 10):
        self.semaphore_limit = semaphore_limit
        self.logger = logging.getLogger(self.__class__.__name__)

    async def fetch_parallel(
        self, tasks: list[tuple[Any, ...]], task_executor: Callable[..., Any]
    ) -> list[tuple[Any, ...]]:
        """
        Выполняет список задач параллельно с ограничением семафора.

        Args:
            tasks: Список кортежей с параметрами для task_executor
            task_executor: Функция для выполнения задачи

        Returns:
            Список результатов в том же порядке, что и задачи
        """
        semaphore = asyncio.Semaphore(self.semaphore_limit)

        async def limited_task(task_params):
            async with semaphore:
                return await task_executor(*task_params)

        # Создаем корутины для всех задач
        coroutines = [limited_task(task) for task in tasks]

        # Выполняем параллельно
        results = await asyncio.gather(*coroutines, return_exceptions=True)

        # Обрабатываем исключения
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.error(f"Ошибка выполнения задачи {i}: {result}")
                processed_results.append((tasks[i], None))
            else:
                processed_results.append((tasks[i], result))

        return processed_results


class BatchDBOperator:
    """Утилита для массовых операций с БД."""

    def __init__(self, session: AsyncSession):
        self.session = session
        self.logger = logging.getLogger(self.__class__.__name__)

    async def bulk_update_with_transaction(
        self,
        updates: list[dict[str, Any]],
        update_func: Callable[[dict[str, Any]], Any],
        operation_name: str,
    ) -> int:
        """
        Выполняет массовые обновления в одной транзакции.

        Args:
            updates: Список словарей с данными для обновления
            update_func: Функция для выполнения одного обновления
            operation_name: Название операции для логирования

        Returns:
            Количество успешно обновленных записей
        """
        if not updates:
            self.logger.info(f"[{operation_name}] Нет данных для обновления")
            return 0

        try:
            updated_count = 0

            for update_data in updates:
                await update_func(update_data)
                updated_count += 1

            await self.session.commit()
            self.logger.info(
                f"[{operation_name}] Успешно обновлено {updated_count} записей одной транзакцией"
            )
            return updated_count

        except Exception as e:
            self.logger.error(f"[{operation_name}] Ошибка при массовом обновлении: {e}")
            await self.session.rollback()
            return 0

    async def bulk_insert_with_cleanup(
        self,
        data_list: list[Any],
        delete_func: Callable[[], Any] | None,
        operation_name: str,
    ) -> int:
        """
        Выполняет массовую вставку с предварительной очисткой данных.

        Args:
            data_list: Список объектов для вставки
            delete_func: Функция для удаления старых данных (опционально)
            operation_name: Название операции для логирования

        Returns:
            Количество вставленных записей
        """
        if not data_list:
            self.logger.warning(f"[{operation_name}] Нет данных для вставки")
            return 0

        try:
            # Удаляем старые данные, если функция предоставлена
            if delete_func:
                await delete_func()

            # Вставляем новые данные
            self.session.add_all(data_list)
            await self.session.commit()

            self.logger.info(
                f"[{operation_name}] Успешно вставлено {len(data_list)} записей"
            )
            return len(data_list)

        except Exception as e:
            self.logger.error(f"[{operation_name}] Ошибка при массовой вставке: {e}")
            await self.session.rollback()
            return 0


def log_processing_time(operation_name: str):
    """Декоратор для логирования времени выполнения операций."""

    def decorator(func):
        async def wrapper(*args, **kwargs):
            timer_start = perf_counter()
            logger.info(f"Начинаем {operation_name}")

            try:
                result = await func(*args, **kwargs)
                timer_stop = perf_counter()
                logger.info(
                    f"Завершен {operation_name} за {timer_stop - timer_start:.2f} секунд"
                )
                return result
            except Exception as e:
                timer_stop = perf_counter()
                logger.error(
                    f"Ошибка {operation_name} за {timer_stop - timer_start:.2f} секунд: {e}"
                )
                raise

        return wrapper

    return decorator


def create_lookup_index(
    items: list[Any], key_func: Callable[[Any], str]
) -> dict[str, Any]:
    """Создает индекс для быстрого поиска по ключу."""
    return {key_func(item): item for item in items}


def filter_items(items: list[Any], filter_func: Callable[[Any], bool]) -> list[Any]:
    """Фильтрует элементы по предоставленной функции."""
    return [item for item in items if filter_func(item)]


def validate_api_result(result: Any, expected_attr: str = None) -> bool:
    """
    Универсальная валидация результатов API для устранения дублирования кода.

    Args:
        result: Результат API вызова
        expected_attr: Ожидаемый атрибут для проверки (опционально)

    Returns:
        True если результат валидный, False иначе

    Examples:
        >>> validate_api_result(None)
        False
        >>> validate_api_result(api_response, "data")
        True  # если api_response.data существует
    """
    if isinstance(result, Exception):
        return False

    if result is None:
        return False

    if expected_attr and not hasattr(result, expected_attr):
        return False

    if expected_attr and getattr(result, expected_attr) is None:
        return False

    return True


def safe_get_attr(obj: Any, attr: str, default=None, converter: Callable = None):
    """
    Безопасное получение атрибута с опциональным преобразованием типа.

    Args:
        obj: Объект для извлечения атрибута
        attr: Имя атрибута
        default: Значение по умолчанию
        converter: Функция преобразования (например, int, float, str)

    Returns:
        Значение атрибута или default

    Examples:
        >>> safe_get_attr(obj, "value", 0, int)
        42  # если obj.value = "42"
        >>> safe_get_attr(obj, "missing", "N/A")
        "N/A"  # если obj.missing не существует
    """
    try:
        value = getattr(obj, attr, default)
        if value is None:
            return default

        if converter and value != default:
            try:
                return converter(value)
            except (ValueError, TypeError):
                logger.warning(
                    f"Не удалось преобразовать {attr}={value} с помощью {converter}"
                )
                return default

        return value
    except AttributeError:
        return default


def validate_and_extract_data(
    results: list[tuple[Any, ...]],
    validator_func: Callable[[Any], bool],
    operation_name: str = "обработка данных",
) -> list[tuple[Any, ...]]:
    """
    Фильтрация и валидация результатов API с логированием предупреждений.

    Args:
        results: Список кортежей (task_params, api_result)
        validator_func: Функция валидации для каждого результата
        operation_name: Название операции для логирования

    Returns:
        Список валидных результатов

    Examples:
        >>> valid_results = validate_and_extract_data(
        ...     results,
        ...     lambda r: validate_api_result(r, "data"),
        ...     "получение KPI"
        ... )
    """
    valid_results = []
    invalid_count = 0

    for task_params, api_result in results:
        if not validator_func(api_result):
            invalid_count += 1
            logger.warning(
                f"[{operation_name}] Невалидный результат для задачи {task_params}: {type(api_result)}"
            )
            continue

        valid_results.append((task_params, api_result))

    if invalid_count > 0:
        logger.info(
            f"[{operation_name}] Отфильтровано {invalid_count} невалидных результатов из {len(results)}"
        )

    return valid_results


# =====================================================================================
# PHASE 1.2: DATA MAPPING UTILITIES
# =====================================================================================


class DataFieldMapper:
    """
    Универсальный маппер полей для устранения ручного поэлементного сопоставления.
    Заменяет повторяющиеся паттерны if/elif для конвертации данных.
    """

    def __init__(self, field_mapping: dict[str, str], default_value: Any = None):
        """
        Инициализация маппера полей.

        Args:
            field_mapping: Словарь {source_field: target_attribute}
            default_value: Значение по умолчанию для отсутствующих полей

        Examples:
            >>> mapper = DataFieldMapper({
            ...     "Поступило": "received_contacts",
            ...     "Принято": "accepted_contacts",
            ...     "% Пропущенных": "missed_contacts_percent"
            ... })
        """
        self.field_mapping = field_mapping
        self.default_value = default_value
        self.logger = logging.getLogger(self.__class__.__name__)

    def map_data_to_object(
        self,
        data_items: list[Any],
        target_obj: Any,
        source_field: str = "text",
        value_field: str = "value",
    ) -> Any:
        """
        Заполняет объект данными на основе маппинга полей.

        Args:
            data_items: Список элементов данных (например, API response items)
            target_obj: Целевой объект для заполнения
            source_field: Поле в data_items для сопоставления (например, "text")
            value_field: Поле в data_items со значением (например, "value")

        Returns:
            Заполненный целевой объект

        Examples:
            >>> sl_object = SL()
            >>> mapper.map_data_to_object(sl.totalData, sl_object, "text", "value")
        """
        mapped_fields = set()

        for item in data_items:
            source_value = safe_get_attr(item, source_field)
            if source_value in self.field_mapping:
                target_attr = self.field_mapping[source_value]
                raw_value = safe_get_attr(item, value_field, self.default_value)

                # Автоматическое определение типа из аннотаций
                target_type = self._get_target_type(target_obj, target_attr)
                converted_value = self._convert_value(
                    raw_value, target_type, source_value
                )

                setattr(target_obj, target_attr, converted_value)
                mapped_fields.add(target_attr)

        # Логирование немапленных полей
        unmapped_fields = set(self.field_mapping.values()) - mapped_fields
        if unmapped_fields:
            self.logger.warning(f"Не найдены данные для полей: {unmapped_fields}")

        return target_obj

    def _get_target_type(self, target_obj: Any, attr_name: str):
        """Получает тип целевого атрибута из аннотаций типов."""
        try:
            annotations = getattr(target_obj.__class__, "__annotations__", {})
            target_type = annotations.get(attr_name, str)

            # Обрабатываем SQLAlchemy Mapped типы
            if (
                hasattr(target_type, "__origin__")
                and target_type.__origin__.__name__ == "Mapped"
            ):
                # Извлекаем внутренний тип из Mapped[внутренний_тип]
                target_type = target_type.__args__[0] if target_type.__args__ else str

            return target_type
        except:
            return str

    def _convert_value(
        self, value: Any, target_type: type, field_name: str = ""
    ) -> Any:
        """Конвертирует значение в целевой тип с обработкой ошибок."""
        if value is None or value == self.default_value:
            return self.default_value

        try:
            # Рекурсивная обработка SQLAlchemy Mapped типов
            if hasattr(target_type, "__origin__") and hasattr(
                target_type.__origin__, "__name__"
            ):
                if target_type.__origin__.__name__ == "Mapped":
                    # Извлекаем внутренний тип из Mapped[внутренний_тип]
                    target_type = (
                        target_type.__args__[0] if target_type.__args__ else str
                    )

            # Обработка Union types (например, Optional[int])
            if hasattr(target_type, "__origin__"):
                if target_type.__origin__ is type(Union):
                    # Берем первый не-None тип
                    args = [
                        arg for arg in target_type.__args__ if arg is not type(None)
                    ]
                    target_type = args[0] if args else str

            # Дополнительная проверка для базовых типов Python
            if target_type is int:
                return int(float(value))  # Сначала float для обработки "42.0"
            elif target_type is float:
                return float(value)
            elif target_type is str:
                return str(value)
            elif target_type is bool:
                return bool(value)
            elif hasattr(target_type, "__name__") and target_type.__name__ in [
                "int",
                "float",
                "str",
                "bool",
            ]:
                # Обрабатываем случаи, когда target_type это строковое представление типа
                if target_type.__name__ == "int":
                    return int(float(value))
                elif target_type.__name__ == "float":
                    return float(value)
                elif target_type.__name__ == "str":
                    return str(value)
                elif target_type.__name__ == "bool":
                    return bool(value)
            else:
                # Попытка прямого преобразования только для безопасных типов
                if callable(target_type) and not hasattr(target_type, "__origin__"):
                    return target_type(value)
                else:
                    # Для сложных типов возвращаем строку как fallback
                    return str(value)

        except (ValueError, TypeError) as e:
            self.logger.warning(
                f"Не удалось конвертировать значение '{value}' в тип {target_type} "
                f"для поля '{field_name}': {e}"
            )
            return self.default_value


def create_data_converter(
    field_mapping: dict[str, str],
    default_value: Any = None,
    converter_functions: dict[str, Callable] = None,
) -> Callable:
    """
    Фабрика для создания функций конвертации данных.

    Args:
        field_mapping: Словарь сопоставления {source: target}
        default_value: Значение по умолчанию
        converter_functions: Дополнительные функции конвертации для специфичных полей

    Returns:
        Функция конвертации данных

    Examples:
        >>> sl_converter = create_data_converter({
        ...     "Поступило": "received_contacts",
        ...     "SL": "sl"
        ... }, converter_functions={"sl": lambda x: float(x) / 100})
        >>> sl_object = sl_converter(api_data, SL())
    """
    mapper = DataFieldMapper(field_mapping, default_value)
    custom_converters = converter_functions or {}

    def convert_data(source_data: list[Any], target_obj: Any) -> Any:
        """Применяет конвертацию данных с дополнительными функциями."""
        # Основная конвертация
        result = mapper.map_data_to_object(source_data, target_obj)

        # Применение пользовательских конвертеров
        for field_name, converter_func in custom_converters.items():
            if hasattr(result, field_name):
                try:
                    current_value = getattr(result, field_name)
                    if current_value is not None:
                        setattr(result, field_name, converter_func(current_value))
                except Exception as e:
                    logger.warning(
                        f"Ошибка применения конвертера для {field_name}: {e}"
                    )

        return result

    return convert_data


# =====================================================================================
# PHASE 1.3: PERIOD MANAGEMENT UTILITIES
# =====================================================================================


class PeriodHelper:
    """
    Утилита для консолидации логики работы с датами и периодами.
    Устраняет дублирование date/period logic между модулями.
    Использует только стандартную библиотеку Python.
    """

    @staticmethod
    def _add_months(date: datetime, months: int) -> datetime:
        """Добавляет месяцы к дате, используя только стандартную библиотеку."""
        year = date.year
        month = date.month + months

        # Корректируем год и месяц
        while month > 12:
            year += 1
            month -= 12
        while month < 1:
            year -= 1
            month += 12

        # Корректируем день, если он не существует в новом месяце
        day = min(date.day, calendar.monthrange(year, month)[1])
        return date.replace(year=year, month=month, day=day)

    @staticmethod
    def parse_period_string(period: str) -> tuple[datetime, datetime]:
        """
        Парсит строку периода и возвращает начальную и конечную даты.

        Args:
            period: Период в формате "YYYY-MM" или "YYYY-MM-DD"

        Returns:
            Кортеж (start_date, end_date)

        Examples:
            >>> start, end = PeriodHelper.parse_period_string("2024-12")
            >>> start  # 2024-12-01 00:00:00
            >>> end    # 2024-12-31 23:59:59
        """
        if "-" in period and len(period.split("-")) == 2:
            # Формат "YYYY-MM"
            year, month = map(int, period.split("-"))
            start_date = datetime(year, month, 1)
            # Последний день месяца
            last_day = calendar.monthrange(year, month)[1]
            end_date = datetime(year, month, last_day, 23, 59, 59)
        elif "-" in period and len(period.split("-")) == 3:
            # Формат "YYYY-MM-DD"
            start_date = datetime.strptime(period, "%Y-%m-%d")
            end_date = start_date.replace(hour=23, minute=59, second=59)
        else:
            raise ValueError(f"Неподдерживаемый формат периода: {period}")

        return start_date, end_date

    @staticmethod
    def generate_period_list(
        start_date: str, end_date: str, format_type: str = "YYYY-MM"
    ) -> list[str]:
        """
        Генерирует список периодов между двумя датами.

        Args:
            start_date: Начальная дата (строка)
            end_date: Конечная дата (строка)
            format_type: Формат вывода ("YYYY-MM", "YYYY-MM-DD")

        Returns:
            Список строк периодов

        Examples:
            >>> PeriodHelper.generate_period_list("2024-10", "2024-12")
            ["2024-10", "2024-11", "2024-12"]
        """
        if format_type == "YYYY-MM":
            start_dt = datetime.strptime(start_date, "%Y-%m")
            end_dt = datetime.strptime(end_date, "%Y-%m")

            periods = []
            current = start_dt
            while current <= end_dt:
                periods.append(current.strftime("%Y-%m"))
                current = PeriodHelper._add_months(current, 1)

        elif format_type == "YYYY-MM-DD":
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")

            periods = []
            current = start_dt
            while current <= end_dt:
                periods.append(current.strftime("%Y-%m-%d"))
                current += timedelta(days=1)
        else:
            raise ValueError(f"Неподдерживаемый формат: {format_type}")

        return periods

    @staticmethod
    def get_previous_months(
        months_count: int = 2, from_date: datetime = None
    ) -> list[str]:
        """
        Возвращает список предыдущих месяцев в формате YYYY-MM.

        Args:
            months_count: Количество месяцев для получения
            from_date: Базовая дата (по умолчанию - текущая)

        Returns:
            Список месяцев

        Examples:
            >>> PeriodHelper.get_previous_months(2)  # Если сейчас 2024-12
            ["2024-10", "2024-11"]
        """
        base_date = from_date or datetime.now()
        periods = []

        for i in range(months_count, 0, -1):
            target_date = PeriodHelper._add_months(base_date, -i)
            periods.append(target_date.strftime("%Y-%m"))

        return periods

    @staticmethod
    def get_recent_months(
        months_count: int = 2, from_date: datetime = None, include_current: bool = True
    ) -> list[str]:
        """
        Возвращает список недавних месяцев в формате YYYY-MM, включая текущий месяц.

        Args:
            months_count: Количество месяцев для получения
            from_date: Базовая дата (по умолчанию - текущая)
            include_current: Включать ли текущий месяц (по умолчанию True)

        Returns:
            Список месяцев

        Examples:
            >>> PeriodHelper.get_recent_months(2)  # Если сейчас 2024-12
            ["2024-11", "2024-12"]
            >>> PeriodHelper.get_recent_months(3, include_current=True)  # Если сейчас 2024-12
            ["2024-10", "2024-11", "2024-12"]
        """
        base_date = from_date or datetime.now()
        periods = []

        if include_current:
            # Включаем текущий месяц плюс предыдущие (months_count - 1) месяцев
            start_offset = months_count - 1
            end_offset = -1
        else:
            # Только предыдущие месяцы
            start_offset = months_count
            end_offset = 0

        for i in range(start_offset, end_offset, -1):
            target_date = PeriodHelper._add_months(base_date, -i)
            periods.append(target_date.strftime("%Y-%m"))

        return periods

    @staticmethod
    def get_date_range_for_period(period: str) -> tuple[str, str]:
        """
        Возвращает диапазон дат для заданного периода.

        Args:
            period: Период в формате "YYYY-MM"

        Returns:
            Кортеж (start_date_str, end_date_str) в формате "DD.MM.YYYY"

        Examples:
            >>> PeriodHelper.get_date_range_for_period("2024-12")
            ("01.12.2024", "31.12.2024")
        """
        start_date, end_date = PeriodHelper.parse_period_string(period)
        return (start_date.strftime("%d.%m.%Y"), end_date.strftime("%d.%m.%Y"))

    @staticmethod
    def format_date_for_api(date_obj: datetime, api_format: str = "DD.MM.YYYY") -> str:
        """
        Форматирует дату для API запросов.

        Args:
            date_obj: Объект datetime
            api_format: Формат для API ("DD.MM.YYYY", "YYYY-MM-DD")

        Returns:
            Отформатированная строка даты

        Examples:
            >>> dt = datetime(2024, 12, 15)
            >>> PeriodHelper.format_date_for_api(dt, "DD.MM.YYYY")
            "15.12.2024"
        """
        if api_format == "DD.MM.YYYY":
            return date_obj.strftime("%d.%m.%Y")
        elif api_format == "YYYY-MM-DD":
            return date_obj.strftime("%Y-%m-%d")
        else:
            raise ValueError(f"Неподдерживаемый формат API: {api_format}")

    @staticmethod
    def is_same_month(date1: datetime, date2: datetime) -> bool:
        """
        Проверяет, относятся ли две даты к одному месяцу.

        Args:
            date1: Первая дата
            date2: Вторая дата

        Returns:
            True если даты в одном месяце
        """
        return date1.year == date2.year and date1.month == date2.month

    @staticmethod
    def get_month_boundaries(date: datetime) -> tuple[datetime, datetime]:
        """
        Возвращает границы месяца для заданной даты.

        Args:
            date: Дата в месяце

        Returns:
            Кортеж (первый_день_месяца, последний_день_месяца)
        """
        first_day = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_day_num = calendar.monthrange(date.year, date.month)[1]
        last_day = date.replace(
            day=last_day_num, hour=23, minute=59, second=59, microsecond=999999
        )
        return first_day, last_day


# =====================================================================================
# PHASE 1.4: ENHANCED TASK WRAPPER
# =====================================================================================


def create_task_function(
    processor_class: type[APIProcessor],
    config_factory: Callable[..., Any],
    operation_name: str,
    return_type_converter: Callable[[int], Any] = None,
) -> Callable:
    """
    Универсальная фабрика для создания публичных функций задач.
    Устраняет дублирование @log_processing_time декоратора и стандартной логики.

    Args:
        processor_class: Класс процессора (наследник APIProcessor)
        config_factory: Функция создания конфигурации
        operation_name: Название операции для логирования
        return_type_converter: Функция преобразования результата (опционально)

    Returns:
        Готовая async функция для использования в задачах

    Examples:
        >>> fill_sl = create_task_function(
        ...     SLProcessor,
        ...     lambda: SL_CONFIG,
        ...     "заполнение Service Level данных"
        ... )
        >>> # Эквивалентно:
        >>> @log_processing_time("заполнение Service Level данных")
        >>> async def fill_sl(api: SlAPI) -> None:
        ...     processor = SLProcessor(api)
        ...     return await processor.process_with_config(SL_CONFIG)
    """

    @log_processing_time(operation_name)
    async def task_function(api, **kwargs):
        """Универсальная функция задачи."""
        # Создаем конфигурацию с переданными параметрами
        config = config_factory(**kwargs)

        # Создаем процессор
        processor = processor_class(api)

        # Выполняем обработку
        result = await processor.process_with_config(config, **kwargs)

        # Преобразуем результат при необходимости
        if return_type_converter:
            return return_type_converter(result)

        return result

    # Сохраняем метаданные для отладки
    task_function.__name__ = f"generated_task_{processor_class.__name__}"
    task_function.__doc__ = f"Автогенерированная задача: {operation_name}"
    task_function._processor_class = processor_class
    task_function._operation_name = operation_name

    return task_function


def create_config_factory(config_class: type, **default_kwargs) -> Callable:
    """
    Создает фабрику конфигураций для стандартизации создания config объектов.

    Args:
        config_class: Класс конфигурации
        **default_kwargs: Значения по умолчанию

    Returns:
        Функция для создания конфигураций

    Examples:
        >>> sl_config_factory = create_config_factory(
        ...     SLProcessingConfig,
        ...     update_type="заполнение SL",
        ...     semaphore_limit=1
        ... )
        >>> config = sl_config_factory(start_date="15.12.2024")
    """

    def config_factory(**kwargs):
        """Создает конфигурацию с объединением default и переданных параметров."""
        merged_kwargs = {**default_kwargs, **kwargs}
        return config_class(**merged_kwargs)

    config_factory.__name__ = f"{config_class.__name__}_factory"
    config_factory._config_class = config_class
    config_factory._defaults = default_kwargs

    return config_factory


def create_standard_task_set(
    processor_class: type[APIProcessor], config_definitions: dict[str, dict]
) -> dict[str, Callable]:
    """
    Создает набор стандартных задач для процессора с разными конфигурациями.

    Args:
        processor_class: Класс процессора
        config_definitions: Словарь {name: {config_params, operation_name}}

    Returns:
        Словарь {task_name: task_function}

    Examples:
        >>> tasks = create_standard_task_set(KPIProcessor, {
        ...     "day": {
        ...         "config_params": {"period_type": "day"},
        ...         "operation_name": "заполнение дневных KPI"
        ...     },
        ...     "week": {
        ...         "config_params": {"period_type": "week"},
        ...         "operation_name": "заполнение недельных KPI"
        ...     }
        ... })
        >>> fill_day_kpi = tasks["day"]
        >>> fill_week_kpi = tasks["week"]
    """
    tasks = {}

    for task_name, definition in config_definitions.items():
        config_params = definition.get("config_params", {})
        operation_name = definition.get("operation_name", f"обработка {task_name}")
        return_converter = definition.get("return_converter")

        # Создаем фабрику конфигурации для этого типа задачи
        def make_config_factory(params):
            return lambda **kwargs: {**params, **kwargs}

        config_factory = make_config_factory(config_params)

        # Создаем функцию задачи
        task_func = create_task_function(
            processor_class, config_factory, operation_name, return_converter
        )

        tasks[task_name] = task_func

    return tasks


class TaskManager:
    """
    Менеджер для организации и управления задачами.
    Предоставляет единый интерфейс для регистрации и выполнения задач.
    """

    def __init__(self):
        self.tasks: dict[str, Callable] = {}
        self.task_metadata: dict[str, dict] = {}
        self.logger = logging.getLogger(self.__class__.__name__)

    def register_task(
        self,
        name: str,
        processor_class: type[APIProcessor],
        config_factory: Callable,
        operation_name: str,
        tags: list[str] = None,
    ) -> None:
        """
        Регистрирует задачу в менеджере.

        Args:
            name: Уникальное имя задачи
            processor_class: Класс процессора
            config_factory: Фабрика конфигураций
            operation_name: Описание операции
            tags: Теги для группировки задач
        """
        if name in self.tasks:
            raise ValueError(f"Задача с именем '{name}' уже зарегистрирована")

        task_func = create_task_function(
            processor_class, config_factory, operation_name
        )

        self.tasks[name] = task_func
        self.task_metadata[name] = {
            "processor_class": processor_class.__name__,
            "operation_name": operation_name,
            "tags": tags or [],
            "created_at": datetime.now().isoformat(),
        }

        self.logger.info(f"Зарегистрирована задача '{name}': {operation_name}")

    def get_task(self, name: str) -> Callable:
        """Получает задачу по имени."""
        if name not in self.tasks:
            raise KeyError(f"Задача '{name}' не найдена")
        return self.tasks[name]

    def list_tasks(self, tag: str = None) -> list[str]:
        """Возвращает список имен задач, опционально фильтруя по тегу."""
        if tag is None:
            return list(self.tasks.keys())

        return [
            name
            for name, metadata in self.task_metadata.items()
            if tag in metadata.get("tags", [])
        ]

    def get_task_info(self, name: str) -> dict:
        """Получает информацию о задаче."""
        if name not in self.task_metadata:
            raise KeyError(f"Задача '{name}' не найдена")
        return self.task_metadata[name].copy()

    async def run_task(self, name: str, api, **kwargs):
        """Выполняет задачу по имени."""
        task_func = self.get_task(name)
        return await task_func(api, **kwargs)
