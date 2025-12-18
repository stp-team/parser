from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import Delete
from sqlalchemy.ext.asyncio import AsyncSession
from stp_database.models.Stats import HeadPremium, SpecPremium

from app.api.premium import PremiumAPI
from app.core.db import get_stats_session
from app.tasks.base import (
    APIProcessor,
    BatchDBOperator,
    ConcurrentAPIFetcher,
    PeriodHelper,
    safe_get_attr,
)

# Константы
HEAD_DIVISIONS = ["НТП", "НЦК"]
MONTHS_IN_HISTORY = 6

# Модели премии в БД
DBModel = SpecPremium | HeadPremium


def generate_premium_periods(months: int = MONTHS_IN_HISTORY) -> list[str]:
    """
    Генерирует список периодов за последние N месяцев, включая текущий месяц.
    Использует PeriodHelper напрямую.

    Args:
        months: Количество месяцев для генерации

    Returns:
        Список периодов в формате ["01.12.2025", "01.11.2025", ...]
    """
    # Используем PeriodHelper для получения недавних месяцев, включая текущий
    recent_months = PeriodHelper.get_recent_months(months, include_current=True)

    # Конвертируем в нужный формат (01.MM.YYYY)
    periods = []
    for month_str in recent_months:
        start_date, _ = PeriodHelper.parse_period_string(month_str)
        period = PeriodHelper.format_date_for_api(start_date, "DD.MM.YYYY")
        periods.append(period)

    return periods


@dataclass
class PremiumProcessingConfig:
    """Конфигурация для обработки данных премий."""

    update_type: str
    divisions: list[str]
    api_method: str
    db_model_class: type[DBModel]
    converter_func: Callable[[Any, datetime], DBModel]
    response_attr: str
    delete_func: Callable[[AsyncSession, datetime], Any]
    semaphore_limit: int = 10

    def get_delete_func(self):
        return self.delete_func


class PremiumDataConverter:
    """
    Конвертеры данных API в модели БД.
    """

    # Общие поля для обоих типов премий
    COMMON_FIELDS = [
        ("user_fullname", "fullname"),
        ("flr", "flr"),
        ("flr_normative", "flr_normative"),
        ("flr_normative_rate", "flr_normative_rate"),
        ("flr_premium", "flr_premium"),
        ("gok", "gok"),
        ("gok_normative", "gok_normative"),
        ("gok_normative_rate", "gok_normative_rate"),
        ("gok_premium", "gok_premium"),
        ("target", "target"),
        ("target_type", "target_type"),
        ("target_normative_first", "target_normative_first"),
        ("target_normative_second", "target_normative_second"),
        ("target_normative_rate_first", "target_normative_rate_first"),
        ("target_normative_rate_second", "target_normative_rate_second"),
        ("target_premium", "target_premium"),
        ("pers_target_manual", "pers_target_manual"),
        ("head_adjust_premium", "head_adjust_premium"),
        ("total_premium", "total_premium"),
    ]

    # Специфичные поля для специалистов
    SPEC_SPECIFIC_FIELDS = [
        ("total_contacts", "contacts_count"),
        ("csi", "csi"),
        ("csi_normative", "csi_normative"),
        ("csi_normative_rate", "csi_normative_rate"),
        ("csi_premium", "csi_premium"),
        ("csi_response", "csi_response"),
        ("csi_response_normative", "csi_response_normative"),
        ("csi_response_normative_rate", "csi_response_normative_rate"),
        ("discipline_premium", "discipline_premium"),
        ("tests_premium", "tests_premium"),
        ("thanks_premium", "thanks_premium"),
        ("tutors_premium", "tutors_premium"),
    ]

    # Специфичные поля для руководителей
    HEAD_SPECIFIC_FIELDS = [
        ("sl", "sl"),
        ("sl_normative_first", "sl_normative_first"),
        ("sl_normative_second", "sl_normative_second"),
        ("sl_normative_rate_first", "sl_normative_rate_first"),
        ("sl_normative_rate_second", "sl_normative_rate_second"),
        ("sl_premium", "sl_premium"),
    ]

    @staticmethod
    def _convert_premium_data(
        row: Any,
        model_class: type[DBModel],
        field_mapping: list[tuple[str, str]],
        extraction_period: datetime,
    ) -> DBModel:
        """
        Конвертер данных премий.

        Args:
            row: Строка данных из API
            model_class: Класс модели (SpecPremium или HeadPremium)
            field_mapping: Маппинг полей (api_field, model_field)
            extraction_period: Период извлечения данных

        Returns:
            Экземпляр модели с заполненными данными
        """
        kwargs = {"extraction_period": extraction_period}

        # Заполняем поля согласно маппингу
        for api_field, model_field in field_mapping:
            value = safe_get_attr(row, api_field)
            if value is not None:
                kwargs[model_field] = value

        return model_class(**kwargs)

    @staticmethod
    def convert_spec_premium_data(row: Any, extraction_period: datetime) -> SpecPremium:
        """Конвертирует данные API в модель SpecPremium."""
        all_fields = (
            PremiumDataConverter.COMMON_FIELDS
            + PremiumDataConverter.SPEC_SPECIFIC_FIELDS
        )
        return PremiumDataConverter._convert_premium_data(
            row, SpecPremium, all_fields, extraction_period
        )

    @staticmethod
    def convert_head_premium_data(row: Any, extraction_period: datetime) -> HeadPremium:
        """Конвертирует данные API в модель HeadPremium."""
        all_fields = (
            PremiumDataConverter.COMMON_FIELDS
            + PremiumDataConverter.HEAD_SPECIFIC_FIELDS
        )
        return PremiumDataConverter._convert_premium_data(
            row, HeadPremium, all_fields, extraction_period
        )


class PremiumDBManager:
    """Управление операциями БД для премий."""

    @staticmethod
    async def delete_old_premium_data(
        session: AsyncSession, model_class: type[DBModel], extraction_period: datetime
    ) -> None:
        """Универсальная функция удаления старых данных премий."""
        query = Delete(model_class).where(
            model_class.extraction_period == extraction_period
        )
        await session.execute(query)

    @staticmethod
    async def delete_old_spec_data(
        session: AsyncSession, extraction_period: datetime
    ) -> None:
        """Удаляет данные SpecPremium за указанный период."""
        await PremiumDBManager.delete_old_premium_data(
            session, SpecPremium, extraction_period
        )

    @staticmethod
    async def delete_old_head_data(
        session: AsyncSession, extraction_period: datetime
    ) -> None:
        """Удаляет данные HeadPremium за указанный период."""
        await PremiumDBManager.delete_old_premium_data(
            session, HeadPremium, extraction_period
        )


class PremiumProcessor(APIProcessor[DBModel, PremiumProcessingConfig]):
    """Процессор для обработки данных премий."""

    def __init__(self, api: PremiumAPI):
        super().__init__(api)
        self.fetcher = ConcurrentAPIFetcher()

    async def fetch_data(
        self, config: PremiumProcessingConfig, periods: list[str] = None, **kwargs
    ) -> list[tuple[Any, ...]]:
        """Получает данные премий из API для указанных периодов. Если периоды не указаны, используются последние 2 месяца."""
        if not periods:
            periods = generate_premium_periods(2)

        tasks = []
        for period in periods:
            for division in config.divisions:
                tasks.append((period, division))

        async def fetch_premium_for_division(period, division):
            try:
                api_method = getattr(self.api, config.api_method)
                result = await api_method(period=period, division=division)
                return period, division, result
            except Exception as e:
                self.logger.error(
                    f"Ошибка получения премий для {division}, период {period}: {e}"
                )
                return period, division, None

        return await self.fetcher.fetch_parallel(tasks, fetch_premium_for_division)

    def process_results(
        self, results: list[tuple[Any, ...]], config: PremiumProcessingConfig, **kwargs
    ) -> list[DBModel]:
        """Обрабатывает результаты API и конвертирует в модели БД."""
        premium_list = []

        for result in results:
            if isinstance(result, Exception):
                continue

            period, division, api_result = result[1]

            if not api_result or not hasattr(api_result, config.response_attr):
                self.logger.warning(
                    f"Нет данных премий для {division}, период {period}"
                )
                continue

            data_items = getattr(api_result, config.response_attr)
            for row in data_items:
                # Используем период из строки для совместимости
                row_period = datetime.strptime(row.period, "%d.%m.%Y")
                premium_model = config.converter_func(row, row_period)
                premium_list.append(premium_model)

        return premium_list

    async def save_data(
        self, data: list[DBModel], config: PremiumProcessingConfig, **kwargs
    ) -> int:
        """Сохраняет данные премий в БД."""
        if not data:
            self.logger.warning(f"Нет данных {config.update_type} для сохранения")
            return 0

        async with get_stats_session() as session:
            db_operator = BatchDBOperator(session)

            # Группируем данные по периодам для правильного удаления
            periods_to_clean = {item.extraction_period for item in data}

            # Удаляем старые данные за все обрабатываемые периоды
            for period in periods_to_clean:
                await config.delete_func(session, period)

            # Вставляем новые данные
            return await db_operator.bulk_insert_with_cleanup(
                data,
                None,
                config.update_type,  # delete_func=None, так как уже очистили выше
            )
