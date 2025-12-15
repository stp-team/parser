import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import Delete
from sqlalchemy.ext.asyncio import AsyncSession
from stp_database.models.Stats import HeadPremium, SpecPremium

from app.api.premium import PremiumAPI
from app.core.db import get_stats_session
from app.services.constants import unites
from app.services.helpers import get_current_month_first_day
from app.tasks.base import (
    APIProcessor,
    BatchDBOperator,
    ConcurrentAPIFetcher,
    log_processing_time,
)

logger = logging.getLogger(__name__)

# Constants
HEAD_DIVISIONS = ["НТП", "НЦК"]
MONTHS_IN_HISTORY = 6

# Type definitions
DBModel = SpecPremium | HeadPremium


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


class PeriodManager:
    """Утилита для управления периодами."""

    @staticmethod
    def generate_last_n_months_periods(months: int = MONTHS_IN_HISTORY) -> list[str]:
        """
        Генерирует список периодов за последние N месяцев.

        Args:
            months: Количество месяцев для генерации

        Returns:
            Список периодов в формате ["01.12.2025", "01.11.2025", ...]
        """
        periods = []
        current_date = datetime.now()

        for i in range(months):
            year = current_date.year
            month = current_date.month - i

            while month <= 0:
                month += 12
                year -= 1

            period = f"01.{month:02d}.{year}"
            periods.append(period)

        return periods

    @staticmethod
    def parse_period(period_str: str) -> datetime:
        """Парсит строку периода в datetime объект."""
        return datetime.strptime(period_str, "%d.%m.%Y")


class PremiumDataConverter:
    """Конвертеры данных API в модели БД."""

    @staticmethod
    def convert_spec_premium_data(row: Any, extraction_period: datetime) -> SpecPremium:
        """Конвертирует данные API в модель SpecPremium."""
        return SpecPremium(
            fullname=row.user_fullname,
            contacts_count=row.total_contacts,
            csi=row.csi,
            csi_normative=row.csi_normative,
            csi_normative_rate=row.csi_normative_rate,
            csi_premium=row.csi_premium,
            csi_response=row.csi_response,
            csi_response_normative=row.csi_response_normative,
            csi_response_normative_rate=row.csi_response_normative_rate,
            flr=row.flr,
            flr_normative=row.flr_normative,
            flr_normative_rate=row.flr_normative_rate,
            flr_premium=row.flr_premium,
            gok=row.gok,
            gok_normative=row.gok_normative,
            gok_normative_rate=row.gok_normative_rate,
            gok_premium=row.gok_premium,
            target=row.target,
            target_type=row.target_type,
            target_normative_first=row.target_normative_first,
            target_normative_second=row.target_normative_second,
            target_normative_rate_first=row.target_normative_rate_first,
            target_normative_rate_second=row.target_normative_rate_second,
            target_premium=row.target_premium,
            pers_target_manual=row.pers_target_manual,
            discipline_premium=row.discipline_premium,
            tests_premium=row.tests_premium,
            thanks_premium=row.thanks_premium,
            tutors_premium=row.tutors_premium,
            head_adjust_premium=row.head_adjust_premium,
            total_premium=row.total_premium,
            extraction_period=extraction_period,
        )

    @staticmethod
    def convert_head_premium_data(row: Any, extraction_period: datetime) -> HeadPremium:
        """Конвертирует данные API в модель HeadPremium."""
        return HeadPremium(
            fullname=row.user_fullname,
            flr=row.flr,
            flr_normative=row.flr_normative,
            flr_normative_rate=row.flr_normative_rate,
            flr_premium=row.flr_premium,
            gok=row.gok,
            gok_normative=row.gok_normative,
            gok_normative_rate=row.gok_normative_rate,
            gok_premium=row.gok_premium,
            target=row.target,
            target_type=row.target_type,
            target_normative_first=row.target_normative_first,
            target_normative_second=row.target_normative_second,
            target_normative_rate_first=row.target_normative_rate_first,
            target_normative_rate_second=row.target_normative_rate_second,
            target_premium=row.target_premium,
            pers_target_manual=row.pers_target_manual,
            sl=row.sl,
            sl_normative_first=row.sl_normative_first,
            sl_normative_second=row.sl_normative_second,
            sl_normative_rate_first=row.sl_normative_rate_first,
            sl_normative_rate_second=row.sl_normative_rate_second,
            sl_premium=row.sl_premium,
            head_adjust_premium=row.head_adjust_premium,
            total_premium=row.total_premium,
            extraction_period=extraction_period,
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
        """Получает данные премий из API для указанных периодов."""
        if not periods:
            periods = [get_current_month_first_day().strftime("%d.%m.%Y")]

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
                row_period = PeriodManager.parse_period(row.period)
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


# Конфигурации для разных типов премий
SPECIALIST_PREMIUM_CONFIG = PremiumProcessingConfig(
    update_type="Премии специалистов",
    divisions=unites,
    api_method="get_specialist_premium",
    db_model_class=SpecPremium,
    converter_func=PremiumDataConverter.convert_spec_premium_data,
    response_attr="items",
    delete_func=PremiumDBManager.delete_old_spec_data,
)

HEAD_PREMIUM_CONFIG = PremiumProcessingConfig(
    update_type="Премии руководителей",
    divisions=HEAD_DIVISIONS,
    api_method="get_head_premium",
    db_model_class=HeadPremium,
    converter_func=PremiumDataConverter.convert_head_premium_data,
    response_attr="premium",
    delete_func=PremiumDBManager.delete_old_head_data,
)


# Публичные функции API
@log_processing_time("заполнение премий специалистов")
async def fill_specialists_premium(api: PremiumAPI, period: str | None = None) -> None:
    """
    Получает данные о премиях специалистов.

    Args:
        api: Экземпляр API
        period: Период в формате "dd.mm.yyyy". Если не указан, используется текущий месяц.
    """
    processor = PremiumProcessor(api)

    periods = (
        [period] if period else [get_current_month_first_day().strftime("%d.%m.%Y")]
    )

    await processor.process_with_config(SPECIALIST_PREMIUM_CONFIG, periods=periods)


@log_processing_time("заполнение премий руководителей")
async def fill_heads_premium(api: PremiumAPI, period: str | None = None) -> None:
    """
    Получает данные о премиях руководителей.

    Args:
        api: Экземпляр API
        period: Период в формате "dd.mm.yyyy". Если не указан, используется текущий месяц.
    """
    processor = PremiumProcessor(api)

    periods = (
        [period] if period else [get_current_month_first_day().strftime("%d.%m.%Y")]
    )

    await processor.process_with_config(HEAD_PREMIUM_CONFIG, periods=periods)


@log_processing_time("заполнение премий специалистов за несколько периодов")
async def fill_specialists_premium_multiple_periods(
    api: PremiumAPI, periods: list[str] | None = None
) -> None:
    """
    Получает данные о премиях специалистов за несколько периодов.

    Args:
        api: Экземпляр API
        periods: Список периодов в формате "dd.mm.yyyy".
                 Если не указан, используются последние 6 месяцев.
    """
    processor = PremiumProcessor(api)

    if periods is None:
        periods = PeriodManager.generate_last_n_months_periods()

    await processor.process_with_config(SPECIALIST_PREMIUM_CONFIG, periods=periods)


@log_processing_time("заполнение премий руководителей за несколько периодов")
async def fill_heads_premium_multiple_periods(
    api: PremiumAPI, periods: list[str] | None = None
) -> None:
    """
    Получает данные о премиях руководителей за несколько периодов.

    Args:
        api: Экземпляр API
        periods: Список периодов в формате "dd.mm.yyyy".
                 Если не указан, используются последние 6 месяцев.
    """
    processor = PremiumProcessor(api)

    if periods is None:
        periods = PeriodManager.generate_last_n_months_periods()

    await processor.process_with_config(HEAD_PREMIUM_CONFIG, periods=periods)


@log_processing_time("заполнение всех премий за последние 6 месяцев")
async def fill_all_premium_last_6_months(api: PremiumAPI) -> None:
    """
    Получает данные о премиях всех сотрудников за последние 6 месяцев.

    Функция собирает данные о премиях специалистов и руководителей за последние 6 месяцев
    и сохраняет их в соответствующие таблицы БД параллельно.

    Args:
        api: Экземпляр API для получения данных о премиях
    """
    periods = PeriodManager.generate_last_n_months_periods()

    logger.info(f"Начинаем сбор всех премий за последние 6 месяцев: {periods}")

    # Запускаем сбор данных для специалистов и руководителей параллельно
    try:
        await asyncio.gather(
            fill_specialists_premium_multiple_periods(api, periods),
            fill_heads_premium_multiple_periods(api, periods),
            return_exceptions=True,
        )
    except Exception as e:
        logger.error(f"Ошибка при сборе премий: {e}")
        raise
