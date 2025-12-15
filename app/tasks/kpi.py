"""
Модуль для заполнения и обновления данных KPI.

Оптимизированная версия с улучшенной архитектурой:
- Использует базовые классы для общей функциональности
- Поддерживает обработку KPI за день, неделю и месяц
- Эффективная параллельная обработка API запросов
- Агрегация данных по сотрудникам и улучшенные операции БД
"""

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import Delete
from sqlalchemy.ext.asyncio import AsyncSession
from stp_database.models.Stats import SpecDayKPI, SpecMonthKPI, SpecWeekKPI

from app.api.kpi import KpiAPI
from app.core.db import get_stats_session
from app.services.helpers import (
    calculate_days_in_month_period,
    calculate_days_in_week_period,
    get_current_month_first_day,
    get_month_period_for_kpi,
    get_week_start_date,
    get_yesterday_date,
)
from app.tasks.base import (
    APIProcessor,
    BatchDBOperator,
    ConcurrentAPIFetcher,
    log_processing_time,
)

logger = logging.getLogger(__name__)

# Type definitions
DBModel = SpecDayKPI | SpecMonthKPI | SpecWeekKPI


@dataclass
class KPIProcessingConfig:
    """Конфигурация для обработки данных KPI."""

    update_type: str
    divisions: list[str]
    report_types: list[str]
    db_model_class: type[DBModel]
    period_days: int | None  # None означает использование кастомных дат
    table_name: str
    delete_func: Callable[[AsyncSession], Any]
    start_date_func: Callable[[], 'datetime'] | None = None  # Функция для получения начальной даты
    extraction_period_func: Callable[[], 'datetime'] | None = None  # Функция для получения extraction_period
    use_custom_dates: bool = False  # Флаг использования кастомных дат
    use_week_period: bool = False  # Флаг для недельного периода
    semaphore_limit: int = 10

    def get_delete_func(self):
        return self.delete_func


class KPIDataAggregator:
    """Агрегатор данных KPI для объединения показателей по сотрудникам."""

    def __init__(self, model_class: type[DBModel]):
        self.model_class = model_class
        self.logger = logging.getLogger(self.__class__.__name__)

    def aggregate_results(
        self, results: list[tuple[Any, ...]], extraction_period=None
    ) -> list[DBModel]:
        """
        Агрегирует результаты API по сотрудникам.

        Args:
            results: Список результатов API
            extraction_period: Период извлечения данных

        Returns:
            Список агрегированных моделей KPI
        """
        if extraction_period is None:
            extraction_period = get_current_month_first_day()

        # Словарь для хранения ORM объектов по fullname
        kpi_objects_by_fullname = {}
        total_processed_rows = 0

        try:
            for result in results:
                if isinstance(result, Exception):
                    continue

                division, report_type, api_result = result[1]

                if (
                    not api_result
                    or not hasattr(api_result, "data")
                    or not api_result.data
                ):
                    self.logger.warning(
                        f"Не найдено показателей для направления: {division}, отчет: {report_type}"
                    )
                    continue

                for row in api_result.data:
                    fullname = getattr(row, "fullname", None)
                    if not fullname:
                        continue

                    # Получаем или создаем объект KPI для данного fullname
                    if fullname not in kpi_objects_by_fullname:
                        kpi_obj = self.model_class()
                        kpi_obj.fullname = fullname
                        kpi_obj.extraction_period = extraction_period
                        kpi_objects_by_fullname[fullname] = kpi_obj
                    else:
                        kpi_obj = kpi_objects_by_fullname[fullname]

                    # Обновляем показатели в зависимости от типа отчета
                    self._update_kpi_object(kpi_obj, row, report_type)
                    total_processed_rows += 1

        except Exception as e:
            self.logger.error(f"Ошибка при агрегации показателей: {e}")

        self.logger.debug(
            f"Обработано {total_processed_rows} строк в {len(kpi_objects_by_fullname)} агрегированных записях"
        )
        return list(kpi_objects_by_fullname.values())

    def _update_kpi_object(self, kpi_obj: DBModel, row: Any, report_type: str) -> None:
        """Обновляет объект KPI данными из строки API в зависимости от типа отчета."""
        report_type_lower = report_type.lower()

        if report_type_lower == "aht" and hasattr(row, "aht") and row.aht:
            self._update_aht_metrics(kpi_obj, row)
        elif report_type_lower == "flr" and hasattr(row, "flr") and row.flr:
            self._update_flr_metrics(kpi_obj, row)
        elif report_type_lower == "csi" and hasattr(row, "csi") and row.csi:
            kpi_obj.csi = row.csi
        elif report_type_lower == "pok" and hasattr(row, "pok") and row.pok:
            self._update_pok_metrics(kpi_obj, row)
        elif report_type_lower == "delay" and hasattr(row, "delay") and row.delay:
            kpi_obj.delay = row.delay
        elif report_type_lower == "sales":
            self._update_sales_metrics(kpi_obj, row)
        elif report_type_lower == "salespotential":
            self._update_sales_potential_metrics(kpi_obj, row)
        elif report_type_lower == "paidservice":
            self._update_paid_service_metrics(kpi_obj, row)

    def _update_aht_metrics(self, kpi_obj: DBModel, row: Any) -> None:
        """Обновляет метрики AHT."""
        kpi_obj.contacts_count = getattr(row, "aht_total_contacts", None)
        kpi_obj.aht = row.aht
        kpi_obj.aht_chats_web = getattr(row, "aht_chats_web", None)
        kpi_obj.aht_chats_dhcp = getattr(row, "aht_chats_dhcp", None)
        kpi_obj.aht_chats_mobile = getattr(row, "aht_chats_mobile", None)
        kpi_obj.aht_chats_smartdom = getattr(row, "aht_chats_smartdom", None)

    def _update_flr_metrics(self, kpi_obj: DBModel, row: Any) -> None:
        """Обновляет метрики FLR."""
        kpi_obj.flr = row.flr
        kpi_obj.flr_services = getattr(row, "flr_services", None)
        kpi_obj.flr_services_cross = getattr(row, "flr_services_cross", None)
        kpi_obj.flr_services_transfer = getattr(row, "flr_services_transfers", None)

    def _update_pok_metrics(self, kpi_obj: DBModel, row: Any) -> None:
        """Обновляет метрики POK."""
        kpi_obj.pok = row.pok
        kpi_obj.pok_rated_contacts = getattr(row, "pok_rated_contacts", None)

    def _update_sales_metrics(self, kpi_obj: DBModel, row: Any) -> None:
        """Обновляет метрики продаж."""
        kpi_obj.sales = getattr(row, "sales", None)
        kpi_obj.sales_videos = getattr(row, "sales_videos", None)
        kpi_obj.sales_routers = getattr(row, "sales_routers", None)
        kpi_obj.sales_tvs = getattr(row, "sales_tvs", None)
        kpi_obj.sales_intercoms = getattr(row, "sales_intercoms", None)
        kpi_obj.sales_conversion = getattr(row, "sales_conversion", None)

    def _update_sales_potential_metrics(self, kpi_obj: DBModel, row: Any) -> None:
        """Обновляет метрики потенциала продаж."""
        kpi_obj.sales_potential = getattr(row, "sales_potential", None)
        kpi_obj.sales_potential_video = getattr(row, "sales_potential_video", None)
        kpi_obj.sales_potential_routers = getattr(row, "sales_potential_routers", None)
        kpi_obj.sales_potential_tvs = getattr(row, "sales_potential_tvs", None)
        kpi_obj.sales_potential_intercoms = getattr(
            row, "sales_potential_intercoms", None
        )
        kpi_obj.sales_potential_conversion = getattr(
            row, "sales_potential_conversion", None
        )

    def _update_paid_service_metrics(self, kpi_obj: DBModel, row: Any) -> None:
        """Обновляет метрики платных услуг."""
        kpi_obj.services = getattr(row, "services", None)
        kpi_obj.services_remote = getattr(row, "services_remote", None)
        kpi_obj.services_onsite = getattr(row, "services_onsite", None)
        kpi_obj.services_conversion = getattr(row, "services_conversion", None)


class KPIDBManager:
    """Управление операциями БД для KPI."""

    @staticmethod
    async def delete_all_kpi_data(
        session: AsyncSession, model_class: type[DBModel]
    ) -> None:
        """Универсальная функция удаления всех данных KPI."""
        query = Delete(model_class)
        await session.execute(query)

    @staticmethod
    async def delete_old_day_data(session: AsyncSession) -> None:
        """Удаляет все данные SpecDayKPI."""
        await KPIDBManager.delete_all_kpi_data(session, SpecDayKPI)

    @staticmethod
    async def delete_old_week_data(session: AsyncSession) -> None:
        """Удаляет все данные SpecWeekKPI."""
        await KPIDBManager.delete_all_kpi_data(session, SpecWeekKPI)

    @staticmethod
    async def delete_old_month_data(session: AsyncSession) -> None:
        """Удаляет все данные SpecMonthKPI."""
        await KPIDBManager.delete_all_kpi_data(session, SpecMonthKPI)


class KPIProcessor(APIProcessor[DBModel, KPIProcessingConfig]):
    """Процессор для обработки данных KPI."""

    def __init__(self, api: KpiAPI):
        super().__init__(api)
        self.fetcher = ConcurrentAPIFetcher()

    async def fetch_data(
        self, config: KPIProcessingConfig, **kwargs
    ) -> list[tuple[Any, ...]]:
        """Получает данные KPI из API для всех подразделений и типов отчетов."""
        tasks = []
        for division in config.divisions:
            for report_type in config.report_types:
                tasks.append((division, report_type))

        async def fetch_kpi_for_division_and_report(division, report_type):
            try:
                if config.use_custom_dates and config.start_date_func:
                    # Используем кастомные даты
                    start_date = config.start_date_func()
                    result = await self.api.get_custom_period_kpi(
                        division=division,
                        report=report_type,
                        start_date=start_date,
                        use_week_period=config.use_week_period
                    )
                else:
                    # Используем старую логику с количеством дней
                    result = await self.api.get_period_kpi(
                        division=division, report=report_type, days=config.period_days
                    )
                return division, report_type, result
            except Exception as e:
                self.logger.error(
                    f"Ошибка получения KPI для {division}, отчет {report_type}: {e}"
                )
                return division, report_type, None

        return await self.fetcher.fetch_parallel(
            tasks, fetch_kpi_for_division_and_report
        )

    def process_results(
        self, results: list[tuple[Any, ...]], config: KPIProcessingConfig, **kwargs
    ) -> list[DBModel]:
        """Обрабатывает результаты API и агрегирует данные по сотрудникам."""
        self.logger.debug(
            f"Получено {len(results)} результатов из API для {config.table_name}"
        )

        # Подсчитываем результаты с данными
        results_with_data = [
            r
            for r in results
            if not isinstance(r, Exception)
            and len(r) > 1
            and r[1][2]
            and hasattr(r[1][2], "data")
            and r[1][2].data
        ]
        self.logger.debug(
            f"Результатов с данными: {len(results_with_data)} из {len(results)}"
        )

        # Определяем extraction_period в зависимости от типа конфигурации
        extraction_period = None
        if config.extraction_period_func:
            # Используем специальную функцию для extraction_period
            extraction_period = config.extraction_period_func()
        elif config.use_custom_dates and config.start_date_func:
            extraction_period = config.start_date_func()
        else:
            extraction_period = get_current_month_first_day()

        # Агрегируем данные по сотрудникам
        aggregator = KPIDataAggregator(config.db_model_class)
        return aggregator.aggregate_results(results, extraction_period)

    async def save_data(
        self, data: list[DBModel], config: KPIProcessingConfig, **kwargs
    ) -> int:
        """Сохраняет данные KPI в БД."""
        if not data:
            self.logger.warning(f"Нет данных в {config.table_name} для сохранения")
            return 0

        try:
            async with get_stats_session() as session:
                db_operator = BatchDBOperator(session)

                # Удаляем все старые данные
                await config.delete_func(session)

                # Вставляем новые данные
                return await db_operator.bulk_insert_with_cleanup(
                    data,
                    None,
                    config.table_name,  # delete_func=None, так как уже очистили выше
                )

        except Exception as e:
            self.logger.error(
                f"Ошибка БД при вставке данных {config.table_name}: {type(e).__name__}: {e}"
            )
            self.logger.error(
                f"Не удалось вставить {len(data)} записей {config.table_name} из-за ошибки БД"
            )
            raise


# Конфигурации для разных периодов KPI
DAY_KPI_CONFIG = KPIProcessingConfig(
    update_type="Дневные KPI",
    divisions=list(KpiAPI.unites.keys()),  # ["НТП1", "НТП2", "НЦК"]
    report_types=[
        "AHT",
        "FLR",
        "CSI",
        "POK",
        "DELAY",
        "Sales",
        "SalesPotential",
        "PaidService",
    ],
    db_model_class=SpecDayKPI,
    period_days=1,  # Сохраняем старую логику для дневных
    table_name="KpiDay",
    delete_func=KPIDBManager.delete_old_day_data,
    extraction_period_func=get_yesterday_date,  # Используем вчерашний день
    use_custom_dates=False,
)

WEEK_KPI_CONFIG = KPIProcessingConfig(
    update_type="Недельные KPI",
    divisions=list(KpiAPI.unites.keys()),  # ["НТП1", "НТП2", "НЦК"]
    report_types=[
        "AHT",
        "FLR",
        "CSI",
        "POK",
        "DELAY",
        "Sales",
        "SalesPotential",
        "PaidService",
    ],
    db_model_class=SpecWeekKPI,
    period_days=None,  # Используем кастомные даты
    table_name="KpiWeek",
    delete_func=KPIDBManager.delete_old_week_data,
    start_date_func=get_week_start_date,
    use_custom_dates=True,
    use_week_period=True,  # Используем недельный период
)

MONTH_KPI_CONFIG = KPIProcessingConfig(
    update_type="Месячные KPI",
    divisions=list(KpiAPI.unites.keys()),  # ["НТП1", "НТП2", "НЦК"]
    report_types=[
        "AHT",
        "FLR",
        "CSI",
        "POK",
        "DELAY",
        "Sales",
        "SalesPotential",
        "PaidService",
    ],
    db_model_class=SpecMonthKPI,
    period_days=None,  # Используем кастомные даты
    table_name="KpiMonth",
    delete_func=KPIDBManager.delete_old_month_data,
    start_date_func=get_month_period_for_kpi,
    use_custom_dates=True,
)


# Публичные функции API
@log_processing_time("заполнение дневных KPI")
async def fill_day_kpi(api: KpiAPI) -> None:
    """Заполняет дневную таблицу KPI."""
    processor = KPIProcessor(api)
    await processor.process_with_config(DAY_KPI_CONFIG)


@log_processing_time("заполнение недельных KPI")
async def fill_week_kpi(api: KpiAPI) -> None:
    """Заполняет недельную таблицу KPI."""
    processor = KPIProcessor(api)
    await processor.process_with_config(WEEK_KPI_CONFIG)


@log_processing_time("заполнение месячных KPI")
async def fill_month_kpi(api: KpiAPI) -> None:
    """Заполняет месячную таблицу KPI."""
    processor = KPIProcessor(api)
    await processor.process_with_config(MONTH_KPI_CONFIG)


@log_processing_time("заполнение всех KPI")
async def fill_kpi(api: KpiAPI) -> None:
    """
    Основная функция для вызова в планировщике.

    Запускает сбор данных для всех периодов параллельно.
    """
    logger.info("Получаем показатели KPI")

    # Запускаем сбор данных для всех периодов параллельно
    try:
        await asyncio.gather(
            fill_day_kpi(api),
            fill_week_kpi(api),
            fill_month_kpi(api),
            return_exceptions=True,
        )
    except Exception as e:
        logger.error(f"Ошибка получения KPI: {e}")
        raise
