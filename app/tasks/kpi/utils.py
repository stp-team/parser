# Type definitions
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
    get_current_month_first_day,
    get_month_period_for_kpi,
    get_week_start_date,
    get_yesterday_date,
)
from app.tasks.base import (
    APIProcessor,
    BatchDBOperator,
    ConcurrentAPIFetcher,
    safe_get_attr,
    validate_and_extract_data,
    validate_api_result,
)

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
    start_date_func: Callable[[], "datetime"] | None = (
        None  # Функция для получения начальной даты
    )
    extraction_period_func: Callable[[], "datetime"] | None = (
        None  # Функция для получения extraction_period
    )
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

        # Используем новую утилиту для валидации и извлечения данных
        valid_results = validate_and_extract_data(
            results,
            lambda r: validate_api_result(r, "data"),
            "агрегация KPI",
        )

        try:
            for result_tuple, api_result in valid_results:
                _division, report_type = (
                    result_tuple[0],
                    result_tuple[1] if len(result_tuple) > 1 else "unknown",
                )

                for row in api_result.data:
                    fullname = safe_get_attr(row, "fullname")
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
        """
        Обновляет объект KPI данными из строки API в зависимости от типа отчета.
        Оптимизированная версия с использованием safe_get_attr.
        """
        report_type_lower = report_type.lower()

        if report_type_lower == "aht" and safe_get_attr(row, "aht"):
            self._update_aht_metrics(kpi_obj, row)
        elif report_type_lower == "flr" and safe_get_attr(row, "flr"):
            self._update_flr_metrics(kpi_obj, row)
        elif report_type_lower == "csi":
            csi_value = safe_get_attr(row, "csi")
            if csi_value:
                kpi_obj.csi = csi_value
        elif report_type_lower == "pok" and safe_get_attr(row, "pok"):
            self._update_pok_metrics(kpi_obj, row)
        elif report_type_lower == "delay":
            delay_value = safe_get_attr(row, "delay")
            if delay_value:
                kpi_obj.delay = delay_value
        elif report_type_lower == "sales":
            self._update_sales_metrics(kpi_obj, row)
        elif report_type_lower == "salespotential":
            self._update_sales_potential_metrics(kpi_obj, row)
        elif report_type_lower == "paidservice":
            self._update_paid_service_metrics(kpi_obj, row)

    def _update_aht_metrics(self, kpi_obj: DBModel, row: Any) -> None:
        """Обновляет метрики AHT с использованием safe_get_attr."""
        kpi_obj.contacts_count = safe_get_attr(row, "aht_total_contacts")
        kpi_obj.aht = safe_get_attr(row, "aht")
        kpi_obj.aht_chats_web = safe_get_attr(row, "aht_chats_web")
        kpi_obj.aht_chats_dhcp = safe_get_attr(row, "aht_chats_dhcp")
        kpi_obj.aht_chats_mobile = safe_get_attr(row, "aht_chats_mobile")
        kpi_obj.aht_chats_smartdom = safe_get_attr(row, "aht_chats_smartdom")

    def _update_flr_metrics(self, kpi_obj: DBModel, row: Any) -> None:
        """Обновляет метрики FLR с использованием safe_get_attr."""
        kpi_obj.flr = safe_get_attr(row, "flr")
        kpi_obj.flr_services = safe_get_attr(row, "flr_services")
        kpi_obj.flr_services_cross = safe_get_attr(row, "flr_services_cross")
        kpi_obj.flr_services_transfer = safe_get_attr(row, "flr_services_transfers")

    def _update_pok_metrics(self, kpi_obj: DBModel, row: Any) -> None:
        """Обновляет метрики POK с использованием safe_get_attr."""
        kpi_obj.pok = safe_get_attr(row, "pok")
        kpi_obj.pok_rated_contacts = safe_get_attr(row, "pok_rated_contacts")

    def _update_sales_metrics(self, kpi_obj: DBModel, row: Any) -> None:
        """Обновляет метрики продаж."""
        kpi_obj.sales = safe_get_attr(row, "sales")
        kpi_obj.sales_videos = safe_get_attr(row, "sales_videos")
        kpi_obj.sales_routers = safe_get_attr(row, "sales_routers")
        kpi_obj.sales_tvs = safe_get_attr(row, "sales_tvs")
        kpi_obj.sales_intercoms = safe_get_attr(row, "sales_intercoms")
        kpi_obj.sales_conversion = safe_get_attr(row, "sales_conversion")

    def _update_sales_potential_metrics(self, kpi_obj: DBModel, row: Any) -> None:
        """Обновляет метрики потенциала продаж."""
        kpi_obj.sales_potential = safe_get_attr(row, "sales_potential")
        kpi_obj.sales_potential_video = safe_get_attr(row, "sales_potential_video")
        kpi_obj.sales_potential_routers = safe_get_attr(row, "sales_potential_routers")
        kpi_obj.sales_potential_tvs = safe_get_attr(row, "sales_potential_tvs")
        kpi_obj.sales_potential_intercoms = safe_get_attr(
            row, "sales_potential_intercoms"
        )
        kpi_obj.sales_potential_conversion = safe_get_attr(
            row, "sales_potential_conversion"
        )

    def _update_paid_service_metrics(self, kpi_obj: DBModel, row: Any) -> None:
        """Обновляет метрики платных услуг."""
        kpi_obj.services = safe_get_attr(row, "services")
        kpi_obj.services_remote = safe_get_attr(row, "services_remote")
        kpi_obj.services_onsite = safe_get_attr(row, "services_onsite")
        kpi_obj.services_conversion = safe_get_attr(row, "services_conversion")


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
                        use_week_period=config.use_week_period,
                    )
                else:
                    # Используем старую логику с количеством дней
                    result = await self.api.get_period_kpi(
                        division=division, report=report_type, days=config.period_days
                    )
                return result
            except Exception as e:
                self.logger.error(
                    f"Ошибка получения KPI для {division}, отчет {report_type}: {e}"
                )
                return None

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
            and r[1]
            and hasattr(r[1], "data")
            and r[1].data
        ]
        self.logger.debug(
            f"Результатов с данными: {len(results_with_data)} из {len(results)}"
        )

        # Определяем extraction_period в зависимости от типа конфигурации
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


def create_kpi_config(period_type: str) -> KPIProcessingConfig:
    """
    Фабрика конфигураций KPI для устранения дублирования.

    Args:
        period_type: Тип периода ("day", "week", "month")

    Returns:
        Настроенная конфигурация KPI

    Examples:
        >>> day_config = create_kpi_config("day")
        >>> week_config = create_kpi_config("week")
    """
    # Общие параметры для всех типов KPI
    base_config = {
        "divisions": list(KpiAPI.unites.keys()),  # ["НТП1", "НТП2", "НЦК"]
        "report_types": [
            "AHT",
            "FLR",
            "CSI",
            "POK",
            "DELAY",
            "Sales",
            "SalesPotential",
            "PaidService",
        ],
        "semaphore_limit": 10,
    }

    # Специфичные параметры для каждого типа периода
    config_map = {
        "day": {
            "update_type": "Дневные KPI",
            "db_model_class": SpecDayKPI,
            "period_days": 1,
            "table_name": "KpiDay",
            "delete_func": KPIDBManager.delete_old_day_data,
            "extraction_period_func": get_yesterday_date,
            "use_custom_dates": False,
        },
        "week": {
            "update_type": "Недельные KPI",
            "db_model_class": SpecWeekKPI,
            "period_days": None,
            "table_name": "KpiWeek",
            "delete_func": KPIDBManager.delete_old_week_data,
            "start_date_func": get_week_start_date,
            "use_custom_dates": True,
            "use_week_period": True,
        },
        "month": {
            "update_type": "Месячные KPI",
            "db_model_class": SpecMonthKPI,
            "period_days": None,
            "table_name": "KpiMonth",
            "delete_func": KPIDBManager.delete_old_month_data,
            "start_date_func": get_month_period_for_kpi,
            "use_custom_dates": True,
        },
    }

    if period_type not in config_map:
        raise ValueError(f"Неподдерживаемый тип периода: {period_type}")

    # Объединяем базовые и специфичные параметры
    config_params = {**base_config, **config_map[period_type]}
    return KPIProcessingConfig(**config_params)
