import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from time import perf_counter
from typing import Any

from sqlalchemy import Delete
from sqlalchemy.ext.asyncio import AsyncSession
from stp_database.models.KPI import SpecDayKPI, SpecMonthKPI, SpecWeekKPI

from app.api.kpi import KpiAPI
from app.core.db import get_stats_session
from app.services.helpers import get_current_month_first_day

logger = logging.getLogger(__name__)

DBModel = SpecDayKPI | SpecMonthKPI | SpecWeekKPI


@dataclass
class KPIConfig:
    """Configuration for KPI data processing."""

    divisions: list[str]
    report_types: list[str]
    db_model_class: type[DBModel]
    period_days: int
    table_name: str
    delete_func: Callable[[AsyncSession], Any]


async def _delete_old_kpi_data(
    session: AsyncSession, model_class: type[DBModel]
) -> None:
    """Универсальная функция удаления всех данных KPI."""
    query = Delete(model_class)
    await session.execute(query)


async def delete_old_day_data(session: AsyncSession) -> None:
    """Удаляет все данные SpecDayKPI."""
    await _delete_old_kpi_data(session, SpecDayKPI)


async def delete_old_week_data(session: AsyncSession) -> None:
    """Удаляет все данные SpecWeekKPI."""
    await _delete_old_kpi_data(session, SpecWeekKPI)


async def delete_old_month_data(session: AsyncSession) -> None:
    """Удаляет все данные SpecMonthKPI."""
    await _delete_old_kpi_data(session, SpecMonthKPI)


async def _bulk_insert_kpi_data(
    kpi_list: list[DBModel],
    model_name: str,
    delete_func: Callable[[AsyncSession], Any],
) -> None:
    """Универсальная функция для массовой вставки данных KPI.

    Args:
        kpi_list: Список объектов модели KPI
        model_name: Название модели для логирования
        delete_func: Функция для удаления старых данных
    """
    if not kpi_list:
        logger.warning(f"[KPI] Нет данных в {model_name} для сохранения")
        return

    try:
        async with get_stats_session() as session:
            # Удаляем все старые данные
            await delete_func(session)

            # Вставляем записи одной операцией
            session.add_all(kpi_list)

            await session.commit()

            logger.info(
                f"[KPI] Успешно сохранено {len(kpi_list)} записей в {model_name}"
            )

    except Exception as e:
        logger.error(
            f"Database error while inserting {model_name} KPI data: {type(e).__name__}: {e}"
        )
        logger.error(
            f"Failed to insert {len(kpi_list)} {model_name} records due to database error"
        )
        raise


async def _fetch_kpi_data_for_divisions(
    api: KpiAPI, divisions: list[str], report_types: list[str], days: int
) -> list[tuple[str, str, Any]]:
    """Получает данные KPI для списка подразделений и типов отчетов параллельно.

    Args:
        api: Экземпляр API
        divisions: Список подразделений
        report_types: Список типов отчетов
        days: Количество дней для периода

    Returns:
        Список кортежей (division, report_type, result) с результатами API
    """
    # Создаем параллельные задачи для всех подразделений и типов отчетов
    tasks = []
    for division in divisions:
        for report_type in report_types:
            coro = api.get_period_kpi(division=division, report=report_type, days=days)
            tasks.append((division, report_type, coro))

    # Выполняем вызовы API одновременно
    results = await asyncio.gather(*(t[2] for t in tasks), return_exceptions=True)

    # Обрабатываем ошибки
    processed_results = []
    for i, result in enumerate(results):
        division, report_type = tasks[i][0], tasks[i][1]
        if isinstance(result, Exception):
            logger.error(
                f"Error getting KPI for division {division}, report {report_type}: {result}"
            )
            processed_results.append((division, report_type, None))
        else:
            processed_results.append((division, report_type, result))

    return processed_results


def _process_api_response_to_models(
    results: list[tuple[str, str, Any]],
    model_class: type[DBModel],
) -> list[DBModel]:
    """Обрабатывает результаты API и конвертирует их в модели БД.

    Args:
        results: Список результатов API
        model_class: Класс модели БД для создания объектов

    Returns:
        Список моделей БД
    """
    # Получаем период извлечения - первое число текущего месяца
    extraction_period = get_current_month_first_day()

    # Словарь для хранения ORM объектов по fullname
    kpi_objects_by_fullname = {}
    total_processed_rows = 0

    try:
        for division, report_type, result in results:
            if not result or not result.data:
                logger.warning(
                    f"[KPI] Не найдено показателей для направления: {division}, отчет: {report_type}"
                )
                continue

            for row in result.data:
                fullname = getattr(row, "fullname", None)
                if not fullname:
                    continue

                # Получаем или создаем объект KPI для данного fullname
                if fullname not in kpi_objects_by_fullname:
                    kpi_obj = model_class()
                    kpi_obj.fullname = fullname
                    kpi_obj.extraction_period = extraction_period
                    kpi_objects_by_fullname[fullname] = kpi_obj
                else:
                    kpi_obj = kpi_objects_by_fullname[fullname]

                # Обновляем показатели в зависимости от типа отчета
                if report_type.lower() == "aht" and row.aht:
                    kpi_obj.contacts_count = row.aht_total_contacts
                    kpi_obj.aht = row.aht
                    kpi_obj.aht_chats_web = row.aht_chats_web
                    kpi_obj.aht_chats_dhcp = row.aht_chats_dhcp
                    kpi_obj.aht_chats_mobile = row.aht_chats_mobile
                    kpi_obj.aht_chats_smartdom = row.aht_chats_smartdom
                elif report_type.lower() == "flr" and row.flr:
                    kpi_obj.flr = row.flr
                    kpi_obj.flr_services = row.flr_services
                    kpi_obj.flr_services_cross = row.flr_services_cross
                    kpi_obj.flr_services_transfer = row.flr_services_transfers
                elif report_type.lower() == "csi" and row.csi:
                    kpi_obj.csi = row.csi
                elif report_type.lower() == "pok" and row.pok:
                    kpi_obj.pok = row.pok
                    kpi_obj.pok_rated_contacts = row.pok_rated_contacts
                elif report_type.lower() == "delay" and row.delay:
                    kpi_obj.delay = row.delay
                elif report_type.lower() == "sales":
                    kpi_obj.sales = row.sales
                    kpi_obj.sales_videos = row.sales_videos
                    kpi_obj.sales_routers = row.sales_routers
                    kpi_obj.sales_tvs = row.sales_tvs
                    kpi_obj.sales_intercoms = row.sales_intercoms
                    kpi_obj.sales_conversion = row.sales_conversion
                elif report_type.lower() == "salespotential":
                    kpi_obj.sales_potential = row.sales_potential
                    kpi_obj.sales_potential_video = row.sales_potential_video
                    kpi_obj.sales_potential_routers = row.sales_potential_routers
                    kpi_obj.sales_potential_tvs = row.sales_potential_tvs
                    kpi_obj.sales_potential_intercoms = row.sales_potential_intercoms
                    kpi_obj.sales_potential_conversion = row.sales_potential_conversion
                elif report_type.lower() == "paidservice":
                    kpi_obj.services = row.services
                    kpi_obj.services_remote = row.services_remote
                    kpi_obj.services_onsite = row.services_onsite
                    kpi_obj.services_conversion = row.services_conversion

                total_processed_rows += 1
    except Exception as e:
        logging.error("Произошла ошибка при сохранении показателей: " + str(e))
    logger.debug(
        f"[KPI] Обработано {total_processed_rows} строк в {len(kpi_objects_by_fullname)}"
    )
    return list(kpi_objects_by_fullname.values())


async def _fill_kpi_for_period(api: KpiAPI, config: KPIConfig) -> None:
    """Универсальная функция заполнения данных KPI за период.

    Args:
        api: Экземпляр API
        config: Конфигурация для обработки KPI
    """
    timer_start = perf_counter()
    model_name = config.db_model_class.__name__
    logger.debug(f"[KPI] Запускаем заполнение {model_name}")

    # Получаем данные из API
    results = await _fetch_kpi_data_for_divisions(
        api, config.divisions, config.report_types, config.period_days
    )

    logger.debug(f"[KPI] Получено {len(results)} результатов из API для {model_name}")

    # Debug: Count results with actual data
    results_with_data = [
        r for r in results if r[2] and hasattr(r[2], "data") and r[2].data
    ]
    logger.debug(
        f"[KPI] Результатов с данными: {len(results_with_data)} из {len(results)}"
    )

    # Конвертируем в модели БД и агрегируем по fullname
    kpi_list = _process_api_response_to_models(results, config.db_model_class)

    if not kpi_list:
        logger.warning(f"Не найдено данных для {model_name}")
        return

    # Записываем данные в БД
    try:
        await _bulk_insert_kpi_data(kpi_list, model_name, config.delete_func)
        timer_stop = perf_counter()
        logger.debug(
            f"[KPI] Успешно заполнили {model_name}, заняло {timer_stop - timer_start:.2f} секунд"
        )
    except Exception as e:
        timer_stop = perf_counter()
        logger.error(
            f"[KPI] Ошибка заполнения {model_name} из-за доступа к БД, заняло {timer_stop - timer_start:.2f} секунд"
        )
        logger.error(f"[KPI] Детали ошибки БД: {type(e).__name__}: {e}")
        return


# Конфигурации для разных периодов KPI
DAY_KPI_CONFIG = KPIConfig(
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
    period_days=1,
    table_name="KpiDay",
    delete_func=delete_old_day_data,
)

WEEK_KPI_CONFIG = KPIConfig(
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
    period_days=7,
    table_name="KpiWeek",
    delete_func=delete_old_week_data,
)

MONTH_KPI_CONFIG = KPIConfig(
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
    period_days=31,
    table_name="KpiMonth",
    delete_func=delete_old_month_data,
)


# Публичные функции API
async def fill_day_kpi(api: KpiAPI) -> None:
    """Заполняет дневную таблицу KPI.

    Args:
        api: Экземпляр API
    """
    await _fill_kpi_for_period(api, DAY_KPI_CONFIG)


async def fill_week_kpi(api: KpiAPI) -> None:
    """Заполняет недельную таблицу KPI.

    Args:
        api: Экземпляр API
    """
    await _fill_kpi_for_period(api, WEEK_KPI_CONFIG)


async def fill_month_kpi(api: KpiAPI) -> None:
    """Заполняет месячную таблицу KPI.

    Args:
        api: Экземпляр API
    """
    await _fill_kpi_for_period(api, MONTH_KPI_CONFIG)


async def fill_kpi(api: KpiAPI) -> None:
    """Основная функция для вызова в планировщике.

    Args:
        api: Экземпляр API KPI
    """
    timer_start = perf_counter()
    logger.info("[KPI] Получаем показатели")

    # Запускаем сбор данных для всех периодов параллельно
    try:
        await asyncio.gather(
            fill_day_kpi(api),
            fill_week_kpi(api),
            fill_month_kpi(api),
            return_exceptions=True,
        )
    except Exception as e:
        logger.error(f"[KPI] Ошибка получения KPI: {e}")
        raise

    timer_stop = perf_counter()
    logger.info(
        f"[KPI] Сбор KPI завершен, это заняло {timer_stop - timer_start:.2f} секунд"
    )
