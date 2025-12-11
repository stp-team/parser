import asyncio
import logging
from datetime import datetime
from time import perf_counter

from sqlalchemy import text
from stp_database.models.KPI import HeadPremium, SpecPremium

from app.api.premium import PremiumAPI
from app.core.db import get_stats_session
from app.services.constants import unites

logger = logging.getLogger(__name__)


async def _bulk_insert_premium(premium_list: list) -> None:
    """Очищает таблицу и вставляет записи премий.

    Args:
        premium_list: Список объектов SpecPremium
    """
    async with get_stats_session() as session:
        # Очищаем таблицу
        await session.execute(text("TRUNCATE TABLE SpecPremium"))

        # Вставляем записи одной операцией
        session.add_all(premium_list)
        await session.commit()

        logger.info(f"Inserted {len(premium_list)} records into SpecPremium table")


async def _bulk_insert_head_premium(premium_list: list) -> None:
    """Очищает таблицу HeadPremium и вставляет записи премий руководителей.

    Args:
        premium_list: Список объектов HeadPremium
    """
    async with get_stats_session() as session:
        # Очищаем таблицу
        await session.execute(text("TRUNCATE TABLE HeadPremium"))

        # Вставляем записи одной операцией
        session.add_all(premium_list)
        await session.commit()

        logger.info(f"Inserted {len(premium_list)} records into HeadPremium table")


async def fill_specialists_premium(api: PremiumAPI, period: str = "01.12.2025"):
    """Получает данные о премиях для всех специалистов по всем подразделениям.

    Args:
        api: Экземпляр API
        period: Период в формате "dd.mm.yyyy"
    """
    timer_start = perf_counter()
    logger.info(f"Starting specialists premium fill for period {period}")

    # Создаем параллельные задачи для всех подразделений
    tasks = []
    for division in unites:
        coro = api.get_specialist_premium(period=period, division=division)
        tasks.append((division, coro))

    # Выполняем вызовы API одновременно
    results = await asyncio.gather(*(t[1] for t in tasks), return_exceptions=True)

    # Обрабатываем ошибки
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"Error getting premium for division {tasks[i][0]}: {result}")
            results[i] = None

    # Агрегируем результаты
    premium_list = []
    for (division, _), result in zip(tasks, results, strict=False):
        if not result or not hasattr(result, "items"):
            logger.warning(f"No premium data returned for division: {division}")
            continue

        for row in result.items:
            dt = datetime.strptime(row.period, "%d.%m.%Y")
            premium_list.append(
                SpecPremium(
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
                    target_normative_rate_second=row.target_normative_second,
                    target_premium=row.target_premium,
                    pers_target_manual=row.pers_target_manual,
                    discipline_premium=row.discipline_premium,
                    tests_premium=row.tests_premium,
                    thanks_premium=row.thanks_premium,
                    tutors_premium=row.tutors_premium,
                    head_adjust_premium=row.head_adjust_premium,
                    total_premium=row.total_premium,
                    extraction_period=dt,
                )
            )

    if not premium_list:
        logger.warning("No premium data to insert")
        return

    # Записываем данные в БД одной операцией
    await _bulk_insert_premium(premium_list)

    timer_stop = perf_counter()
    logger.info(
        f"Finished specialists premium fill, taken {timer_stop - timer_start:.2f} seconds"
    )


async def fill_heads_premium(api: PremiumAPI, period: str = "01.12.2025"):
    """Получает данные о премиях для всех руководителей по всем подразделениям.

    Args:
        api: Экземпляр API
        period: Период в формате "dd.mm.yyyy"
    """
    timer_start = perf_counter()
    logger.info(f"Starting heads premium fill for period {period}")

    # Создаем параллельные задачи для всех подразделений
    tasks = []
    for division in ["НТП", "НЦК"]:  # Подразделения для руководителей
        coro = api.get_head_premium(period=period, division=division)
        tasks.append((division, coro))

    # Выполняем вызовы API одновременно
    results = await asyncio.gather(*(t[1] for t in tasks), return_exceptions=True)

    # Обрабатываем ошибки
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(
                f"Error getting head premium for division {tasks[i][0]}: {result}"
            )
            results[i] = None

    # Агрегируем результаты
    premium_list = []
    for (division, _), result in zip(tasks, results, strict=False):
        if not result or not hasattr(result, "premium"):
            logger.warning(f"No head premium data returned for division: {division}")
            continue

        for row in result.premium:
            dt = datetime.strptime(row.period, "%d.%m.%Y")
            premium_list.append(
                HeadPremium(
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
                    extraction_period=dt,
                )
            )

    if not premium_list:
        logger.warning("No head premium data to insert")
        return

    # Записываем данные в БД одной операцией
    await _bulk_insert_head_premium(premium_list)

    timer_stop = perf_counter()
    logger.info(
        f"Finished heads premium fill, taken {timer_stop - timer_start:.2f} seconds"
    )
