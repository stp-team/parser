
import asyncio
import logging
import sys
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from okc_py import OKC
from okc_py.config import Settings

from src.core.config import settings
from src.tasks.sales_rg import fill_rg_sales


logger = logging.getLogger("sales_rg_worker")

TZ = settings.SCHEDULER_TIMEZONE
RETRY_MINUTES = 5

sync_lock = asyncio.Lock()
last_success_date = None


async def sync_once() -> int:
    """Одна полная выгрузка продаж РГ."""

    client = OKC(
        username=settings.OKC_USERNAME,
        password=settings.OKC_PASSWORD,
        settings=Settings(
            BASE_URL=settings.OKC_BASE_URL
        ),
    )

    try:
        await client.connect()

        count = await fill_rg_sales(
            client.api.ure
        )

        logger.info(
            "Продажи обновлены, строк: %s",
            count,
        )

        return count

    finally:
        await client.close()


async def run_due():
    """
    После 10:00 выполняем выгрузку,
    если сегодня успешной выгрузки ещё не было.
    """

    global last_success_date

    now = datetime.now(TZ)

    if now.hour < 10:
        return

    if last_success_date == now.date():
        return

    async with sync_lock:
        now = datetime.now(TZ)

        if last_success_date == now.date():
            return

        logger.info(
            "Начинаем выгрузку продаж РГ"
        )

        try:
            count = await sync_once()

            if count <= 0:
                logger.warning(
                    "Выгрузка не дала ни одной строки. "
                    "Повтор через %s минут",
                    RETRY_MINUTES,
                )
                return

            last_success_date = now.date()

            logger.info(
                "Продажи РГ успешно обновлены "
                "за %s",
                last_success_date,
            )

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception(
                "Ошибка выгрузки продаж РГ. "
                "Повтор через %s минут",
                RETRY_MINUTES,
            )


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | %(levelname)s | "
            "%(name)s | %(message)s"
        ),
    )

    # Однократный запуск для ручного теста.
    if "--once" in sys.argv:
        await sync_once()
        return

    scheduler = AsyncIOScheduler(
        timezone=TZ
    )

    # Основная выгрузка в 10:00.
    scheduler.add_job(
        run_due,
        trigger="cron",
        hour=10,
        minute=0,
        id="sales_rg_daily",
        max_instances=1,
        coalesce=True,
    )

    # Повторяем проверку при ошибке OKC
    # или отсутствии успешной выгрузки.
    scheduler.add_job(
        run_due,
        trigger="interval",
        minutes=RETRY_MINUTES,
        id="sales_rg_retry",
        max_instances=1,
        coalesce=True,
    )

    scheduler.start()

    logger.info(
        "Воркер продаж запущен. "
        "Расписание: ежедневно 10:00, "
        "проверка каждые %s минут",
        RETRY_MINUTES,
    )

    # Если воркер запущен после 10:00,
    # сразу догоняем сегодняшнюю выгрузку.
    try:
        await run_due()

        # Держим воркер запущенным.
        await asyncio.Event().wait()

    finally:
        scheduler.shutdown(wait=False)


if __name__ == "__main__":
    asyncio.run(main())
