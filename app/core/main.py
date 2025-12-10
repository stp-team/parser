import asyncio
import logging
import signal
import sys

from aiohttp import ClientSession

from app.api.employees import EmployeesAPI
from app.api.sl import SlAPI
from app.core.auth import authenticate
from app.core.config import settings
from app.core.scheduler import scheduler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def signal_handler(sig, frame):
    """Handle shutdown signals gracefully"""
    logger.info("Received shutdown signal. Stopping scheduler...")
    scheduler.shutdown()
    sys.exit(0)


async def main():
    try:
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        session = ClientSession(base_url=settings.OKC_BASE_URL)
        await authenticate(
            username=settings.OKC_USERNAME,
            password=settings.OKC_PASSWORD,
            session=session,
        )

        employees_api = EmployeesAPI(session=session)
        employees = await employees_api.get_employee(employee_id=99918)
        print(employees)

        sl_api = SlAPI(session=session)
        sl = await sl_api.get_sl(
            start_date="10.12.2025",
            stop_date="11.12.2025",
            units=[7],
        )
        print(sl)

        # scheduler.add_job(fill_sl, "interval", seconds=3, args=[session])
        # scheduler.start()

        # Keep the program running
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received. Shutting down...")
            scheduler.shutdown()

    except Exception as e:
        logger.error(f"Error in main: {e}")
        if scheduler.running:
            scheduler.shutdown()
        raise


if __name__ == "__main__":
    asyncio.run(main())
