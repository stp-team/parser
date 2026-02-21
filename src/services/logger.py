import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

try:
    from src.services.cli_dashboard import get_dashboard

    DASHBOARD_AVAILABLE = True
except ImportError:
    DASHBOARD_AVAILABLE = False


def setup_logging(use_dashboard: bool = True) -> None:
    log_level = logging.INFO

    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    log_format = "%(filename)s:%(lineno)d #%(levelname)-8s [%(asctime)s] - %(name)s - %(message)s"
    formatter = logging.Formatter(log_format)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers.clear()

    dashboard_active = False
    if use_dashboard and DASHBOARD_AVAILABLE:
        try:
            dashboard = get_dashboard()
            dashboard_handler = dashboard.get_log_handler()
            dashboard_handler.setLevel(log_level)
            root_logger.addHandler(dashboard_handler)
            dashboard_active = True
        except Exception as e:
            logging.warning(f"Failed to setup dashboard handler: {e}")

    if not dashboard_active:
        import sys

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    file_handler = TimedRotatingFileHandler(
        filename=log_dir / "app.log",
        when="midnight",
        interval=1,
        backupCount=2,
        encoding="utf-8",
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    file_handler.suffix = "%Y-%m-%d.log"
    root_logger.addHandler(file_handler)
