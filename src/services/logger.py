"""Сервис логирования."""

import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import betterlogging as bl

# Optional dashboard import - only available when rich is installed
try:
    from src.services.cli_dashboard import get_dashboard

    DASHBOARD_AVAILABLE = True
except ImportError:
    DASHBOARD_AVAILABLE = False


def setup_logging(use_dashboard: bool = True) -> None:
    """Настраивает логирование в проекте.

    Args:
        use_dashboard: Whether to use CLI dashboard (requires rich package)
    """
    log_level = logging.INFO

    # Создаем директорию для логов, если она не существует
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # Формат логов
    log_format = "%(filename)s:%(lineno)d #%(levelname)-8s [%(asctime)s] - %(name)s - %(message)s"
    formatter = logging.Formatter(log_format)

    # Настройка корневого логгера
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Очистка существующих хендлеров
    root_logger.handlers.clear()

    # Add dashboard handler if available and requested (logs go to Live display)
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

    # Only add console handler if dashboard is NOT active (avoids conflicts)
    if not dashboard_active:
        import sys
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    # Файловый хендлер с ротацией (каждый день, хранить 2 дня)
    file_handler = TimedRotatingFileHandler(
        filename=log_dir / "app.log",
        when="midnight",  # Ротация в полночь
        interval=1,  # Каждые 1 день
        backupCount=2,  # Хранить 2 бэкапа (2 дня)
        encoding="utf-8",
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    file_handler.suffix = "%Y-%m-%d.log"  # Формат имени ротированного файла
    root_logger.addHandler(file_handler)
