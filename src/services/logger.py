"""Сервис логирования."""

import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import betterlogging as bl


def setup_logging() -> None:
    """Настраивает логирование в проекте."""
    log_level = logging.INFO

    # Создаем директорию для логов, если она не существует
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # Формат логов
    log_format = "%(filename)s:%(lineno)d #%(levelname)-8s [%(asctime)s] - %(name)s - %(message)s"
    formatter = logging.Formatter(log_format)

    # Настройка цветного вывода в консоль
    bl.basic_colorized_config(level=log_level)

    # Консольный хендлер
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)

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

    # Настройка корневого логгера
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Очистка существующих хендлеров и добавление новых
    root_logger.handlers.clear()
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
