"""Сервис логирования."""

import logging

import betterlogging as bl


def setup_logging() -> None:
    """Настраивает логирование в проекте."""
    log_level = logging.INFO
    bl.basic_colorized_config(level=log_level)

    logging.basicConfig(
        level=log_level,
        format="%(filename)s:%(lineno)d #%(levelname)-8s [%(asctime)s] - %(name)s - %(message)s",
    )
