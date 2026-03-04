import json
from typing import Literal

import pytz
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from pytz.tzinfo import DstTzInfo


class Settings(BaseSettings):
    ENVIRONMENT: str

    # Настройки OKC
    OKC_USERNAME: str
    OKC_PASSWORD: str
    OKC_BASE_URL: str

    # Настройки БД
    DB_HOST: str
    DB_PORT: int
    DB_USER: str
    DB_PASSWORD: str

    DB_STP_NAME: str
    DB_STATS_NAME: str
    DB_QUESTIONS_NAME: str

    # Настройки NATS
    NATS_HOST: str = ""
    NATS_PORT: int = 4222
    NATS_TOKEN: str = ""
    NATS_SUBJECT: str = "api_test"

    # Настройки WebSocket
    WS_LINES: list[str] = [
        "nck",
        "ntp1",
        "ntp2",
    ]  # Линии для подключения: nck, ntp1, ntp2

    @field_validator("WS_LINES", mode="before")
    @classmethod
    def parse_ws_lines(cls, v):
        """Parse WS_LINES from JSON string or list."""
        if isinstance(v, str):
            try:
                return json.loads(v)
            except json.JSONDecodeError:
                # If comma-separated string
                return [line.strip() for line in v.split(",")]
        return v

    @field_validator("DOSSIER_BULK_SYNC_HOURS", mode="before")
    @classmethod
    def parse_bulk_sync_hours(cls, v):
        """Parse DOSSIER_BULK_SYNC_HOURS from JSON string, CSV or list."""
        if isinstance(v, str):
            raw_value = v.strip()
            if not raw_value:
                return [8]
            try:
                parsed = json.loads(raw_value)
            except json.JSONDecodeError:
                parsed = [part.strip() for part in raw_value.split(",") if part.strip()]
            v = parsed

        if isinstance(v, int):
            v = [v]

        if not isinstance(v, list):
            raise ValueError("DOSSIER_BULK_SYNC_HOURS must be a list of integers")

        normalized = sorted({int(hour) for hour in v})
        for hour in normalized:
            if hour < 0 or hour > 23:
                raise ValueError("DOSSIER_BULK_SYNC_HOURS values must be in range 0..23")

        return normalized or [8]

    # Настройки планировщика
    SCHEDULER_ENABLE_PERSISTENCE: bool = False
    SCHEDULER_MAX_WORKERS: int = 5
    SCHEDULER_TIMEZONE: DstTzInfo = pytz.timezone("Asia/Yekaterinburg")
    SCHEDULER_JOB_STORE_URL: str = ""

    # Массовые выгрузки Dossier
    # DOSSIER_BULK_SYNC_MODE:
    # - once  -> автоматически [8]
    # - twice -> автоматически [8, 20]
    # - custom -> используется DOSSIER_BULK_SYNC_HOURS
    DOSSIER_BULK_SYNC_MODE: Literal["once", "twice", "custom"] = "once"
    DOSSIER_BULK_SYNC_HOURS: list[int] = [8]
    DOSSIER_BULK_SYNC_ON_STARTUP: bool = False

    # Dashboard settings
    ENABLE_DASHBOARD: bool = True

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
