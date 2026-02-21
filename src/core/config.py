import json

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

    # Настройки планировщика
    SCHEDULER_ENABLE_PERSISTENCE: bool = False
    SCHEDULER_MAX_WORKERS: int = 5
    SCHEDULER_TIMEZONE: DstTzInfo = pytz.timezone("Asia/Yekaterinburg")
    SCHEDULER_JOB_STORE_URL: str = ""

    # Dashboard settings
    ENABLE_DASHBOARD: bool = True

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
