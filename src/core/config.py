import pytz
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

    # Настройки NATS
    NATS_HOST: str = ""
    NATS_PORT: int = 4222
    NATS_TOKEN: str = ""
    NATS_SUBJECT: str = "api_test"

    # Настройки планировщика
    SCHEDULER_ENABLE_PERSISTENCE: bool = False
    SCHEDULER_MAX_WORKERS: int = 5
    SCHEDULER_TIMEZONE: DstTzInfo = pytz.timezone("Asia/Yekaterinburg")
    SCHEDULER_JOB_STORE_URL: str = ""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
