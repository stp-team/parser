from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    OKC_USERNAME: str
    OKC_PASSWORD: str
    OKC_BASE_URL: str

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
