from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class Settings(BaseSettings):
    # The model_config tells Pydantic where to load .env from
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # -------------------------
    # Environment Variables
    # -------------------------
    SEC_API_KEY: str = Field(..., env="SEC_API_KEY")
    BASE_URL: str = Field("https://api.sec-api.com", env="BASE_URL")

    # Database
    DB_HOST: str = Field("localhost", env="DB_HOST")
    DB_PORT: int = Field(5432, env="DB_PORT")
    DB_USER: str = Field(..., env="DB_USER")
    DB_PASSWORD: str = Field(..., env="DB_PASSWORD")
    DB_NAME: str = Field(..., env="DB_NAME")

    # ETL settings
    RATE_LIMIT: int = Field(30, env="RATE_LIMIT")
    RATE_PERIOD: int = Field(60, env="RATE_PERIOD")


# Singleton config instance
settings = Settings()

