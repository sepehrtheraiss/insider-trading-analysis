from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
# from pathlib import Path
# ROOT = Path(__file__).resolve().parents[2]
class Settings(BaseSettings):
    # The model_config tells Pydantic where to load .env from
    model_config = SettingsConfigDict(
        env_file=".env",
        #env_file=str(ROOT / ".env"),
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # -------------------------
    # Environment Variables
    # -------------------------
    sec_api_key: str = Field("", env="SEC_API_KEY")
    base_url: str = Field("https://api.sec-api.io", env="BASE_URL")
    openai_api_key: str = Field("", env="OPENAI_API_KEY")

    # Database
    db_host: str = Field("localhost", env="DB_HOST")
    db_port: int = Field(5432, env="DB_PORT")
    db_user: str = Field("insider", env="DB_USER")
    db_password: str = Field("insider", env="DB_PASSWORD")
    db_name: str = Field("insider_trading_etl", env="DB_NAME")

    # ETL settings
    rate_limit: int = Field(30, env="RATE_LIMIT")
    rate_period: int = Field(60, env="RATE_PERIOD")

    # Test / Override settings
    test_mode_tx: bool = Field(False, env="TEST_MODE_TX")
    test_mode_map: bool = Field(False, env="TEST_MODE_MAP")

    test_path_tx: str | None = Field(None, env="TEST_PATH_TX")
    test_path_map: str | None = Field(None, env="TEST_PATH_MAP")


# Singleton config instance
settings = Settings()

