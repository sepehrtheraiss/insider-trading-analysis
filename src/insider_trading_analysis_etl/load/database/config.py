import os
from functools import lru_cache

DEFAULT_DB_URL = "postgresql+psycopg2://insider:insider@localhost:5432/insider_trading"


class Settings:
  """
  Simple config holder for DB settings.
  """
  def __init__(self) -> None:
      self.database_url: str = os.getenv("DATABASE_URL", DEFAULT_DB_URL)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
  return Settings()

