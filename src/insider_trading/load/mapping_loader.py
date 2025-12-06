import pandas as pd
from utils.logger import Logger
from db.database.models import ExchangeMapping


class ExchangeMappingLoader:
    """
    Loads the fully transformed exchange mapping (dimension table)
    into the database using UPSERT semantics.

    Responsibilities:
      - Ensure DataFrame matches DB schema
      - UPSERT by issuer_ticker (business key)
      - Update ETL staleness metadata via ETLDatabase
    """

    # Final schema must match SQLAlchemy ExchangeMapping model
    FINAL_SCHEMA = [
        "name",
        "issuer_ticker",
        "cik",
        "exchange",
        "is_delisted",
        "category",
        "sector",
        "industry",
        "sic_sector",
        "sic_industry",
    ]

    def __init__(self, db):
        """
        db: ETLDatabase instance (not the read-only repository)
        """
        self.db = db
        self.log = Logger(self.__class__.__name__)

    # -----------------------------------------------------------
    def _validate_schema(self, df: pd.DataFrame):
        """Ensure DataFrame exactly matches DB schema."""
        missing = [col for col in self.FINAL_SCHEMA if col not in df.columns]
        if missing:
            raise ValueError(f"Loader received DataFrame missing columns: {missing}")

    # -----------------------------------------------------------
    def load(self, df: pd.DataFrame):
        """
        Main loader for the exchange_mapping dimension table.
        Performs an ORM-based UPSERT driven by issuer_ticker.
        """

        if df.empty:
            self.log.warning("ExchangeMappingLoader received empty DataFrame — nothing to load.")
            return

        # 1. Validate schema
        self._validate_schema(df)

        # 2. Convert to list-of-dicts
        rows = df.to_dict(orient="records")

        self.log.info(
            f"Loading {len(rows)} rows into 'exchange_mapping' using business-key UPSERT..."
        )

        # 3. UPSERT rows (in ETLDatabase)
        self.db.upsert(
            model=ExchangeMapping,
            rows=rows,
            key="issuer_ticker",
        )

        # 4. Update ETL staleness metadata
        self.db.set_last_updated("exchange_mapping")

        self.log.info("ExchangeMappingLoader complete.")
