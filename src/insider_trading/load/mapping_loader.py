import pandas as pd
from utils.logger import Logger


class ExchangeMappingLoader:
    """
    Loads validated exchange mapping data (Gold layer) into the database.

    Responsibilities:
    - Validate final schema
    - UPSERT into DB (idempotent)
    - Update last_updated timestamp for pipeline staleness checks
    """

    TABLE = "exchange_mapping"

    EXPECTED_COLUMNS = [
        "issuerTicker",
        "cik",
        "exchange",
        "sector",
        "industry",
        "category",
        "name",
    ]

    def __init__(self, db):
        self.db = db
        self.log = Logger(self.__class__.__name__)

    # -----------------------------------------------------------
    # Schema validation
    # -----------------------------------------------------------
    def _validate(self, df: pd.DataFrame):
        cols = list(df.columns)

        if cols != self.EXPECTED_COLUMNS:
            raise ValueError(
                f"ExchangeMappingLoader expected schema {self.EXPECTED_COLUMNS}, "
                f"but got {cols}"
            )

    # -----------------------------------------------------------
    # Main load function
    # -----------------------------------------------------------
    def load(self, df: pd.DataFrame):
        """
        Perform bulk UPSERT of exchange mapping metadata.
        Pipeline gives us a fully validated, deduped DataFrame.
        """

        # 1. Schema validation
        self._validate(df)

        if df.empty:
            self.log.warning("No mapping rows to load (df empty).")
            return

        # 2. Convert DataFrame → dict rows
        records = df.to_dict(orient="records")

        self.log.info(f"Loading {len(records)} mapping rows into DB...")

        # 3. UPSERT
        # NOTE: We assume DB provides a repository with an upsert method.
        # Example: db.upsert(table, rows, unique_key)
        self.db.upsert(
            table=self.TABLE,
            rows=records,
            key="issuerTicker",  # unique business key
        )

        # 4. Update last_updated for pipeline freshness checks
        self.db.set_last_updated(self.TABLE)

        self.log.info("Exchange mapping load complete.")
