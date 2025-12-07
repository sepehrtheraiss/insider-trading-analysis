import pandas as pd
from .normalize_transactions import normalize_transactions

class InsiderTransactionsTransformer:
    """
    Normalize → clean → dedupe → validate insider transaction data.
    Produces a DB-ready DataFrame that matches the InsiderTransaction model.
    """

    # DB schema — MUST match SQLAlchemy InsiderTransaction fields
    SCHEMA = [
        "filed_at",
        "period_of_report",
        "document_type",
        "issuer_ticker",
        "issuer_cik",
        "issuer_name",
        "reporter",
        "reporter_cik",
        "is_officer",
        "officer_title",
        "is_director",
        "is_ten_percent_owner",
        "table",
        "code",
        "acquired_disposed",
        "transaction_date",
        "shares",
        "price_per_share",
        "total_value",
        "shares_owned_following",
        "is_10b5_1",
    ]

    # Rename map from API → DB snake_case
    RENAME_COLS = {
        "issuerTicker": "issuer_ticker",
        "issuerCik": "issuer_cik",
        "issuerName": "issuer_name",
        "reporterName": "reporter",
        "reporterCik": "reporter_cik",
        "isOfficer": "is_officer",
        "isDirector": "is_director",
        "isTenPercentOwner": "is_ten_percent_owner",
        "officerTitle": "officer_title",
        "transactionDate": "transaction_date",
        "periodOfReport": "period_of_report",
        "filedAt": "filed_at",
        "pricePerShare": "price_per_share",
        "sharesOwnedFollowing": "shares_owned_following",
        "is10b5_1": "is_10b5_1",
    }

    # -----------------------------------------------------------
    def normalize(self, raw: list[dict]) -> pd.DataFrame:
        """Convert raw JSON into a DataFrame."""
        return normalize_transactions(raw)

    # -----------------------------------------------------------
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rename columns, enforce schema, coerce timestamps & numeric values."""

        # if df.empty:
        #     return pd.DataFrame(columns=self.SCHEMA)

        # # Rename API → DB fields
        # df = df.rename(columns=self.RENAME_COLS)

        # # Add missing columns
        # for col in self.SCHEMA:
        #     if col not in df.columns:
        #         df[col] = None

        # # Convert timestamps
        # for col in ["filed_at", "period_of_report", "transaction_date"]:
        #     df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

        # # Convert numerics
        # numeric_cols = [
        #     "shares",
        #     "price_per_share",
        #     "total_value",
        #     "shares_owned_following",
        # ]
        # for col in numeric_cols:
        #     df[col] = pd.to_numeric(df[col], errors="coerce")

        # # Restrict schema ordering
        # df = df[self.SCHEMA]

        # return df
        if df.empty:
            return df

        # Convert to datetime
        df["filed_at"] = pd.to_datetime(df["filed_at"], errors="coerce", utc=True)
        df["period_of_report"] = pd.to_datetime(df["period_of_report"], errors="coerce", utc=True)

        # Drop invalid
        df = df[df["period_of_report"].notna()]
        df = df[df["filed_at"].notna()]

        # Valid transaction business rules
        ignore_ciks = [810893,1454510,1463208,1877939,1556801,827187]

        filter_all = (
            (df["shares"] != df["price_per_share"]) &
            ((df["price_per_share"] < 6000) | (df["shares"] == 1)) &
            (df["total_value"] > 0) &
            (df["code"] != "M") &
            (~df["issuer_ticker"].isin(["NONE","N/A","NA"])) &
            (~df["issuer_cik"].astype("Int64").isin(ignore_ciks))
        )

        return df[filter_all]
    # -----------------------------------------------------------
    def dedupe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate insider filings.
        Business key is usually:
        (issuer_ticker, reporter, transaction_date, code)
        """
        return df.drop_duplicates(
            subset=["issuer_ticker", "reporter", "transaction_date", "code"],
            keep="first",
        )

    # -----------------------------------------------------------
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Placeholder for stricter validation rules."""
        return df

    # -----------------------------------------------------------
    def transform(self, raw: list[dict], staging_writer=None) -> pd.DataFrame:
        """Full ETL transform step with optional staging outputs."""

        df = self.normalize(raw)
        breakpoint()
        if staging_writer:
            staging_writer.save("insider_normalized", df)

        df = self.clean(df)
        if staging_writer:
            staging_writer.save("insider_cleaned", df)

        df = self.dedupe(df)
        if staging_writer:
            staging_writer.save("insider_deduped", df)

        df = self.validate(df)
        if staging_writer:
            staging_writer.save("insider_validated", df)

        return df
