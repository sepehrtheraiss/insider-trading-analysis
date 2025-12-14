import pandas as pd

class MappingTransformer:
    """Normalizes exchange mapping raw JSON into DB-ready rows."""
    # Final DB schema
    SCHEMA = [
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

    # Rename map from API → DB
    RENAME_COLS = {
        "ticker": "issuer_ticker",
        "isDelisted": "is_delisted",
        "sicSector": "sic_sector",
        "sicIndustry": "sic_industry",
    }

    # Columns API provides but we discard
    DROP_COLS = [
        "cusip", "sic", "famaSector", "famaIndustry", 
        "id", "currency", "location"
    ]

    def normalize(self, raw: list[dict]) -> pd.DataFrame:
        """Turn raw API response into a DataFrame with consistent fields."""
        df = pd.DataFrame(raw)
        return df

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop noise, rename columns, enforce expected formats."""
        if df.empty:
            # avoid upstream breakage.
            # returns a DataFrame with the correct schema, but no rows.
            return pd.DataFrame(columns=self.SCHEMA)

        # pandas will not overwrite an existing column name on rename
        # so rename before adding rename_col
        df = df.rename(columns=self.RENAME_COLS)

        # Create missing columns if needed
        for col in self.SCHEMA:
            if col not in df.columns:
                df[col] = None

        # drop delisted stocks
        df = df[df['is_delisted'] == False]
        # Safely enforce schema
        df = df[self.SCHEMA]
        return df

    def dedupe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicates based on business key."""
        # Combine all exchange datasets → remove companies that appear more than once.
        # NASDAQ wins if duplicate issuerTicker appears in both
        # Pandas normally sorts text alphabetically
        # explicitly converts exchange names to numbers
        df = df.sort_values(
            by=["exchange"], 
            key=lambda col: col.map({"nasdaq": 0, "nyse": 1})
        )
        return df.drop_duplicates(subset=["issuer_ticker"], keep="first")

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure required schema & datatypes."""
        return df  # placeholder for future checks

    def transform(self, raw: list[dict], staging_writer=None) -> pd.DataFrame:
        """
        Main entrypoint: full transformation pipeline.
        Optional: write staging outputs for debugging.
        """

        # Normalize -----------------------------------------------
        df = self.normalize(raw)
        if staging_writer:
            staging_writer.save("exchange_mapping_normalized", df)

        # Clean ----------------------------------------------------
        df = self.clean(df)
        if staging_writer:
            staging_writer.save("exchange_mapping_cleaned", df)

        # Dedupe ---------------------------------------------------
        df = self.dedupe(df)
        if staging_writer:
            staging_writer.save("exchange_mapping_deduped", df)

        # Validate -------------------------------------------------
        df = self.validate(df)
        if staging_writer:
            staging_writer.save("exchange_mapping_validated", df)

        return df        