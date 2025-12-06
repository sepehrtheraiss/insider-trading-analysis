import pandas as pd

class MappingTransformer:
    """Normalizes exchange mapping raw JSON into DB-ready rows."""
    KEEP = [
        "issuerTicker",
        "cik",
        "exchange",
        "sector",
        "industry",
        "category",
        "name"
    ]

    DROP_COLS = [
        "cusip", "sic", "famaSector", "famaIndustry", 
        "id", "currency", "location"
    ]

    RENAME_COLS = {
        "ticker": "issuerTicker"
    }
    def normalize(self, raw: list[dict]) -> pd.DataFrame:
        """Turn raw API response into a DataFrame with consistent fields."""
        df = pd.DataFrame(raw)
        return df

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop noise, rename columns, enforce expected formats."""
        if df.empty:
            # avoid upstream breakage.
            # returns a DataFrame with the correct schema, but no rows.
            return pd.DataFrame(columns=self.KEEP)

        # pandas will not overwrite an existing column name on rename
        # so rename before adding rename_col
        df = df.rename(columns=self.RENAME_COLS)
        # Create missing columns if needed
        for col in self.KEEP:
            if col not in df.columns:
                df[col] = None

        # Safely enforce schema
        df = df[self.KEEP]
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
        return df.drop_duplicates(subset=["issuerTicker"], keep="first")

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure required schema & datatypes."""
        return df  # placeholder for future checks

    def transform(self, raw: list[dict]) -> pd.DataFrame:
        """Main entrypoint: full transformation pipeline."""
        df = self.normalize(raw)
        df = self.clean(df)
        df = self.dedupe(df)
        df = self.validate(df)
        return df