from pathlib import Path
from datetime import datetime, UTC
import pandas as pd


class FinalWriter:
    """
    Writes final ETL DataFrames to Parquet WITH STRICT SCHEMA VALIDATION.
    This represents the Gold layer: validated, deduped, analytics-ready data.
    """

    def __init__(
        self,
        directory="data/final",
        expected_schema: list[str] = None,
        enforce_types: dict[str, type] = None,
        keep_history: bool = True,
    ):
        """
        expected_schema: required column names in order.
        enforce_types: optional dict of COLUMN -> Python type (e.g. str, int).
        keep_history: if False, overwrite latest file instead of timestamping.
        """
        self.dir = Path(directory)
        self.expected_schema = expected_schema or []
        self.enforce_types = enforce_types or {}
        self.keep_history = keep_history

    # --------------------------------------------------------------
    # Schema Validation
    # --------------------------------------------------------------
    def _validate_schema(self, df: pd.DataFrame):
        """Ensures df columns EXACTLY match expected schema (order + names)."""
        df_cols = list(df.columns)

        if df_cols != self.expected_schema:
            raise ValueError(
                f"[FinalWriter] Schema mismatch.\n"
                f"Expected: {self.expected_schema}\n"
                f"Got     : {df_cols}"
            )

    # --------------------------------------------------------------
    # Optional Type Validation
    # --------------------------------------------------------------
    def _validate_types(self, df: pd.DataFrame):
        """Check column element types (lightweight, optional)."""
        for col, expected_type in self.enforce_types.items():
            if col not in df:
                raise ValueError(f"[FinalWriter] Missing column for type check: {col}")

            # Allow null values but type-check non-null ones
            bad = df[col].dropna().map(lambda x: not isinstance(x, expected_type))

            if bad.any():
                raise TypeError(
                    f"[FinalWriter] Column '{col}' contains wrong data types.\n"
                    f"Expected: {expected_type.__name__}\n"
                    f"Invalid rows: {bad.sum()}"
                )

    # --------------------------------------------------------------
    # Save Function
    # --------------------------------------------------------------
    def save(self, name: str, df: pd.DataFrame):
        if not isinstance(df, pd.DataFrame):
            raise TypeError("FinalWriter only accepts pandas DataFrames.")

        self.dir.mkdir(parents=True, exist_ok=True)

        # 1. Strict schema validation
        if self.expected_schema:
            self._validate_schema(df)

        # 2. Optional type validation
        if self.enforce_types:
            self._validate_types(df)

        # 3. Determine filepath
        if self.keep_history:
            timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
            path = self.dir / f"{name}_{timestamp}.parquet"
        else:
            path = self.dir / f"{name}.parquet"

        # 4. Write final Parquet file
        df.to_parquet(path, index=False)

        return path

