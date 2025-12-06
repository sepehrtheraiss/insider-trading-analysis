from pathlib import Path
from datetime import datetime, UTC
from typing import Dict, Any

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_datetime64tz_dtype


class FinalWriter:
    """
    Strict 'Gold' layer writer.

    Validates DataFrame columns + types, then writes Parquet.

    NEW BEHAVIOR:
      - Extra columns are allowed (ignored)
      - Column order is automatically corrected
    """

    def __init__(
        self,
        directory: str | Path,
        expected_schema: list[str],
        enforce_types: Dict[str, Any] | None = None,
        keep_history: bool = True,
    ):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

        self.expected_schema = expected_schema
        self.enforce_types = enforce_types or {}
        self.keep_history = keep_history

    # ----------------------------------------------------------------------------
    # Public API
    # ----------------------------------------------------------------------------
    def save(self, name: str, df: pd.DataFrame) -> Path:
        """Validate → reorder → write parquet."""
        self._validate_schema(df)
        self._validate_types(df)

        # Reorder columns to canonical schema
        df = df[self.expected_schema]

        # Generate filename
        if self.keep_history:
            ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
            filename = f"{name}_{ts}.parquet"
        else:
            filename = f"{name}.parquet"

        path = self.directory / filename

        df.to_parquet(path, index=False)
        return path

    # ----------------------------------------------------------------------------
    # Validation
    # ----------------------------------------------------------------------------
    def _validate_schema(self, df: pd.DataFrame) -> None:
        """Ensure all required columns exist (extra columns OK)."""
        missing = [c for c in self.expected_schema if c not in df.columns]

        if missing:
            raise ValueError(
                f"[FinalWriter] Schema mismatch.\n"
                f"Expected: {self.expected_schema}\n"
                f"Missing:  {missing}"
            )

        # NOTE: do NOT check for extra columns
        # NOTE: do NOT check column order

    def _validate_types(self, df: pd.DataFrame) -> None:
        """Strict type validation for configured columns."""
        for col, expected in self.enforce_types.items():

            if col not in df.columns:
                raise ValueError(f"[FinalWriter] Cannot enforce type. Missing column '{col}'.")

            series = df[col]

            # ----------------------------------------------------------------------
            # Datetime with timezone enforcement: "datetime64[ns, UTC]"
            # ----------------------------------------------------------------------
            if expected == "datetime64[ns, UTC]":
                if not is_datetime64_any_dtype(series.dtype):
                    raise TypeError(f"[FinalWriter] Column '{col}' must be datetime64.")

                if not is_datetime64tz_dtype(series.dtype):
                    raise TypeError(
                        f"[FinalWriter] Column '{col}' must be timezone-aware datetime."
                    )

                if str(series.dtype.tz) != "UTC":
                    raise TypeError(f"[FinalWriter] Column '{col}' must be UTC timezone.")

                continue

            # ----------------------------------------------------------------------
            # Python scalar types (str, float, bool, etc.)
            # ----------------------------------------------------------------------
            non_null = series.dropna()
            invalid_mask = non_null.map(lambda v: not isinstance(v, expected))

            if invalid_mask.any():
                raise TypeError(
                    f"[FinalWriter] Column '{col}' has invalid values. "
                    f"Expected type {expected}, got mismatches."
                )
