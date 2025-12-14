from pathlib import Path
from datetime import datetime, UTC
import pandas as pd


class StagingWriter:
    """
    Writes cleaned intermediate DataFrames (Silver) to timestamped Parquet files.
    No schema enforcement — intentionally looser than FinalWriter.
    """

    def __init__(self, directory: str | Path = "data/staging"):
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)

    def save(self, name: str, df: pd.DataFrame) -> Path:
        if not isinstance(df, pd.DataFrame):
            raise TypeError("StagingWriter only accepts pandas DataFrames.")

        # Defensive copy — avoid mutating upstream DataFrames
        df = df.copy()

        # Optionally harmonize object columns to string
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].astype("string")

        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        filename = f"{name}_{timestamp}.parquet"

        path = self.dir / filename
        df.to_parquet(path, index=False)

        return path
