from pathlib import Path
from datetime import datetime, UTC
import pandas as pd


class StagingWriter:
    """Writes cleaned intermediate DataFrames to Parquet files."""

    def __init__(self, directory="data/staging"):
        self.dir = Path(directory)

    def save(self, name: str, df: pd.DataFrame):
        if not isinstance(df, pd.DataFrame):
            raise TypeError("StagingWriter only accepts pandas DataFrames.")

        self.dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        path = self.dir / f"{name}_{timestamp}.parquet"

        df.to_parquet(path, index=False)

        return path

