import argparse
from pathlib import Path
import pandas as pd
from sqlalchemy.orm import Session
import glob

from db.db import engine
from db.models import OHLC


def load_ohlc_csv(csv_path: Path) -> None:
    print(f"Loading OHLC prices from {csv_path}")

    df = pd.read_csv(csv_path)

    # ------------------------------------------------------------
    # 1. Column rename (Yahoo/AlphaVantage → snake_case)
    # ------------------------------------------------------------
    rename_map = {
        "Date": "date",
        "date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "AdjClose": "adj_close",
        "Volume": "volume",
        "Ticker": "ticker",
        "symbol": "ticker",
    }

    df = df.rename(columns=rename_map)

    # Must have these columns:
    required = ["ticker", "date", "open", "high", "low", "close", "volume"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Required column missing in OHLC CSV: {col}")

    # ------------------------------------------------------------
    # 2. Type conversions
    # ------------------------------------------------------------
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)

    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows with no date or no price
    df = df.dropna(subset=["date", "close"])

    # ------------------------------------------------------------
    # 3. Build ORM records
    # ------------------------------------------------------------
    records = []
    for _, row in df.iterrows():
        rec = OHLC(
            ticker=row["ticker"],
            date=row["date"],
            open=row["open"],
            high=row["high"],
            low=row["low"],
            close=row["close"],
            volume=row["volume"],
        )
        records.append(rec)

    # ------------------------------------------------------------
    # 4. Bulk insert
    # ------------------------------------------------------------
    with Session(engine, future=True) as session:
        session.bulk_save_objects(records)
        session.commit()

    print(f"Inserted {len(records)} OHLC price records.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load OHLC prices into DB.")
    parser.add_argument(
        "--csv",
        type=str,
        default="src/insider_trading_analysis/out/ohlc_prices_*.csv",
        help="Glob path for OHLC CSVs",
    )

    args = parser.parse_args()

    files = sorted(glob.glob(args.csv))
    if not files:
        print(f"No CSV files matched pattern: {args.csv}")
        return

    for f in files:
        load_ohlc_csv(Path(f))


if __name__ == "__main__":
    main()
