from sqlalchemy import text
import pandas as pd
from sqlalchemy.orm import Session

from .db import engine
from .models import OHLC

class InsiderRepository:
    """
    Centralized data access layer for your entire project.
    All DB reads should go through this class.

    Returns pandas DataFrames with consistent snake_case columns.
    """

    def __init__(self):
        self.engine = engine

    # ---------------------------------------
    # Raw Insider Transactions (source of truth)
    # ---------------------------------------
    def get_transactions(self, start=None, end=None):
        sql = "SELECT * FROM insider_transactions"
        filters = []

        if start:
            filters.append("period_of_report >= %(start)s")
        if end:
            filters.append("period_of_report <= %(end)s")

        if filters:
            sql += " WHERE " + " AND ".join(filters)

        sql += " ORDER BY period_of_report"

        return pd.read_sql(sql, self.engine, params={"start": start, "end": end})

    # ---------------------------------------
    # Exchange Mapping Metadata (source of truth)
    # ---------------------------------------
    def get_mapping(self):
        sql = "SELECT * FROM exchange_mapping ORDER BY issuer_ticker"
        return pd.read_sql(sql, self.engine)

    # ---------------------------------------
    # Standalone OHLC Prices Table
    # ---------------------------------------
    def get_ohlc(self, ticker, start=None, end=None):
        sql = "SELECT * FROM ohlc_prices WHERE ticker = %(ticker)s"
        params = {"ticker": ticker}

        if start:
            sql += " AND date >= %(start)s"
            params["start"] = start
        if end:
            sql += " AND date <= %(end)s"
            params["end"] = end

        sql += " ORDER BY date"
        return pd.read_sql(sql, self.engine, params=params)

    # ---------------------------------------
    # Merged Insider Rollup View (recommended for analytics)
    # ---------------------------------------
    def get_rollup(self, start=None, end=None):
        """
        Query the SQL VIEW `insider_rollup` that merges:
        - insider_transactions
        - exchange_mapping

        This is the preferred entry point for all analytics & plotting.
        """

        sql = "SELECT * FROM insider_rollup"
        filters = []

        if start:
            filters.append("period_of_report >= %(start)s")
        if end:
            filters.append("period_of_report <= %(end)s")

        if filters:
            sql += " WHERE " + " AND ".join(filters)

        sql += " ORDER BY period_of_report"

        return pd.read_sql(sql, self.engine, params={"start": start, "end": end})

    def ohlc_exists_in_range(self, ticker: str, start: str, end: str) -> bool:
        """
        Returns True if OHLC data exists for this ticker between start and end dates.
        """
        sql = text("""
            SELECT 1
            FROM ohlc_prices
            WHERE ticker = :ticker
              AND date >= :start
              AND date <= :end
            LIMIT 1
        """)
        params = {
            "ticker": ticker,
            "start": start,
            "end": end
        }
        with engine.connect() as conn:
            row = conn.execute(sql, params).fetchone()
            return row is not None
        
    def insert_ohlc_dataframe(self, df):
        """
        Insert OHLC prices into the database from a pandas DataFrame.

        Required columns:
        ticker, date, open, high, low, close, volume
        """

        required_cols = ["ticker", "date", "open", "high", "low", "close", "volume"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required OHLC column: {col}")

        # Convert date column to datetime
        df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)

        # Drop invalid rows
        df = df.dropna(subset=["ticker", "date", "close"])

        # Convert numeric columns
        numeric_cols = ["open", "high", "low", "close", "volume"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Build ORM objects
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

        # Bulk insert
        with Session(engine, future=True) as session:
            session.bulk_save_objects(records)
            session.commit()

        print(f"Inserted {len(records)} OHLC rows.")            
