import pandas as pd
from sqlalchemy.orm import Session
from insider_trading_analysis.database.db import engine
from insider_trading_analysis.database import models


class DB:
    """
    Backwards-compatible replacement for CSV-based storage.
    df_csv_dump / df_csv_read now operate on Postgres instead of files.
    """

    def df_csv_dump(self, file_name, df, index=False):
        """
        Old behavior: write DataFrame to CSV.
        New behavior: write DataFrame into Postgres based on file_name.
        """
        table = self._infer_table(file_name)
        if table is None:
            raise ValueError(f"Unknown file/table mapping for: {file_name}")

        with Session(engine) as session:
            objs = []

            # OHLC 
            if table == "ohlc":
                if "ticker" in df.columns:
                    tickers = set(df["ticker"].astype(str))
                    session.query(models.OHLC).filter(
                        models.OHLC.ticker.in_(list(tickers))
                    ).delete(synchronize_session=False)

                for row in df.to_dict("records"):
                    objs.append(models.OHLC(**row))

            # Insider transactions
            elif table == "insider":
                session.query(models.InsiderTransaction).delete(synchronize_session=False)
                for row in df.to_dict("records"):
                    objs.append(models.InsiderTransaction(**row))

            # Rollups
            elif table == "rollups":
                session.query(models.InsiderTradeRollup).delete(synchronize_session=False)
                for row in df.to_dict("records"):
                    objs.append(models.InsiderTradeRollup(**row))

            # Exchange mapping
            elif table == "exchange_mapping":
                session.query(models.ExchangeMapping).delete(synchronize_session=False)
                for row in df.to_dict("records"):
                    objs.append(models.ExchangeMapping(**row))

            session.bulk_save_objects(objs)
            session.commit()

    def df_csv_read(self, file_name, index_col=None, parse_dates=None):
        """
        Old behavior: read CSV into DataFrame.
        New behavior: read from Postgres.
        """
        table = self._infer_table(file_name)
        if table is None:
            raise ValueError(f"Unknown file/table mapping for: {file_name}")

        if table == "ohlc":
            return pd.read_sql(
                "SELECT * FROM ohlc_prices ORDER BY date;",
                engine,
                parse_dates=parse_dates,
            )

        if table == "insider":
            return pd.read_sql(
                "SELECT * FROM insider_transactions ORDER BY transaction_date;",
                engine,
                parse_dates=parse_dates,
            )

        if table == "rollups":
            return pd.read_sql(
                "SELECT * FROM insider_trade_rollups ORDER BY year;",
                engine
            )

        if table == "exchange_mapping":
            return pd.read_sql(
                "SELECT * FROM exchange_mapping ORDER BY ticker;",
                engine
            )

        raise ValueError(f"Unhandled table: {table}")

    def _infer_table(self, file_name: str):
        """
        Infer which DB table corresponds to an old CSV filename.
        """
        name = file_name.lower()

        # OHLC: "AAPL.csv"
        if (
            name.endswith(".csv")
            and "insider" not in name
            and "all_trades" not in name
            and "mapping" not in name
        ):
            return "ohlc"

        if "insider" in name and "all_trades" not in name:
            return "insider"

        if "all_trades" in name or "rollup" in name:
            return "rollups"

        if "mapping" in name:
            return "exchange_mapping"

        return None
