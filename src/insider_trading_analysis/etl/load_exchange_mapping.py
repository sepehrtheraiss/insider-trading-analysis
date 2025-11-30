import argparse
from pathlib import Path

import pandas as pd
from sqlalchemy.orm import Session

from insider_trading_analysis.database.db import engine
from insider_trading_analysis.database.models import ExchangeMapping


def load_exchange_mapping(csv_path: Path) -> None:
    df = pd.read_csv(csv_path)

    # Adjust these renames to match your actual headers if needed
    rename_map = {
        "Ticker": "ticker",
        "Company Name": "company_name",
        "Market Cap": "market_cap",
        "IPO Year": "ipo_year",
        "Sector": "sector",
        "Industry": "industry",
        "Exchange": "exchange",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    records = []
    for _, row in df.iterrows():
        ticker = row.get("ticker")

        # skip rows with no ticker
        if pd.isna(ticker) or ticker is None or str(ticker).strip() == "":
            continue
        rec = ExchangeMapping(
            ticker=ticker,
            company_name=row.get("company_name"),
            market_cap=row.get("market_cap"),
            ipo_year=row.get("ipo_year"),
            sector=row.get("sector"),
            industry=row.get("industry"),
            exchange=row.get("exchange"),
        )
        records.append(rec)

    with Session(engine, future=True) as session:
        # Upsert-simple: delete existing, then insert (good enough for smallish table)
        existing_tickers = {
            t for (t,) in session.query(ExchangeMapping.ticker).all()
        }
        new_tickers = {r.ticker for r in records if r.ticker is not None}
        to_delete = existing_tickers & new_tickers
        if to_delete:
            session.query(ExchangeMapping).filter(
                ExchangeMapping.ticker.in_(list(to_delete))
            ).delete(synchronize_session=False)

        session.bulk_save_objects(records)
        session.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load exchange mapping into DB.")
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path("src/insider_trading_analysis/out/exchange_mapping_nasdaq_nyse.csv"),
        help="Path to exchange_mapping_nasdaq_nyse.csv",
    )
    args = parser.parse_args()
    load_exchange_mapping(args.csv)


if __name__ == "__main__":
    main()

