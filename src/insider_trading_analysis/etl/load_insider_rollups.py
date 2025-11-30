import argparse
from pathlib import Path

import pandas as pd
from sqlalchemy.orm import Session

from insider_trading_analysis.database.db import engine
from insider_trading_analysis.database.models import InsiderTradeRollup


def load_insider_rollups(csv_path: Path) -> None:
    df = pd.read_csv(csv_path)

    # Adjust based on actual headers
    rename_map = {
        "ticker": "ticker",
        "company_name": "company_name",
        "company_cik": "company_cik",
        "reporting_owner_cik": "reporting_owner_cik",
        "reporting_owner_name": "reporting_owner_name",
        "year": "year",
        "total_value": "total_value",
        "num_transactions": "num_transactions",
        "total_shares": "total_shares",
        "avg_price": "avg_price",
        "acquired_value": "acquired_value",
        "disposed_value": "disposed_value",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    records = []
    for _, row in df.iterrows():
        rec = InsiderTradeRollup(
            ticker=row.get("ticker"),
            company_name=row.get("company_name"),
            company_cik=row.get("company_cik"),
            reporting_owner_cik=row.get("reporting_owner_cik"),
            reporting_owner_name=row.get("reporting_owner_name"),
            year=int(row.get("year")) if not pd.isna(row.get("year")) else None,
            total_value=row.get("total_value"),
            num_transactions=row.get("num_transactions"),
            total_shares=row.get("total_shares"),
            avg_price=row.get("avg_price"),
            acquired_value=row.get("acquired_value"),
            disposed_value=row.get("disposed_value"),
        )
        records.append(rec)

    with Session(engine, future=True) as session:
        session.bulk_save_objects(records)
        session.commit()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load insider trade rollups (all_trades_2016_2025.csv) into DB."
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path("src/insider_trading_analysis/out/all_trades_2016_2025.csv"),
        help="Path to all_trades_2016_2025.csv",
    )
    args = parser.parse_args()
    load_insider_rollups(args.csv)


if __name__ == "__main__":
    main()

