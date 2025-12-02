import argparse
from pathlib import Path

import pandas as pd
from sqlalchemy.orm import Session

from insider_trading_analysis.database.db import engine
from insider_trading_analysis.database.models import InsiderTransaction

def safe_date(value):
    if value is None:
        return None
    if pd.isna(value):  # catches NaN and NaT
        return None
    return value

def load_insider_transactions(csv_path: Path) -> None:
    df = pd.read_csv(csv_path)

    # Normalize column names from your CSV to model attributes.
    # Adjust this mapping to match exactly what you have.
    rename_map = {
        "issuerCik": "issuer_cik",
        "issuerTicker": "issuer_ticker",
        "issuerName": "issuer_name",
        "reportingOwnerCik": "reporting_owner_cik",
        "reportingOwnerName": "reporting_owner_name",
        "periodOfReport": "period_of_report",
        "transactionDate": "transaction_date",
        "filedAt": "filedAt",
        "securityTitle": "security_title",
        "transactionCode": "transaction_code",
        "acquiredDisposed": "acquired_disposed",
        "transactionShares": "transaction_shares",
        "transactionPricePerShare": "transaction_price_per_share",
        "transactionValue": "transaction_value",
        "link": "link",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Parse dates
    for col in ("period_of_report", "transaction_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    records = []
    for _, row in df.iterrows():
        rec = InsiderTransaction(
            issuer_cik=row.get("issuer_cik"),
            issuer_ticker=row.get("issuer_ticker"),
            issuer_name=row.get("issuer_name"),
            reporting_owner_cik=row.get("reporting_owner_cik"),
            reporting_owner_name=row.get("reporting_owner_name"),
            period_of_report=safe_date(row.get("period_of_report")),
            transaction_date=safe_date(row.get("transaction_date")),
            filed_at=row.get("filedAt"),
            security_title=row.get("security_title"),
            transaction_code=row.get("transaction_code"),
            acquired_disposed=row.get("acquired_disposed"),
            transaction_shares=row.get("transaction_shares"),
            transaction_price_per_share=row.get("transaction_price_per_share"),
            transaction_value=row.get("transaction_value"),
            link=row.get("link"),
        )
        records.append(rec)

    with Session(engine, future=True) as session:
        session.bulk_save_objects(records)
        session.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load insider transactions into DB.")
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path(
            "src/insider_trading_analysis/out/insider_transactions.*:*_2016-01-01_2025-01-01.csv"
        ),
        help="Path to insider_transactions_*.csv",
    )
    args = parser.parse_args()
    load_insider_transactions(args.csv)


if __name__ == "__main__":
    main()

