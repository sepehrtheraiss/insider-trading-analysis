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
    print(f"Loading insider transactions from {csv_path}")

    # ----------------------------------------------------------
    # 1. Load CSV
    # ----------------------------------------------------------
    df = pd.read_csv(csv_path)

    # ----------------------------------------------------------
    # 2. Rename camelCase -> snake_case (exact 1-to-1 mapping)
    # ----------------------------------------------------------
    rename_map = {
        "filedAt": "filed_at",
        "periodOfReport": "period_of_report",
        "documentType": "document_type",
        "issuerTicker": "issuer_ticker",
        "issuerCik": "issuer_cik",
        "issuerName": "issuer_name",
        "reporter": "reporter",
        "reporterCik": "reporter_cik",
        "isOfficer": "is_officer",
        "officerTitle": "officer_title",
        "isDirector": "is_director",
        "isTenPercentOwner": "is_ten_percent_owner",
        "table": "table",
        "code": "code",
        "acquiredDisposed": "acquired_disposed",
        "transactionDate": "transaction_date",
        "shares": "shares",
        "pricePerShare": "price_per_share",
        "totalValue": "total_value",
        "sharesOwnedFollowing": "shares_owned_following",
        "is10b5_1": "is_10b5_1",
    }

    df = df.rename(columns=rename_map)

    # ----------------------------------------------------------
    # 3. Datetime conversion
    # ----------------------------------------------------------
    datetime_cols = [
        "filed_at",
        "period_of_report",
        "transaction_date",
    ]

    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    # ----------------------------------------------------------
    # 4. Boolean conversion
    # ----------------------------------------------------------
    boolean_cols = [
        "is_officer",
        "is_director",
        "is_ten_percent_owner",
        "is_10b5_1",
    ]

    for col in boolean_cols:
        if col in df.columns:
            df[col] = df[col].astype("boolean")

    # ----------------------------------------------------------
    # 5. Numeric conversion
    # ----------------------------------------------------------
    numeric_cols = [
        "shares",
        "price_per_share",
        "total_value",
        "shares_owned_following",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ----------------------------------------------------------
    # 6. Build ORM objects
    # ----------------------------------------------------------
    records = []
    for _, row in df.iterrows():
        rec = InsiderTransaction(
            filed_at=row.get("filed_at"),
            period_of_report=safe_date(row.get("period_of_report")),
            document_type=row.get("document_type"),

            issuer_ticker=row.get("issuer_ticker"),
            issuer_cik=row.get("issuer_cik"),
            issuer_name=row.get("issuer_name"),

            reporter=row.get("reporter"),
            reporter_cik=row.get("reporter_cik"),

            is_officer=row.get("is_officer"),
            officer_title=row.get("officer_title"),

            is_director=row.get("is_director"),
            is_ten_percent_owner=row.get("is_ten_percent_owner"),

            table=row.get("table"),
            code=row.get("code"),
            acquired_disposed=row.get("acquired_disposed"),

            transaction_date=safe_date(row.get("transaction_date")),

            shares=row.get("shares"),
            price_per_share=row.get("price_per_share"),
            total_value=row.get("total_value"),
            shares_owned_following=row.get("shares_owned_following"),

            is_10b5_1=row.get("is_10b5_1"),
        )
        records.append(rec)

    # ----------------------------------------------------------
    # 7. Bulk insert
    # ----------------------------------------------------------
    with Session(engine, future=True) as session:
        session.bulk_save_objects(records)
        session.commit()

    print(f"Inserted {len(records)} insider transactions.")

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

