import argparse
from pathlib import Path

import pandas as pd
from sqlalchemy.orm import Session

from insider_trading_analysis.database.db import engine
from insider_trading_analysis.database.models import InsiderTradeRollup

def safe_date(value):
    if value is None:
        return None
    if pd.isna(value):  # catches NaN and NaT
        return None
    return value

def load_insider_rollups(csv_path: Path) -> None:
    print(f"Loading insider rollups from {csv_path}")

    df = pd.read_csv(csv_path)

    # ----------------------------------------
    # 1. Rename camelCase -> snake_case
    # ----------------------------------------
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
        "name": "name",
        "cik": "cik",
        "exchange": "exchange",
        "isDelisted": "is_delisted",
        "category": "category",
        "sector": "sector",
        "industry": "industry",
        "sicSector": "sic_sector",
        "sicIndustry": "sic_industry",
        "code_simple": "code_simple",
    }

    df = df.rename(columns=rename_map)

    # ----------------------------------------
    # 2. Datetime conversions
    # ----------------------------------------
    datetime_cols = ["filed_at", "period_of_report", "transaction_date"]

    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    # ----------------------------------------
    # 3. Boolean conversions
    # ----------------------------------------
    boolean_cols = ["is_officer", "is_director", "is_ten_percent_owner", "is_10b5_1", "is_delisted"]

    for col in boolean_cols:
        if col in df.columns:
            df[col] = df[col].astype("boolean")

    # ----------------------------------------
    # 4. Numeric conversions
    # ----------------------------------------
    numeric_cols = [
        "shares",
        "price_per_share",
        "total_value",
        "shares_owned_following",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ----------------------------------------
    # 5. Convert rows to ORM objects
    # ----------------------------------------
    records = []
    for _, row in df.iterrows():
        rec = InsiderTradeRollup(
            filed_at=safe_date(row.get("filed_at")),
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

            name=row.get("name"),
            cik=row.get("cik"),
            exchange=row.get("exchange"),
            is_delisted=row.get("is_delisted"),

            category=row.get("category"),
            sector=row.get("sector"),
            industry=row.get("industry"),

            sic_sector=row.get("sic_sector"),
            sic_industry=row.get("sic_industry"),

            code_simple=row.get("code_simple"),
        )
        records.append(rec)

    # ----------------------------------------
    # 6. Bulk insert
    # ----------------------------------------
    with Session(engine, future=True) as session:
        session.bulk_save_objects(records)
        session.commit()

    print(f"Inserted {len(records)} rollup records.")


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

