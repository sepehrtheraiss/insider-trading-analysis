import argparse
from pathlib import Path

import pandas as pd
from sqlalchemy.orm import Session

from db.db import engine
from db.models import ExchangeMapping


def load_exchange_mapping(csv_path: Path) -> None:
    print(f"Loading exchange mapping from {csv_path}")

    df = pd.read_csv(csv_path)

    # ----------------------------------------
    # Rename camelCase -> snake_case
    # ----------------------------------------
    rename_map = {
        "name": "name",
        "issuerTicker": "issuer_ticker",
        "cik": "cik",
        "exchange": "exchange",
        "isDelisted": "is_delisted",
        "category": "category",
        "sector": "sector",
        "industry": "industry",
        "sicSector": "sic_sector",
        "sicIndustry": "sic_industry",
    }

    df = df.rename(columns=rename_map)

    # ----------------------------------------
    # Boolean conversions
    # ----------------------------------------
    if "is_delisted" in df.columns:
        df["is_delisted"] = df["is_delisted"].astype("boolean")

    # ----------------------------------------
    # Build ORM objects
    # ----------------------------------------
    records = []
    for _, row in df.iterrows():
        rec = ExchangeMapping(
            name=row.get("name"),
            issuer_ticker=row.get("issuer_ticker"),
            cik=row.get("cik"),
            exchange=row.get("exchange"),
            is_delisted=row.get("is_delisted"),
            category=row.get("category"),
            sector=row.get("sector"),
            industry=row.get("industry"),
            sic_sector=row.get("sic_sector"),
            sic_industry=row.get("sic_industry"),
        )
        records.append(rec)

    with Session(engine, future=True) as session:
        # # Upsert-simple: delete existing, then insert (good enough for smallish table)
        # existing_tickers = {
        #     t for (t,) in session.query(ExchangeMapping.ticker).all()
        # }
        # new_tickers = {r.ticker for r in records if r.ticker is not None}
        # to_delete = existing_tickers & new_tickers
        # if to_delete:
        #     session.query(ExchangeMapping).filter(
        #         ExchangeMapping.ticker.in_(list(to_delete))
        #     ).delete(synchronize_session=False)

        session.bulk_save_objects(records)
        session.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load exchange mapping into DB.")
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path("samples/exchange_mapping_nasdaq_nyse.csv"),
        help="Path to exchange_mapping_nasdaq_nyse.csv",
    )
    args = parser.parse_args()
    load_exchange_mapping(args.csv)


if __name__ == "__main__":
    main()

