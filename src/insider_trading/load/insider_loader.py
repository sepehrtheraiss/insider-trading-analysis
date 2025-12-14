from sqlalchemy.orm import Session
from sqlalchemy import select
from utils.logger import Logger
from db.db import engine
from db.models import InsiderTransaction
import pandas as pd


class InsiderTransactionsLoader:
    """
    Loads transformed insider transaction data into the database.

    - Insert-only (historical records)
    - Duplicate protection via business key:
        (issuer_ticker, period_of_report, shares, price_per_share)
    """

    DEDUPE_KEY = ["issuer_ticker", "period_of_report", "shares", "price_per_share"]

    def __init__(self, db):
        self.db = db
        self.log = Logger(self.__class__.__name__)

    # ----------------------------------------------------------------------
    def _exists(self, session: Session, row: pd.Series) -> bool:
        """
        Return True if a duplicate insider transaction already exists.
        Duplicate defined by business key.
        """

        filters = []
        for col in self.DEDUPE_KEY:
            value = row[col]
            if pd.isna(value):
                filters.append(getattr(InsiderTransaction, col).is_(None))
            else:
                filters.append(getattr(InsiderTransaction, col) == value)

        stmt = select(InsiderTransaction).filter(*filters).limit(1)
        return session.execute(stmt).first() is not None

    # ----------------------------------------------------------------------
    def load(self, df: pd.DataFrame):
        """
        Insert transformed insider transactions into the database.
        Duplicate transactions (by DEDUPE_KEY) are skipped.
        """

        if df.empty:
            self.log.info("[LOAD] No insider transactions to load.")
            return

        inserted = 0
        skipped = 0

        with Session(engine, future=True) as session:
            for _, row in df.iterrows():

                #if self._exists(session, row):
                #    skipped += 1
                #    continue

                obj = InsiderTransaction(
                    accession_no=row["accession_no"],
                    filed_at=row["filed_at"],
                    period_of_report=row["period_of_report"],
                    document_type=row["document_type"],
                    issuer_ticker=row["issuer_ticker"],
                    issuer_cik=row["issuer_cik"],
                    issuer_name=row["issuer_name"],
                    reporter=row["reporter"],
                    reporter_cik=row["reporter_cik"],
                    is_officer=row["is_officer"],
                    officer_title=row["officer_title"],
                    is_director=row["is_director"],
                    is_ten_percent_owner=row["is_ten_percent_owner"],
                    table=row["table"],
                    code=row["code"],
                    acquired_disposed=row["acquired_disposed"],
                    transaction_date=row["transaction_date"],
                    shares=row["shares"],
                    price_per_share=row["price_per_share"],
                    total_value=row["total_value"],
                    shares_owned_following=row["shares_owned_following"],
                    is_10b5_1=row["is_10b5_1"],
                )

                session.add(obj)
                inserted += 1

            session.commit()

        self.log.info(
            f"[LOAD] Insider transactions: inserted={inserted}, skipped={skipped}"
        )
