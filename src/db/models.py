# db/models.py
from sqlalchemy import (
    Boolean,
    Column,
    Integer,
    String,
    Date,
    Numeric,
    BigInteger,
    DateTime,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


# -----------------------------
# OHLC Prices
# -----------------------------
class OHLC(Base):
    __tablename__ = "ohlc_prices"

    ticker = Column(String, primary_key=True)
    date = Column(Date, primary_key=True)

    open = Column(Numeric)
    high = Column(Numeric)
    low = Column(Numeric)
    close = Column(Numeric)
    volume = Column(BigInteger)


# -----------------------------
# Insider Transactions
# -----------------------------
class InsiderTransaction(Base):
    __tablename__ = "insider_transactions"

    # __table_args__ = (
    #     # Prevent exact duplicate transactions (business key)
    #     UniqueConstraint(
    #         "issuer_ticker",
    #         "period_of_report",
    #         "shares",
    #         "price_per_share",
    #         name="uq_insider_transactions_business_key",
    #     ),
    # )

    id = Column(Integer, primary_key=True, autoincrement=True)

    filed_at = Column(DateTime(timezone=True), index=True)
    period_of_report = Column(DateTime(timezone=True), index=True)
    document_type = Column(String)

    issuer_ticker = Column(String, index=True)
    issuer_cik = Column(String, index=True)
    issuer_name = Column(String)

    reporter = Column(String)
    reporter_cik = Column(String)

    is_officer = Column(Boolean)
    officer_title = Column(String)

    is_director = Column(Boolean)
    is_ten_percent_owner = Column(Boolean)

    table = Column(String)
    code = Column(String)
    acquired_disposed = Column(String)

    transaction_date = Column(DateTime(timezone=True), index=True)

    shares = Column(Numeric)
    price_per_share = Column(Numeric)
    total_value = Column(Numeric)
    shares_owned_following = Column(Numeric)

    is_10b5_1 = Column(Boolean)


# -----------------------------
# Exchange Mapping
# -----------------------------
class ExchangeMapping(Base):
    __tablename__ = "exchange_mapping"

    __table_args__ = (
        UniqueConstraint("issuer_ticker", name="uq_exchange_mapping_issuer_ticker"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)

    name = Column(String)
    issuer_ticker = Column(String, index=True)
    cik = Column(String, index=True)
    exchange = Column(String)
    is_delisted = Column(Boolean)

    category = Column(String)
    sector = Column(String)
    industry = Column(String)

    sic_sector = Column(String)
    sic_industry = Column(String)
