from sqlalchemy import (
    Boolean,
    Column,
    Integer,
    String,
    Date,
    Numeric,
    BigInteger,
    DateTime,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


# -----------------------------
# OHLC (you said this is correct)
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
# 1) Insider transactions (atomic, from insider_transactions_*.csv)
# -----------------------------
class InsiderTransaction(Base):
    __tablename__ = "insider_transactions"

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
# 2) Insider trade rollups (from all_trades_2016_2025.csv)
#    one row per (ticker, owner, year)
# -----------------------------
class InsiderTradeRollup(Base):
    __tablename__ = "insider_trade_rollups"

    id = Column(Integer, primary_key=True, autoincrement=True)

    filed_at = Column(DateTime(timezone=True))
    period_of_report = Column(DateTime(timezone=True))
    document_type = Column(String)

    issuer_ticker = Column(String)
    issuer_cik = Column(String)
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

    transaction_date = Column(DateTime(timezone=True))

    shares = Column(Numeric)
    price_per_share = Column(Numeric)
    total_value = Column(Numeric)
    shares_owned_following = Column(Numeric)

    is_10b5_1 = Column(Boolean)

    name = Column(String)
    cik = Column(String)
    exchange = Column(String)
    is_delisted = Column(Boolean)

    category = Column(String)
    sector = Column(String)
    industry = Column(String)

    sic_sector = Column(String)
    sic_industry = Column(String)

    code_simple = Column(String)



# -----------------------------
# 3) Exchange mapping (from exchange_mapping_nasdaq_nyse.csv)
#    dimension table: one row per ticker
# -----------------------------
class ExchangeMapping(Base):
    __tablename__ = "exchange_mapping"

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

