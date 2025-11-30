from sqlalchemy import (
    Column,
    Integer,
    String,
    Date,
    Numeric,
    BigInteger,
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

    issuer_cik = Column(String, index=True)
    issuer_ticker = Column(String, index=True)
    issuer_name = Column(String)

    reporting_owner_cik = Column(String, index=True)
    reporting_owner_name = Column(String)

    period_of_report = Column(Date, index=True)
    transaction_date = Column(Date, index=True)

    security_title = Column(String)
    transaction_code = Column(String)
    acquired_disposed = Column(String)  # 'A' or 'D'

    transaction_shares = Column(Numeric)
    transaction_price_per_share = Column(Numeric)
    transaction_value = Column(Numeric)

    link = Column(String)


# -----------------------------
# 2) Insider trade rollups (from all_trades_2016_2025.csv)
#    one row per (ticker, owner, year)
# -----------------------------
class InsiderTradeRollup(Base):
    __tablename__ = "insider_trade_rollups"

    id = Column(Integer, primary_key=True, autoincrement=True)

    ticker = Column(String, index=True)
    company_name = Column(String)
    company_cik = Column(String)

    reporting_owner_cik = Column(String, index=True)
    reporting_owner_name = Column(String)

    year = Column(Integer, index=True)

    total_value = Column(Numeric)
    num_transactions = Column(Integer)
    total_shares = Column(Numeric)
    avg_price = Column(Numeric)
    acquired_value = Column(Numeric)
    disposed_value = Column(Numeric)


# -----------------------------
# 3) Exchange mapping (from exchange_mapping_nasdaq_nyse.csv)
#    dimension table: one row per ticker
# -----------------------------
class ExchangeMapping(Base):
    __tablename__ = "exchange_mapping"

    ticker = Column(String, primary_key=True)

    company_name = Column(String)
    market_cap = Column(Numeric)
    ipo_year = Column(Integer)
    sector = Column(String)
    industry = Column(String)
    exchange = Column(String)

