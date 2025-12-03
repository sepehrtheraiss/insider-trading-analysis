from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from .config import get_settings
from .models import Base  # this will be defined below

settings = get_settings()

engine = create_engine(
    settings.database_url,
    future=True,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    future=True,
)


def init_db() -> None:
    """
    Create all tables (for local dev / tests).
    For prod, prefer Alembic migrations.
    """
    Base.metadata.create_all(bind=engine)

    # Create or replace the merged view
    view_sql = """
    CREATE OR REPLACE VIEW insider_rollup AS
    SELECT
        t.*,
        m.name AS ticker_name,
        m.exchange,
        m.is_delisted,
        m.category,
        m.sector,
        m.industry,
        m.sic_sector,
        m.sic_industry
    FROM insider_transactions t
    LEFT JOIN exchange_mapping m
    ON t.issuer_ticker = m.issuer_ticker;
    """

    with engine.connect() as conn:
        conn.execute(text(view_sql))
        conn.commit()    

