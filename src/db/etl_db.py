# db/etl_db.py
from datetime import datetime, UTC

from sqlalchemy import text
from sqlalchemy.orm import Session

from utils.logger import Logger
from .db import engine


class ETLDatabase:
    """
    Writable ETL database layer.

    Provides:
      - last_updated(table_name)
      - set_last_updated(table_name)
      - upsert(model, rows, key)
      - insert_many(model, rows)
    """

    def __init__(self):
        self.engine = engine
        self.log = Logger(self.__class__.__name__)

    # -----------------------------------------------------------
    # Session helper
    # short-lived, isolated sessions
    # -----------------------------------------------------------
    def _session(self) -> Session:
        return Session(self.engine, future=True)

    # -----------------------------------------------------------
    # ETL state tracking
    # -----------------------------------------------------------
    def last_updated(self, table_name: str):
        sql = text("""
            SELECT last_updated
            FROM etl_state
            WHERE table_name = :t
            LIMIT 1
        """)

        with self._session() as session:
            row = session.execute(sql, {"t": table_name}).fetchone()
            return row[0] if row else None

    def set_last_updated(self, table_name: str):
        sql = text("""
            INSERT INTO etl_state (table_name, last_updated)
            VALUES (:t, :ts)
            ON CONFLICT (table_name)
            DO UPDATE SET last_updated = EXCLUDED.last_updated
        """)

        ts = datetime.now(UTC)

        with self._session() as session:
            session.execute(sql, {"t": table_name, "ts": ts})
            session.commit()

        self.log.info(f"[ETL_STATE] Updated last_updated for '{table_name}' → {ts}")

    # -----------------------------------------------------------
    # Generic UPSERT
    # -----------------------------------------------------------
    def upsert(self, model, rows: list[dict], key: str):
        """
        Naive ORM-based upsert using a single-column business key.

        For each row:
          - SELECT existing by model.<key> == row[key]
          - UPDATE fields if exists
          - INSERT new row if not
        """
        if not rows:
            return

        with self._session() as session:
            for row in rows:
                existing = (
                    session.query(model)
                    .filter(getattr(model, key) == row[key])
                    .one_or_none()
                )

                if existing:
                    for k, v in row.items():
                        setattr(existing, k, v)
                else:
                    session.add(model(**row))

            session.commit()

    # -----------------------------------------------------------
    # Bulk insert
    # -----------------------------------------------------------
    def insert_many(self, model, rows: list[dict]):
        """
        Bulk insert many rows for a given SQLAlchemy model.
        """
        if not rows:
            return

        objects = [model(**r) for r in rows]
        with self._session() as session:
            session.bulk_save_objects(objects)
            session.commit()
