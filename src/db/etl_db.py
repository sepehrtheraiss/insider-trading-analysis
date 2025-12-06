from datetime import datetime, UTC
from sqlalchemy.orm import Session
from sqlalchemy import text
from utils.logger import Logger
from .db import engine


class ETLDatabase:
    """
    Writable ETL database layer.
    Provides:
      - ORM session
      - upsert(model, rows, key)
      - insert_many(model, rows)
      - last_updated(table)
      - set_last_updated(table)
    """

    def __init__(self):
        self.engine = engine
        self.session = Session(self.engine, future=True)
        self.log = Logger(self.__class__.__name__)

    # -----------------------------------------------------------
    def last_updated(self, table_name: str):
        sql = text("""
            SELECT last_updated
            FROM etl_state
            WHERE table_name = :t
            LIMIT 1
        """)
        row = self.session.execute(sql, {"t": table_name}).fetchone()
        return row[0] if row else None

    def set_last_updated(self, table_name: str):
        sql = text("""
            INSERT INTO etl_state (table_name, last_updated)
            VALUES (:t, :ts)
            ON CONFLICT (table_name)
            DO UPDATE SET last_updated = EXCLUDED.last_updated
        """)
        ts = datetime.now(UTC)

        self.session.execute(sql, {"t": table_name, "ts": ts})
        self.session.commit()

        self.log.info(f"Updated last_updated for '{table_name}' → {ts}")

    # -----------------------------------------------------------
    def upsert(self, model, rows: list[dict], key: str):
        """
        UPSERT based on business key, using ORM logic.
        """
        for row in rows:
            existing = (
                self.session.query(model)
                .filter(getattr(model, key) == row[key])
                .one_or_none()
            )

            if existing:
                for k, v in row.items():
                    setattr(existing, k, v)
            else:
                self.session.add(model(**row))

        self.session.commit()

    # -----------------------------------------------------------
    def insert_many(self, model, rows: list[dict]):
        objects = [model(**r) for r in rows]
        self.session.bulk_save_objects(objects)
        self.session.commit()

    # -----------------------------------------------------------
    def close(self):
        self.session.close()
