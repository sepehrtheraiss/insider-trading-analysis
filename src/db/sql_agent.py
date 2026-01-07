
from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import Any, Optional

from openai import OpenAI

from config.settings import settings


_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|VACUUM|COPY)\b",
    re.IGNORECASE,
)


def assert_read_only_single_statement(sql: str) -> None:
    if _FORBIDDEN.search(sql):
        raise ValueError("Unsafe SQL detected (write/DDL keyword present).")
    s = sql.strip()
    if ";" in s.rstrip(";"):
        raise ValueError("Multiple statements are not allowed.")


def build_schema_context_text() -> str:
    return """DATABASE DIALECT: PostgreSQL

            TABLE: insider_transactions
            - id (integer, primary key)
            - accession_no (text)
            - filed_at (timestamptz)
            - period_of_report (timestamptz)
            - document_type (text)
            - issuer_ticker (text)
            - issuer_cik (text)
            - issuer_name (text)
            - reporter (text)
            - reporter_cik (text)
            - is_officer (boolean)
            - officer_title (text)
            - is_director (boolean)
            - is_ten_percent_owner (boolean)
            - table (text)
            - code (text)
            - acquired_disposed (text)
            - transaction_date (timestamptz)
            - shares (numeric)
            - price_per_share (numeric)
            - total_value (numeric)
            - shares_owned_following (numeric)
            - is_10b5_1 (boolean)

            TABLE: exchange_mapping
            - id (integer, primary key)
            - issuer_ticker (text)
            - name (text)
            - exchange (text)
            - is_delisted (boolean)
            - category (text)
            - sector (text)
            - industry (text)
            - sic_sector (text)
            - sic_industry (text)

            VIEW: insider_rollup
            - insider_transactions.*
            - ticker_name
            - exchange
            - is_delisted
            - category
            - sector
            - industry
            - sic_sector
            - sic_industry

            RULES:
            - Prefer insider_rollup by default.
            - Use timestamptz-aware filters.
            """


@dataclass(frozen=True)
class SqlAgentConfig:
    model: str = "gpt-5"
    temperature: float = 0.1
    default_limit: int = 200


class SqlAgent:
    def __init__(self, cfg: SqlAgentConfig, schema_text: Optional[str] = None):
        self.cfg = cfg
        self.schema_text = schema_text or build_schema_context_text()
        self.client = OpenAI(api_key=settings.openai_api_key)

    def generate(self, question: str) -> str:
        system = (
            "You are a senior data engineer writing PostgreSQL SQL for analytics. "
            "Output ONLY a single read-only query."
        )

        user = f"""SCHEMA:
            {self.schema_text}

            QUESTION:
            {question}

            CONSTRAINTS:
            - Output ONLY SQL.
            - Read-only.
            - Prefer insider_rollup.
            - Add LIMIT {self.cfg.default_limit} if needed.
            """

        resp = self.client.responses.create(
            model=self.cfg.model,
            #temperature=self.cfg.temperature,
            input=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )

        sql = resp.output_text.strip()
        assert_read_only_single_statement(sql)
        return sql

    def validate_semantics(self, *, question: str, sql: str) -> dict:
        system = (
            "Validate SQL semantics against the question. "
            "Check joins, filters, and aggregation correctness."
        )

        user = f"""SCHEMA:
            {self.schema_text}

            QUESTION:
            {question}

            SQL:
            {sql}

            Return STRICT JSON:
            {{
            "is_correct": true/false,
            "issues": ["..."],
            "corrected_sql": null | "..."
            }}
            """

        resp = self.client.responses.create(
            model=self.cfg.model,
            #temperature=0.0,
            input=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )

        return json.loads(resp.output_text)

    def optimize_from_explain(self, *, sql: str, explain_text: str) -> dict[str, Any]:
        system = (
            "You are a PostgreSQL performance expert. "
            "Rewrite SQL to reduce runtime without changing meaning."
        )

        user = f"""SQL:
            {sql}

            EXPLAIN:
            {explain_text}

            Return STRICT JSON:
            {{
            "improved_sql": "...",
            "notes": ["..."],
            "index_suggestions": ["CREATE INDEX ..."]
            }}
            """

        resp = self.client.responses.create(
            model=self.cfg.model,
            #temperature=0.0,
            input=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )

        data = json.loads(resp.output_text)
        assert_read_only_single_statement(data["improved_sql"])
        return data
