
from sqlalchemy import text
from typing import Any
from .db import engine
from .sql_agent import assert_read_only_single_statement


def explain_analyze(sql: str, timeout_ms: int = 5000) -> str:
    assert_read_only_single_statement(sql)
    explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, VERBOSE) {sql}"

    with engine.begin() as conn:
        conn.execute(text(f"SET LOCAL statement_timeout = {timeout_ms}"))
        rows = conn.execute(text(explain_sql)).fetchall()

    return "\n".join(r[0] for r in rows)


def run_query(sql: str, timeout_ms: int = 5000) -> list[dict[str, Any]]:
    assert_read_only_single_statement(sql)

    with engine.begin() as conn:
        conn.execute(text(f"SET LOCAL statement_timeout = {timeout_ms}"))
        result = conn.execute(text(sql))
        cols = list(result.keys())
        return [dict(zip(cols, row)) for row in result.fetchall()]
