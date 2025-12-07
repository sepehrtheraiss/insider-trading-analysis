# insider_trading/transform/normalize_transactions.py

from typing import List, Sequence
import pandas as pd

def _footnotes_text(footnotes: List[dict] | None) -> str:
    if not footnotes:
        return ""
    return "\n".join(f.get("text","") for f in footnotes if isinstance(f, dict))

def _extract_table_rows(t: dict, table_key: str) -> list[dict]:
    table = t.get(table_key) or {}
    rows = table.get("transactions") or []
    return rows if isinstance(rows, list) else []

def normalize_transactions(transactions: Sequence[dict]) -> pd.DataFrame:
    """
    Convert raw SEC 'transactions' objects to a normalized flat DataFrame.
    Produces 1 row per transaction line-item.
    """
    records: list[dict] = []

    for t in transactions:
        issuer = (t.get("issuer") or {})
        ro = (t.get("reportingOwner") or {})
        rel = (ro.get("relationship") or {})

        filed_at = t.get("filedAt") or t.get("filed_at")
        period = t.get("periodOfReport") or t.get("period_of_report")
        doc_type = t.get("documentType") or t.get("document_type")

        # Footnotes → detect 10b5-1 plans
        fnotes = t.get("footnotes") or []
        fn_text = _footnotes_text(fnotes)
        is_10b5 = (
            "10b5-1" in fn_text or
            "10b5–1" in fn_text or
            "Rule 10b5" in fn_text
        )

        # Both derivative & non-derivative tables
        for table_key, label in (
            ("nonDerivativeTable", "non-derivative"),
            ("derivativeTable", "derivative"),
        ):
            for row in _extract_table_rows(t, table_key):
                coding = row.get("coding") or {}
                amts = row.get("amounts") or {}
                post = row.get("postTransactionAmounts") or {}

                shares = amts.get("shares")
                price = amts.get("price_per_share")
                code = coding.get("code")
                ad = amts.get("acquiredDisposedCode")
                tx_date = row.get("transaction_date")
                post_sh = post.get("sharesOwnedFollowingTransaction")

                try:
                    total_value = float(shares) * float(price) if shares and price else None
                except:
                    total_value = None

                records.append({
                    "filed_at": filed_at,
                    "period_of_report": period,
                    "document_type": doc_type,

                    "issuer_ticker": issuer.get("tradingSymbol"),
                    "issuer_cik": issuer.get("cik"),
                    "issuer_name": issuer.get("name"),

                    "reporter": ro.get("name"),
                    "reporter_cik": ro.get("cik"),

                    "is_officer": rel.get("isOfficer") or rel.get("is_officer"),
                    "officer_title": rel.get("officerTitle"),

                    "is_director": rel.get("isDirector"),
                    "is_ten_percent_owner": rel.get("isTenPercentOwner"),

                    "table": label,
                    "code": code,
                    "acquired_disposed": ad,
                    "transaction_date": tx_date,

                    "shares": shares,
                    "price_per_share": price,
                    "total_value": total_value,
                    "shares_owned_following": post_sh,

                    "is_10b5_1": bool(is_10b5),
                })

    df = pd.DataFrame.from_records(records)

    # Normalize types
    if not df.empty:
        numeric_cols = [
            "shares", "price_per_share", "total_value", "shares_owned_following"
        ]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        datetime_cols = [
            "filed_at", "transaction_date", "period_of_report"
        ]
        for col in datetime_cols:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    return df
