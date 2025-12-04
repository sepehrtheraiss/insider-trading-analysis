from typing import Dict, Any, Iterable, List
import pandas as pd
import math

def _footnotes_text(footnotes: List[dict] | None) -> str:
    if not footnotes:
        return ""
    return "\n".join(f.get("text","") for f in footnotes if isinstance(f, dict))

def _extract_table_rows(t: dict, table_key: str) -> list[dict]:
    table = t.get(table_key) or {}
    rows = table.get("transactions") or []
    return rows if isinstance(rows, list) else []

def normalize_transactions(transactions: Iterable[Dict[str, Any]]) -> pd.DataFrame:
    """
    Convert raw InsiderTradingApi 'transactions' list into a flat DataFrame of individual line-items.
    Returns columns:
      filed_at, period_of_report, issuer_ticker, issuer_cik, issuer_name,
      reporter, reporter_cik, is_officer, officer_title, is_director, is_ten_percent_owner,
      table, code, acquired_disposed, transaction_date, shares, price_per_share,
      shares_owned_following, total_value, is_10b5_1, document_type
    """
    records: list[dict] = []
    for t in transactions:
        issuer = (t.get("issuer") or {})
        ro = (t.get("reportingOwner") or {})
        rel = (ro.get("relationship") or {})
        filed_at = t.get("filed_at")
        period = t.get("period_of_report")
        doc_type = t.get("document_type")

        fnotes = t.get("footnotes") or []
        fn_text = _footnotes_text(fnotes)
        is_10b5 = "10b5-1" in fn_text or "10b5–1" in fn_text or "Rule 10b5" in fn_text

        for table_key, label in (("nonDerivativeTable","non-derivative"), ("derivativeTable","derivative")):
            for row in _extract_table_rows(t, table_key):
                coding = row.get("coding") or {}
                amts = row.get("amounts") or {}
                post = row.get("postTransactionAmounts") or {}

                shares = amts.get("shares")
                price = amts.get("price_per_share")
                code = (coding.get("code") or None)
                ad = amts.get("acquiredDisposedCode")
                tx_date = row.get("transaction_date")
                post_sh = post.get("sharesOwnedFollowingTransaction")
                # for case of shares being a str
                try:
                    total_value = float(shares) * float(price) if shares is not None and price is not None else None
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
                    "is_officer": rel.get("is_officer"),
                    "officer_title": rel.get("officer_title"),
                    "is_director": rel.get("is_director"),
                    "is_ten_percent_owner": rel.get("is_ten_percent_owner"),
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
        for col in ["shares","price_per_share","total_value","shares_owned_following"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        for col in ["filed_at","transaction_date","period_of_report"]:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df
