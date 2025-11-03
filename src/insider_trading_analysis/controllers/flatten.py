from typing import Dict, Any, Iterable, List
import pandas as pd

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
      filedAt, periodOfReport, issuerTicker, issuerCik, issuerName,
      reporter, reporterCik, isOfficer, officerTitle, isDirector, isTenPercentOwner,
      table, code, acquiredDisposed, transactionDate, shares, pricePerShare,
      sharesOwnedFollowing, dollarValue, is10b5_1, documentType
    """
    records: list[dict] = []
    for t in transactions:
        issuer = (t.get("issuer") or {})
        ro = (t.get("reportingOwner") or {})
        rel = (ro.get("relationship") or {})
        filed_at = t.get("filedAt")
        period = t.get("periodOfReport")
        doc_type = t.get("documentType")

        fnotes = t.get("footnotes") or []
        fn_text = _footnotes_text(fnotes)
        is_10b5 = "10b5-1" in fn_text or "10b5–1" in fn_text or "Rule 10b5" in fn_text

        for table_key, label in (("nonDerivativeTable","non-derivative"), ("derivativeTable","derivative")):
            for row in _extract_table_rows(t, table_key):
                coding = row.get("coding") or {}
                amts = row.get("amounts") or {}
                post = row.get("postTransactionAmounts") or {}
                own = row.get("ownershipNature") or {}

                shares = amts.get("shares")
                price = amts.get("pricePerShare")
                code = (coding.get("code") or None)
                ad = amts.get("acquiredDisposedCode")
                tx_date = row.get("transactionDate")
                post_sh = post.get("sharesOwnedFollowingTransaction")

                # Compute a dollar value if possible
                try:
                    dollar_value = float(shares) * float(price) if shares is not None and price is not None else None
                except Exception:
                    dollar_value = None

                records.append({
                    "filedAt": filed_at,
                    "periodOfReport": period,
                    "documentType": doc_type,
                    "issuerTicker": issuer.get("tradingSymbol"),
                    "issuerCik": issuer.get("cik"),
                    "issuerName": issuer.get("name"),
                    "reporter": ro.get("name"),
                    "reporterCik": ro.get("cik"),
                    "isOfficer": rel.get("isOfficer"),
                    "officerTitle": rel.get("officerTitle"),
                    "isDirector": rel.get("isDirector"),
                    "isTenPercentOwner": rel.get("isTenPercentOwner"),
                    "table": label,
                    "code": code,
                    "acquiredDisposed": ad,
                    "transactionDate": tx_date,
                    "shares": shares,
                    "pricePerShare": price,
                    "sharesOwnedFollowing": post_sh,
                    "dollarValue": dollar_value,
                    "is10b5_1": bool(is_10b5),
                })
    df = pd.DataFrame.from_records(records)
    # Normalize types
    if not df.empty:
        for col in ["shares","pricePerShare","dollarValue","sharesOwnedFollowing"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        for col in ["filedAt","transactionDate","periodOfReport"]:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df
