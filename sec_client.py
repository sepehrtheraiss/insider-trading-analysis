import time
from typing import Dict, Any, Iterable
from config import require_api_key

def _insider_api():
    from sec_api import InsiderTradingApi
    key = require_api_key()
    return InsiderTradingApi(key)

DEFAULT_PAGE_SIZE = 50

def fetch_transactions(
    query_string: str,
    start: str,
    end: str,
    size: int = DEFAULT_PAGE_SIZE,
    max_pages: int = 100,
    sleep_seconds: float = 0.2,
    sort_desc: bool = True,
) -> Iterable[Dict[str, Any]]:
    """
    Streams insider transactions (Forms 3/4/5) that match query_string and filedAt in [start, end].
    Dates are YYYY-MM-DD. Uses SEC-API InsiderTradingApi with pagination.
    """
    api = _insider_api()
    frm = 0
    pages = 0
    sort = [{ "filedAt": { "order": "desc" if sort_desc else "asc" } }]
    while pages < max_pages:
        payload = {
            "query": {"query_string": {"query": f"({query_string}) AND filedAt:[{start} TO {end}]"}},
            "from": str(frm),
            "size": str(size),
            "sort": sort,
        }
        data = api.get_data(payload)  # returns dict with 'transactions'
        txs = data.get("transactions", []) or []
        if not txs:
            break
        for t in txs:
            yield t
        frm += size
        pages += 1
        time.sleep(sleep_seconds)
