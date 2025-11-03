from typing import Dict, Any, Iterable
from .base_client import ApiBaseClient, HttpBaseClient
from sec_api import InsiderTradingApi
from .config import DEFAULT_PAGE_SIZE # needs work
import time
import pandas as pd

class SecClient:
    def __init__(self, base_url:str='', api_key:str='', timeout: int=0):
        self.api_client = SecApiClient(api_key, timeout)
        self.http_client = SecHttpClient(base_url, api_key, timeout)


class SecApiClient(ApiBaseClient):
    def __init__(self, api_key: str='', api_timeout: int=0):
        super().__init__(key=api_key, timeout=api_timeout)
        self._insider_trading = InsiderTradingApi(api_key)
    
    def fetch_transactions(
        self,
        query_string: str,
        start: str,
        end: str,
        size: int = DEFAULT_PAGE_SIZE,
        sleep_seconds: float = 0.2,
        sort_desc: bool = True,
    ) -> Iterable[Dict[str, Any]]:
        """
        Streams insider transactions (Forms 3/4/5) that match query_string and filedAt in [start, end].
        Dates are YYYY-MM-DD. Uses SEC-API InsiderTradingApi with pagination.
        e.g.
            query_string = "issuer.tradingSymbol:TSLA"
            insider_trades_sample = insiderTradingApi.get_data({
            "query": {"query_string": {"query": query_string}},
            "from": "0",
            "size": "2",
            "sort": [{ "filedAt": { "order": "desc" } }]
            })
        """
        frm = 0
        sort = [{ "filedAt": { "order": "desc" if sort_desc else "asc" } }]
        data = 1
        while data:
            payload = {
                "query": {"query_string": {"query": f"({query_string}) AND filedAt:[{start} TO {end}]"}},
                "from": str(frm),
                "size": str(size),
                "sort": sort,
            }
            # returns dict with 'transactions'
            data = self._insider_trading.get_data(payload)
            txs = data.get("transactions", []) #or [] # what ?
            if not txs:
                break
            for t in txs:
                yield t
            frm += size
            # needs work
            time.sleep(sleep_seconds)

class SecHttpClient(HttpBaseClient):
    def __init__(self, base_url='', api_key=None, timeout=10):
        super().__init__(base_url, api_key, timeout)
        
    def load_exchange_mapping(self, exchanges=( "nasdaq", "nyse" )) -> pd.DataFrame:
        """
        Fetch company metadata (ticker, sector, industry, exchange) from SEC-API Mapping endpoints.
        Returns a DataFrame with columns: issuerTicker, cik, exchange, sector, industry, category, name
        """
        MAPPING_ENDPOINT = "mapping/exchange/{exchange}?token={key}"
        frames = []
        for ex in exchanges:
            endpoint = MAPPING_ENDPOINT.format(exchange=ex,key=self.api_key)
            data = self.get(endpoint)
            df = pd.DataFrame(data)
            if df.empty:
                continue
            df.drop(["cusip","sic","famaSector","famaIndustry","id", "currency", "location"], axis=1, inplace=True)
            df.rename(columns={"ticker": "issuerTicker"}, inplace=True)
            frames.append(df)
            #keep = ["issuerTicker","cik","exchange","sector","industry","category","name"]
            #frames.append(df[keep])
        if not frames:
            return pd.DataFrame(columns=["issuerTicker","cik","exchange","sector","industry","category","name"])
        out = pd.concat(frames).drop_duplicates("issuerTicker")
        return out