from typing import Dict, Any, Iterable
import time
import pandas as pd
import yfinance as yf

from ..library_client import LibraryClient
from ..http_client import HttpClient

DEFAULT_PAGE_SIZE = 50
class SecClient:
    def __init__(self, base_url:str='', api_key:str='', lib_client=None, http_client=None):
        self._libClient = lib_client or LibraryClient(api_key)
        self._httpClient = http_client or HttpClient(base_url, api_key)

    def fetch_insider_transactions(
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
                # returns dict_keys(['total', 'transactions']) 
                # total -> <class 'dict'>
                # transactions -> <class 'list'> -> <class 'dict'>
                '''
                dict_keys(['id', 'accessionNo', 'filedAt', 'schemaVersion',
                           'documentType', 'periodOfReport', 'notSubjectToSection16',
                           'issuer', 'reportingOwner', 'nonDerivativeTable',
                           'derivativeTable', 'footnotes', 'ownerSignatureName',
                           'ownerSignatureNameDate'])
                '''
                data = self._libClient.fetch('InsiderTradingApi', 'get_data',payload)
                txs = data.get("transactions", [])
                if not txs:
                    break
                for t in txs:
                    yield t
                frm += size
                # needs work
                time.sleep(sleep_seconds)
            yield {}

     
    def fetch_exchange_mapping(self, exchanges=( "nasdaq", "nyse" )) -> pd.DataFrame:
        """
        Fetch company metadata (ticker, sector, industry, exchange) from SEC-API Mapping endpoints.
        Returns a DataFrame with columns: issuerTicker, cik, exchange, sector, industry, category, name
        """
        MAPPING_ENDPOINT = "mapping/exchange/{exchange}?token={key}"
        frames = []
        for ex in exchanges:
            endpoint = MAPPING_ENDPOINT.format(exchange=ex,key=self._httpClient.api_key)
            # dict_keys(['name', 'ticker', 'cik', 'cusip', 'exchange', 'isDelisted', 'category',
            #            'sector', 'industry', 'sic', 'sicSector', 'sicIndustry', 'famaSector',
            #            'famaIndustry', 'currency', 'location', 'id'])
            data = self._httpClient.fetch(endpoint)
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

    def fetch_ticker_ohlc(self, ticker, start, end, interval, period=None) -> pd.DataFrame: #Optional[pandas.core.frame.DataFrame]:
        """
        :Parameters:
            tickers : str, list
                List of tickers to download
            period : str
                Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
                Default: 1mo
                Either Use period parameter or use start and end
            interval : str
                Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
                Intraday data cannot extend last 60 days
            start: str
                Download start date string (YYYY-MM-DD) or _datetime, inclusive.
                Default is 99 years ago
                E.g. for start="2020-01-01", the first data point will be on "2020-01-01"
            end: str
                Download end date string (YYYY-MM-DD) or _datetime, exclusive.
                Default is now
                E.g. for end="2023-01-01", the last data point will be on "2022-12-31"
        """
        return yf.download(ticker, period=period, start=start, end=end, interval=interval, multi_level_index=False)