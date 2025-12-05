
import yfinance as yf

class YahooFinanceSource:
    def __init__(self):
        pass

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