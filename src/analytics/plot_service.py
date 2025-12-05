# plot_service.py
from ohlc.yahoo_fetcher import YahooFetcher
from ohlc.ohlc_cleaner import OhlcCleaner
from ohlc.merge_for_plot import merge_for_plot


class PlotService:
    """Fetch OHLC → load insiders → merge → plot."""

    def __init__(self, db):
        self.db = db

    def plot(self, ticker, start, end):
        raw = YahooFetcher().fetch(ticker, start, end)
        ohlc = OhlcCleaner().normalize(raw)

        insiders = self.db.get_insider_data(ticker, start, end)

        merged = merge_for_plot(ohlc, insiders)

        # TODO: Actually plot
        return merged
