# plot_service.py
from ohlc.yahoo_fetcher import YahooFetcher
#from ohlc.ohlc_cleaner import OhlcCleaner
from ohlc.merge_for_plot import merge_for_plot

from .analysis import (
    total_sec_acq_dis_day,
    companies_bs_in_period,
    companies_bs_in_period_by_person,
    distribution_by_codes,
    sector_stats_by_year,
)

class PlotDatasetBuilder:
    """Pure business logic for building datasets used in plotting."""

    def build_amount_assets(self, df):
        return total_sec_acq_dis_day(df)

    def build_distribution_codes(self, df):
        return distribution_by_codes(df)

    def build_companies_bs(self, df, year):
        return companies_bs_in_period(df, year)

    def build_companies_by_person(self, df, year):
        return companies_bs_in_period_by_person(df, year)

    def build_sector_stats(self, df):
        return sector_stats_by_year(df)

class PlotService:
    """Fetch OHLC → load insiders → merge → plot."""

    def __init__(self, db):
        self.db = db

    def plot(self, ticker, start, end):
        raw = YahooFetcher().fetch(ticker, start, end)
       # ohlc = OhlcCleaner().normalize(raw)

        insiders = self.db.get_insider_data(ticker, start, end)

      #  merged = merge_for_plot(ohlc, insiders)

       # # TODO: Actually plot
        #return merged
