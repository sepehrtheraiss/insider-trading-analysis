# business logic
from analytics.plots import (
    plot_amount_assets_acquired_disposed,
    plot_distribution_trans_codes,
    plot_n_most_companies_bs,
    plot_n_most_companies_bs_by_reporter,
    plot_line_chart,
    plot_sector_stats,
)
from analytics.analysis import (companies_bs_in_period,
                                companies_bs_in_period_by_reporter,
                                distribution_by_codes, sector_stats_by_year,
                                total_sec_acq_dis_day)
from config.insider_trading_config import InsiderTradingConfig
from db.etl_db import ETLDatabase
from db.repository import InsiderRepository
from insider_trading.extract.sources.insider_api_source import InsiderApiSource
from insider_trading.pipeline import InsiderTradingPipeline
from utils.logger import Logger
from writers.raw_writer import RawWriter
import pandas as pd

log = Logger(__name__)
# ─────────────────────────
# ETL HANDLERS
# ─────────────────────────

def handle_fetch_insider_tx(ticker: str, start: str, end: str):
    """force fetch insider trading transactions"""
    if "*" not in ticker:
        query = f"issuer.tradingSymbol:{ticker}"
    else:
        query = "*:*"
    config = InsiderTradingConfig()
    src = InsiderApiSource(config.base_url, config.api_key)
    log.info(f"[TRANSACTIONS] Running for window: {start} → {end} (query={query})")
    raw = list(src.fetch_insider_transactions(query, start, end))

    log.info(f"[EXTRACT] Raw filing records = {len(raw)}")

    raw_writer = RawWriter(directory="data/raw")
    raw_path = raw_writer.save(f"insider_transactions_{ticker}_{start}_{end}", raw)
    log.info(f"[RAW] Saved → {raw_path}")

def handle_fetch_exchange_mapping():
    """force fetch exchange mapping"""
    config = InsiderTradingConfig()
    src = InsiderApiSource(config.base_url, config.api_key)
    log.info("[TRANSACTIONS] Running for exchange mapping")
    raw = list(src.fetch_exchange_mapping())

    log.info(f"[EXTRACT] Raw filing records = {len(raw)}")

    raw_writer = RawWriter(directory="data/raw")
    raw_path = raw_writer.save("exchange_mapping",raw)
    log.info(f"[RAW] Saved → {raw_path}")

def handle_build_dataset(raw_path: str):
    """force run pipeline on raw_path"""
    config = InsiderTradingConfig()
    config.test_mode_tx = True
    config.test_path_tx = raw_path
    InsiderTradingPipeline(config, ETLDatabase()).run()
    return


# ─────────────────────────
# PLOT HANDLERS
# ─────────────────────────

def handle_plot_amount_assets(ticker, start, end, save, outpath, show):
    db = InsiderRepository()
    df = db.get_transactions(start, end)

    # BUSINESS LOGIC (analysis layer)
    dataset = total_sec_acq_dis_day(df)
    dataset.index = dataset.index.normalize()
    acquired_yr = dataset.groupby(pd.Grouper(freq='Y'))['acquired'].sum()
    disposed_yr = dataset.groupby(pd.Grouper(freq='Y'))['disposed'].sum()
    acquired_disposed_yr = pd.merge(acquired_yr, disposed_yr, on='period_of_report', how='outer')

    # RENDERING (plotting layer)
    plot_amount_assets_acquired_disposed(
        acquired_disposed_yr,
        save=save,
        outpath=outpath,
        show=show,
        start=start,
        end=end,
    )

def handle_plot_distribution_codes(ticker, start, end, save, outpath, show):
    db = InsiderRepository()
    df = db.get_transactions(start, end)

    dataset = distribution_by_codes(df)

    plot_distribution_trans_codes(
        dataset,
        start=start,
        end=end,
        save=save,
        outpath=outpath,
        show=show,
    )

def handle_plot_n_companies(ticker, start, end, n, save, outpath, show):
    db = InsiderRepository()
    df = db.get_transactions(start, end)

    acquired, disposed = companies_bs_in_period(df, start, end)

    plot_n_most_companies_bs(
        acquired=acquired,
        disposed=disposed,
        n=n,
        save=save,
        outpath=outpath,
        show=show,
        start=start,
        end=end,
    )

def handle_plot_n_companies_reporter(ticker, start, end, n, save, outpath, show):
    db = InsiderRepository()
    df = db.get_transactions(start, end)

    acquired, disposed = companies_bs_in_period_by_reporter(df, start, end)

    plot_n_most_companies_bs_by_reporter(
        acquired=acquired,
        disposed=disposed,
        n=n,
        save=save,
        outpath=outpath,
        show=show,
        start=start,
        end=end,
    )

def handle_plot_line_chart(ticker, start, end, save, outpath, show):
    db = InsiderRepository()
    df = db.get_ohlc(ticker, start, end)

    plot_line_chart(
        df,
        ticker=ticker,
        save=save,
        outpath=outpath,
        show=show,
    )

def handle_plot_sector_stats(ticker, start, end, save, outpath, show):
    db = InsiderRepository()
    df = db.get_rollup(start, end)

    dataset = sector_stats_by_year(df)

    plot_sector_stats(
        dataset,
        save=save,
        outpath=outpath,
        show=show,
        start=start,
        end=end,
    )