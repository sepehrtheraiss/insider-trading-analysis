# business logic
from config.insider_trading_config import InsiderTradingConfig
from db.etl_db import ETLDatabase# adjust to your actual DB factory
from insider_trading.pipeline import InsiderTradingPipeline
from insider_trading.extract.sources.insider_api_source import InsiderApiSource
from writers.raw_writer import RawWriter

from analytics.plot_service import PlotService  # or analytics.plotting.* tasks
from utils.logger import Logger


log = Logger(__name__)


def _make_config_and_db():
    config = InsiderTradingConfig()
    db = None#get_db(config)  # or however you create your DB/session
    return config, db


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
# You can later refactor these to dedicated plot Task classes
# ─────────────────────────

def _require_outpath_if_saved(save: bool, outpath: str | None):
    if save and not outpath:
        from click import UsageError
        raise UsageError("--save requires --outpath")
    return outpath


def handle_plot_amount_assets_acquired_disposed(
    ticker: str,
    start: str,
    end: str,
    save: bool,
    outpath: str | None,
    show: bool,
):
    config, db = _make_config_and_db()
    outpath = _require_outpath_if_saved(save, outpath)

    svc = PlotService(config=config, db=db)
    return svc.plot_amount_assets_acquired_disposed(
        ticker=ticker,
        start=start,
        end=end,
        save=save,
        outpath=outpath,
        show=show,
    )


def handle_plot_distribution_trans_codes(
    ticker: str,
    start: str,
    end: str,
    save: bool,
    outpath: str | None,
    show: bool,
):
    config, db = _make_config_and_db()
    outpath = _require_outpath_if_saved(save, outpath)

    svc = PlotService(config=config, db=db)
    return svc.plot_distribution_trans_codes(
        ticker=ticker,
        start=start,
        end=end,
        save=save,
        outpath=outpath,
        show=show,
    )


def handle_plot_n_most_companies_bs(
    ticker: str,
    start: str,
    end: str,
    year: int,
    n: int,
    save: bool,
    outpath: str | None,
    show: bool,
):
    config, db = _make_config_and_db()
    outpath = _require_outpath_if_saved(save, outpath)

    svc = PlotService(config=config, db=db)
    return svc.plot_n_most_companies_bs(
        ticker=ticker,
        start=start,
        end=end,
        year=year,
        n=n,
        save=save,
        outpath=outpath,
        show=show,
    )


def handle_plot_n_most_companies_bs_by_person(
    ticker: str,
    start: str,
    end: str,
    year: int,
    n: int,
    save: bool,
    outpath: str | None,
    show: bool,
):
    config, db = _make_config_and_db()
    outpath = _require_outpath_if_saved(save, outpath)

    svc = PlotService(config=config, db=db)
    return svc.plot_n_most_companies_bs_by_person(
        ticker=ticker,
        start=start,
        end=end,
        year=year,
        n=n,
        save=save,
        outpath=outpath,
        show=show,
    )


def handle_plot_acquired_disposed_line_chart_ticker(
    ticker: str,
    start: str,
    end: str,
    ticker: str,
    save: bool,
    outpath: str | None,
    show: bool,
):
    config, db = _make_config_and_db()
    outpath = _require_outpath_if_saved(save, outpath)

    svc = PlotService(config=config, db=db)
    return svc.plot_acquired_disposed_line_chart_ticker(
        ticker=ticker,
        start=start,
        end=end,
        ticker=ticker,
        save=save,
        outpath=outpath,
        show=show,
    )


def handle_plot_sector_statistics(
    ticker: str,
    start: str,
    end: str,
    save: bool,
    outpath: str | None,
    show: bool,
):
    config, db = _make_config_and_db()
    outpath = _require_outpath_if_saved(save, outpath)

    svc = PlotService(config=config, db=db)
    return svc.plot_sector_statistics(
        ticker=ticker,
        start=start,
        end=end,
        save=save,
        outpath=outpath,
        show=show,
    )
