# business logic
import re
import pandas as pd
import yfinance as yf
from typing import Optional
import click
import json
from pathlib import Path
from dateutil.parser import parse

from analytics.analysis import (companies_bs_in_period,
                                companies_bs_in_period_by_reporter,
                                distribution_by_codes, sector_stats_by_year,
                                total_sec_acq_dis_day)
from analytics.plots import (plot_amount_assets_acquired_disposed,
                             plot_distribution_trans_codes, plot_line_chart,
                             plot_n_most_companies_bs,
                             plot_n_most_companies_bs_by_reporter,
                             plot_sector_stats)
from config.insider_trading_config import InsiderTradingConfig
from config.settings import settings
from db.etl_db import ETLDatabase
from db.repository import InsiderRepository
from db.sql_workflow import answer_question_with_sql
from insider_trading.extract.sources.insider_api_source import InsiderApiSource
from insider_trading.pipeline import InsiderTradingPipeline
from utils.logger import Logger
from utils.utils import iterate_months
from writers.raw_writer import RawWriter

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
    #config = InsiderTradingConfig()
    src = InsiderApiSource(settings.base_url, settings.sec_api_key)
    log.info(f"[TRANSACTIONS] Running for window: {start} → {end} (query={query})")
    raw = list(src.fetch_insider_transactions(query, start, end))

    log.info(f"[EXTRACT] Raw filing records = {len(raw)}")

    raw_writer = RawWriter(directory="data/raw")
    raw_path = raw_writer.save(f"insider_transactions_{start}_{end}", raw)
    log.info(f"[RAW] Saved → {raw_path}")

def handle_fetch_exchange_mapping():
    """force fetch exchange mapping"""
    src = InsiderApiSource(settings.base_url, settings.sec_api_key)
    log.info("[TRANSACTIONS] Running for exchange mapping")
    raw = list(src.fetch_exchange_mapping())

    log.info(f"[EXTRACT] Raw filing records = {len(raw)}")

    raw_writer = RawWriter(directory="data/raw")
    raw_path = raw_writer.save("exchange_mapping",raw)
    log.info(f"[RAW] Saved → {raw_path}")

def handle_build_dataset(raw_path: str):
    """force run pipeline on raw_path"""
    config = settings 
    config.test_mode_tx = True
    config.test_mode_map = True
    config.test_path_map = 'exchange_mapping.json'
    db = ETLDatabase()
    _path = Path("data/raw/"+raw_path)
    if _path.exists() and _path.is_dir():
        files = _path.glob("insider_transactions_*.json")
        f_names = sorted([f.name for f in files])
        for name in f_names:
            config.test_path_tx = name
            InsiderTradingPipeline(config, db).run()
            if config.test_mode_map: 
                config.test_mode_map = False
    else:
        config.test_path_tx = raw_path
        InsiderTradingPipeline(config, db).run()
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
    acquired, disposed = companies_bs_in_period_by_reporter(df, start, end, ticker)

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

def handle_plot_line_chart(ticker, reporter, start, end, save, outpath, show):
    db = InsiderRepository()
    df = db.get_transactions(start, end)
    #start_date = datetime.strptime(start, "%Y-%m-%d").date()
    start_date = parse(start).date()
    #end_date = datetime.strptime(end, "%Y-%m-%d").date()
    end_date = parse(end).date()
    ticker_filter = (df["issuer_ticker"]==ticker) & \
    (df["period_of_report"].dt.date >=start_date) & \
    (df["period_of_report"].dt.date <=end_date)
    def name_tokens(name: str) -> set[str]:
        if not isinstance(name, str):
            return set()
        #removes punctuation, commas, dots, numbers
        return set(re.findall(r"[a-z]+", name.casefold()))

    if reporter is not None:
        target_tokens = name_tokens(reporter)
        ticker_filter &= df["reporter"].apply(
            lambda x: target_tokens.issubset(name_tokens(x))
        )
    ticker_acquired = df[ticker_filter].groupby("period_of_report").agg({
        "total_value": "sum",
        "acquired_disposed": "first"
    })
    
    # remove time format 2022-03-14 00:00:00+00:00 -> 2022-03-14
    ticker_acquired.index = ticker_acquired.index.tz_convert(None)
    ticker_acquired.index.names = ['Date']

    ohcl = yf.download(ticker, start=start_date.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"), interval='1d', multi_level_index=False)
    ohcl.columns = ohcl.columns.str.lower()
    ticker_all = ohcl.join(ticker_acquired, how="outer")
    # forward-fill OHLC values
    # Missing values in these columns only happen on non-trading days (holidays/weekends).
    ticker_all[["open", "high", "low", "close", "volume"]].ffill()
    plot_line_chart(
        ticker_all,
        ticker=ticker,
        save=save,
        outpath=outpath,
        show=show,
        start=start,
        end=end,
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


def handle_answer_question_with_sql(
    question: str,
    pretty: bool,
    output: Optional[str],
    show_sql: bool,
    show_explain: bool,
) -> None:
    """
    question: str
        Natural-language question to be translated into SQL and executed.
    pretty: bool
        Pretty-print JSON output.
    output: Optional[str]
        If provided, write result JSON to this path.
    show_sql: bool
        If true, include generated SQL in output.
    show_explain: bool
        If true, include EXPLAIN ANALYZE text in output (can be large).
    """
    result = answer_question_with_sql(question)

    # Trim what we print unless user asked for it
    payload = {
        "question": result.get("question"),
        "rows": result.get("rows", []),
        "optimized": result.get("optimized", {}),
    }
    if show_sql:
        payload["sql"] = result.get("sql")
        # include final SQL if present in optimized
        if isinstance(payload["optimized"], dict) and "improved_sql" in payload["optimized"]:
            payload["final_sql"] = payload["optimized"]["improved_sql"]

    if show_explain:
        payload["explain"] = result.get("explain")

    text = json.dumps(payload, indent=2 if pretty else None, default=str)

    if output:
        with open(output, "w", encoding="utf-8") as f:
            f.write(text)
        click.echo(f"Wrote output to: {output}")
    else:
        click.echo(text)
