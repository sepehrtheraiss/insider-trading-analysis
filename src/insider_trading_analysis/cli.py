import argparse, os
import pandas as pd
from views.present import summarize_codes, sector_year_pivot, top_reporters
from views.plots import bar_by_code, heatmap_sector_year
from utils.utils import millions_formatter
from utils.config import Config
from controllers.core_controller import CoreController

def handle_build_dataset(args):
    ctrl = CoreController(Config())
    ctrl.build_dataset(args)

def handle_summary(args):
    os.makedirs(args.outdir, exist_ok=True)
    df = pd.read_csv(args.csv, parse_dates=["filedAt","periodOfReport","transactionDate"])
    s = summarize_codes(df)
    s['totalValue'] = s['totalValue'].apply(millions_formatter)
    print("\n=== Dollar Value by Code & A/D ===\n", s.head(20).to_string(index=False))
    r = top_reporters(df)
    r['totalValue'] = r['totalValue'].apply(millions_formatter)
    print("\n=== Top reporters ===\n", r.to_string(index=False))
    pv = sector_year_pivot(df)
    bar_by_code(df, os.path.join(args.outdir, "by_code.png"))
    heatmap_sector_year(pv, os.path.join(args.outdir, "sector_year.png"))
    print(f"Charts saved in {args.outdir}/")

def main():
    parser = argparse.ArgumentParser(prog="Insider trading analysis", description="Analyze insider trading data CSV")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # ─────────────── BUILD DATASET COMMAND ───────────────
    build_dataset_parser = subparsers.add_parser("build_dataset", help="Build insider trades dataset via SEC-API")
    #build_dataset_subparsers = build_dataset_parser.add_subparsers(dest="subcommand", required=True)
    build_dataset_parser.add_argument("--query", default="*:*", help="Lucene query, e.g. issuer.tradingSymbol:TSLA")
    build_dataset_parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD for filedAt range")
    build_dataset_parser.add_argument("--end", required=True, help="End date YYYY-MM-DD for filedAt range")
    build_dataset_parser.add_argument("-o","--output", default="out/insider_trades.csv", help="CSV output file")
    build_dataset_parser.set_defaults(func=handle_build_dataset)


    # ─────────────── SUMMARY COMMAND ───────────────
    summary_parser = subparsers.add_parser("summary", help="Summarize codes / reporters / sector-year")
    summary_parser.add_argument("csv", help="Path to iargsider_trades.csv")
    summary_parser.add_argument("--outdir", default="out", help="Output directory for charts")
    summary_parser.set_defaults(func=handle_summary) 

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
