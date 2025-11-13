import argparse, os, sys
import pandas as pd
from views.present import summarize_codes, sector_year_pivot, top_reporters
from views.plots import savefig_bar_by_code, savefig_heatmap_sector_year
from utils.utils import millions_formatter
from utils.config import Config
from controllers.core_controller import CoreController

def handle_build_dataset(args):
    ctrl = CoreController(Config())
    ctrl.build_dataset(args)

def handle_plot_annual_graph(args):
    ctrl = CoreController(Config())
    ctrl.do_plot_annual_graph(args)

def handle_plot_distribution_trans_codes(args):
    ctrl = CoreController(Config())
    ctrl.do_plot_distribution_trans_codes(args)

def handle_n_most_companies_bs(args):
    ctrl = CoreController(Config())
    ctrl.do_plot_n_most_companies_bs(args)

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
    savefig_bar_by_code(df, os.path.join(args.outdir, "by_code.png"))
    savefig_heatmap_sector_year(pv, os.path.join(args.outdir, "sector_year.png"))
    print(f"Charts saved in {args.outdir}/")

def main():
    parser = argparse.ArgumentParser(prog="Insider trading analysis", description="Analyze insider trading data CSV")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # ─────────────── BUILD DATASET COMMAND ───────────────
    build_dataset_parser = subparsers.add_parser("build_dataset", help="Build insider trades dataset via SEC-API")
    build_dataset_parser.add_argument("--query", default="*:*", help="Lucene query, e.g. issuer.tradingSymbol:TSLA")
    build_dataset_parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD for filedAt range")
    build_dataset_parser.add_argument("--end", required=True, help="End date YYYY-MM-DD for filedAt range")
    build_dataset_parser.add_argument("-o","--output", default="out/insider_trades.csv", help="CSV output file")
    build_dataset_parser.set_defaults(func=handle_build_dataset)


    # ─────────────── SUMMARY COMMAND ───────────────
    summary_parser = subparsers.add_parser("summary", help="Summarize codes / reporters / sector-year")
    summary_parser.add_argument("csv", help="Path to insider_trades.csv")
    summary_parser.add_argument("--outdir", default="out", help="Output directory for charts")
    summary_parser.set_defaults(func=handle_summary) 
    
    # ─────────────── PLOT COMMAND ───────────────
    plot_parser = subparsers.add_parser("plot", help="plotter")
    plot_subparsers = plot_parser.add_subparsers(dest="subcommand", required=True)

    # plot_annual_graph 
    annual_graph_parser = plot_subparsers.add_parser("annual_graph", help="plots annual graph")
    annual_graph_parser.add_argument("--query", default="*:*", help="Lucene query, e.g. issuer.tradingSymbol:TSLA")
    annual_graph_parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD for filedAt range")
    annual_graph_parser.add_argument("--end", required=True, help="End date YYYY-MM-DD for filedAt range")
    annual_graph_parser.add_argument("--save", action='store_true', default=False, help="Save plot")
    annual_graph_parser.add_argument("--outpath", required='--save' in sys.argv, help="path for save plot")
    annual_graph_parser.add_argument("--show", action='store_true', required=False, help="Show plot")
    annual_graph_parser.set_defaults(func=handle_plot_annual_graph) 

    # plot_distribution_trans_codes 
    plot_distribution_trans_codes_parser = plot_subparsers.add_parser("distribution_trans_codes", help="plot distribution transaction codes")
    plot_distribution_trans_codes_parser.add_argument("--query", default="*:*", help="Lucene query, e.g. issuer.tradingSymbol:TSLA")
    plot_distribution_trans_codes_parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD for filedAt range")
    plot_distribution_trans_codes_parser.add_argument("--end", required=True, help="End date YYYY-MM-DD for filedAt range")
    plot_distribution_trans_codes_parser.add_argument("--save", action='store_true', default=False, help="Save plot")
    plot_distribution_trans_codes_parser.add_argument("--outpath", required='--save' in sys.argv, help="path for save plot")
    plot_distribution_trans_codes_parser.add_argument("--show", action='store_true', required=False, help="Show plot")
    plot_distribution_trans_codes_parser.set_defaults(func=handle_plot_distribution_trans_codes) 

    # plot_n_most_companies_bs 
    plot_n_most_companies_bs= plot_subparsers.add_parser("n_most_companies_bs", help="plot top n most_companies bought and sold")
    plot_n_most_companies_bs.add_argument("--query", default="*:*", help="Lucene query, e.g. issuer.tradingSymbol:TSLA")
    plot_n_most_companies_bs.add_argument("--start", required=True, help="Start date YYYY-MM-DD for filedAt range")
    plot_n_most_companies_bs.add_argument("--end", required=True, help="End date YYYY-MM-DD for filedAt range")
    plot_n_most_companies_bs.add_argument("--year", required=True, type=int, help="period to analyze")
    plot_n_most_companies_bs.add_argument("--n", required=False, default=15, type=int, help="top n trades")
    plot_n_most_companies_bs.add_argument("--save", action='store_true', default=False, help="Save plot")
    plot_n_most_companies_bs.add_argument("--outpath", required='--save' in sys.argv, help="path for save plot")
    plot_n_most_companies_bs.add_argument("--show", action='store_true', required=False, help="Show plot")
    plot_n_most_companies_bs.set_defaults(func=handle_n_most_companies_bs) 

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
