import argparse, sys
from utils.config import Config
from controllers.core_controller import CoreController

def handle_build_dataset(args):
    ctrl = CoreController(Config())
    ctrl.build_dataset(args)

def handle_plot_amount_assets_acquired_disposed(args):
    ctrl = CoreController(Config())
    ctrl.do_plot_amount_assets_acquired_disposed(args)

def handle_plot_distribution_trans_codes(args):
    ctrl = CoreController(Config())
    ctrl.do_plot_distribution_trans_codes(args)

def handle_n_most_companies_bs(args):
    ctrl = CoreController(Config())
    ctrl.do_plot_n_most_companies_bs(args)

def handle_n_most_companies_bs_by_person(args):
    ctrl = CoreController(Config())
    ctrl.do_plot_n_most_companies_bs_by_person(args)

def handle_plot_acquired_disposed_line_chart(args):
    ctrl = CoreController(Config())
    ctrl.do_plot_acquired_disposed_line_chart(args)

def handle_plot_sector_statistics(args):
    ctrl = CoreController(Config())
    ctrl.do_plot_sector_statistics(args)

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
    
    # ─────────────── PLOT COMMAND ───────────────
    plot_parser = subparsers.add_parser("plot", help="plotter")
    plot_subparsers = plot_parser.add_subparsers(dest="subcommand", required=True)

    # plot_annual_graph 
    amount_assets_acquired_disposed_parser = plot_subparsers.add_parser("amount_assets_acquired_disposed", help="plots amount of assets acquired disposed")
    amount_assets_acquired_disposed_parser.add_argument("--query", default="*:*", help="Lucene query, e.g. issuer.tradingSymbol:TSLA")
    amount_assets_acquired_disposed_parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD for filedAt range")
    amount_assets_acquired_disposed_parser.add_argument("--end", required=True, help="End date YYYY-MM-DD for filedAt range")
    amount_assets_acquired_disposed_parser.add_argument("--save", action='store_true', default=False, help="Save plot")
    amount_assets_acquired_disposed_parser.add_argument("--outpath", required='--save' in sys.argv, help="path for save plot")
    amount_assets_acquired_disposed_parser.add_argument("--show", action='store_true', required=False, help="Show plot")
    amount_assets_acquired_disposed_parser.set_defaults(func=handle_plot_amount_assets_acquired_disposed) 

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

    # plot_n_most_companies_bs by person
    plot_n_most_companies_bs_by_person= plot_subparsers.add_parser("n_most_companies_bs_by_person", help="plot top n most_companies bought and sold")
    plot_n_most_companies_bs_by_person.add_argument("--query", default="*:*", help="Lucene query, e.g. issuer.tradingSymbol:TSLA")
    plot_n_most_companies_bs_by_person.add_argument("--start", required=True, help="Start date YYYY-MM-DD for filedAt range")
    plot_n_most_companies_bs_by_person.add_argument("--end", required=True, help="End date YYYY-MM-DD for filedAt range")
    plot_n_most_companies_bs_by_person.add_argument("--year", required=True, type=int, help="period to analyze")
    plot_n_most_companies_bs_by_person.add_argument("--n", required=False, default=15, type=int, help="top n trades")
    plot_n_most_companies_bs_by_person.add_argument("--save", action='store_true', default=False, help="Save plot")
    plot_n_most_companies_bs_by_person.add_argument("--outpath", required='--save' in sys.argv, help="path for save plot")
    plot_n_most_companies_bs_by_person.add_argument("--show", action='store_true', required=False, help="Show plot")
    plot_n_most_companies_bs_by_person.set_defaults(func=handle_n_most_companies_bs_by_person) 

    # plot_line_chart for acquired disposed ticker 
    plot_acquired_disposed_line_chart_ticker= plot_subparsers.add_parser("acquired_disposed_line_chart_ticker", help="plot line chart acquired/disposed by ticker")
    plot_acquired_disposed_line_chart_ticker.add_argument("--query", default="*:*", help="Lucene query, e.g. issuer.tradingSymbol:TSLA")
    plot_acquired_disposed_line_chart_ticker.add_argument("--start", required=True, help="Start date YYYY-MM-DD for filedAt range")
    plot_acquired_disposed_line_chart_ticker.add_argument("--end", required=True, help="End date YYYY-MM-DD for filedAt range")
    plot_acquired_disposed_line_chart_ticker.add_argument("--ticker", required=True, help="ticker symbol to use")
    plot_acquired_disposed_line_chart_ticker.add_argument("--save", action='store_true', default=False, help="Save plot")
    plot_acquired_disposed_line_chart_ticker.add_argument("--outpath", required='--save' in sys.argv, help="path for save plot")
    plot_acquired_disposed_line_chart_ticker.add_argument("--show", action='store_true', required=False, help="Show plot")
    plot_acquired_disposed_line_chart_ticker.set_defaults(func=handle_plot_acquired_disposed_line_chart) 

    # plot sector statistics
    plot_sector_statistics= plot_subparsers.add_parser("sector_statistics", help="sector statistics")
    plot_sector_statistics.add_argument("--query", default="*:*", help="Lucene query, e.g. issuer.tradingSymbol:TSLA")
    plot_sector_statistics.add_argument("--start", required=True, help="Start date YYYY-MM-DD for filedAt range")
    plot_sector_statistics.add_argument("--end", required=True, help="End date YYYY-MM-DD for filedAt range")
    plot_sector_statistics.add_argument("--save", action='store_true', default=False, help="Save plot")
    plot_sector_statistics.add_argument("--outpath", required='--save' in sys.argv, help="path for save plot")
    plot_sector_statistics.add_argument("--show", action='store_true', required=False, help="Show plot")
    plot_sector_statistics.set_defaults(func=handle_plot_sector_statistics) 

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
