# structure
# Every command is defined once here.
# No Click decorators, no) parser logic.

from .cli_handlers import (
    handle_fetch_insider_tx,
    handle_fetch_exchange_mapping,
    handle_build_dataset,
    handle_plot_amount_assets_acquired_disposed,
    handle_plot_distribution_trans_codes,
    handle_plot_n_most_companies_bs,
    handle_plot_n_most_companies_bs_by_person,
    handle_plot_acquired_disposed_line_chart_ticker,
    handle_plot_sector_statistics,
)

COMMON_PLOT_OPTIONS = [
    ("--ticker", {"default": "*", "help": "Lucene query string"}),
    ("--start", {"required": True, "help": "Start date YYYY-MM-DD"}),
    ("--end", {"required": True, "help": "End date YYYY-MM-DD"}),
    ("--save", {"is_flag": True, "default": False, "help": "Save plot"}),
    ("--outpath", {"required": False, "help": "Path if saving"}),
    ("--show", {"is_flag": True, "help": "Show plot"}),
]

COMMANDS = {
    # --------------- ETL COMMANDS ----------------
    "fetch_insider_tx": {
        "handler": handle_fetch_insider_tx,
        "help": "Force fetch insider trades tx",
        "options": [
            ("--ticker", {"default": "*"}),
            ("--start", {"required": True}),
            ("--end", {"required": True}),
        ],
    },
    "fetch_exchange_mapping": {
        "handler": handle_fetch_exchange_mapping,
        "help": "Force fetch exchange mapping",
        "options": [
        ],
    },
    "build-dataset": {
        "handler": handle_build_dataset,
        "help": "Force run insider trades pipeline",
        "options": [
            ("--rawinput")
        ],
    },

    # ─────────── PLOT GROUP ───────────
    "plot.amount_assets_acquired_disposed": {
        "callback": handle_plot_amount_assets_acquired_disposed,
        "help": "Plot amount of assets acquired/disposed",
        "options": COMMON_PLOT_OPTIONS,
    },
    "plot.distribution_trans_codes": {
        "callback": handle_plot_distribution_trans_codes,
        "help": "Plot distribution of transaction codes",
        "options": COMMON_PLOT_OPTIONS,
    },
    "plot.n_most_companies_bs": {
        "callback": handle_plot_n_most_companies_bs,
        "help": "Plot top N companies bought/sold",
        "options": COMMON_PLOT_OPTIONS + [
            ("--year", {"required": True, "type": int, "help": "Year to analyze"}),
            ("--n", {"default": 15, "type": int, "help": "Number of companies"}),
        ],
    },
    "plot.n_most_companies_bs_by_person": {
        "callback": handle_plot_n_most_companies_bs_by_person,
        "help": "Plot top N companies bought/sold by person",
        "options": COMMON_PLOT_OPTIONS + [
            ("--year", {"required": True, "type": int}),
            ("--n", {"default": 15, "type": int}),
        ],
    },
    "plot.acquired_disposed_line_chart_ticker": {
        "callback": handle_plot_acquired_disposed_line_chart_ticker,
        "help": "Plot acquired/disposed line chart per ticker",
        "options": COMMON_PLOT_OPTIONS + [
            ("--ticker", {"required": True, "help": "Ticker symbol"}),
        ],
    },
    "plot.sector_statistics": {
        "callback": handle_plot_sector_statistics,
        "help": "Plot sector statistics",
        "options": COMMON_PLOT_OPTIONS,
    },
}
