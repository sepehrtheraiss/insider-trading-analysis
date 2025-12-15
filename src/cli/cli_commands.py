# structure
# Every command is defined once here.
# No Click decorators, no parser logic.

from cli.cli_handlers import (
    handle_fetch_insider_tx,
    handle_fetch_exchange_mapping,
    handle_build_dataset,
    handle_plot_amount_assets,
    handle_plot_distribution_codes,
    handle_plot_n_companies,
    handle_plot_n_companies_reporter,
    handle_plot_line_chart,
    handle_plot_sector_stats,
)

# Common options for plot commands
COMMON_PLOT_OPTIONS = [
    ("--ticker", {"default": None, "help": "Ticker or * for all"}),
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
        "help": "Force fetch insider trading transactions",
        "options": [
            ("--ticker", {"default": "*", "help": "Ticker or * for all"}),
            ("--start", {"required": True, "help": "Start date YYYY-MM-DD"}),
            ("--end", {"required": True, "help": "End date YYYY-MM-DD"}),
        ],
    },

    "fetch_exchange_mapping": {
        "handler": handle_fetch_exchange_mapping,
        "help": "Force fetch exchange mapping",
        "options": [],
    },

    "build-dataset": {
        "handler": handle_build_dataset,
        "help": "Run pipeline on an existing raw file",
        "options": [
            # NOTE: flag uses dash so Click maps it to raw_path
            ("--raw-path", {"required": True, "help": "Path to raw insider tx JSON/NDJSON"}),
        ],
    },

    # ─────────── PLOT GROUP ───────────
    "plot.amount_assets_acquired_disposed": {
        "handler": handle_plot_amount_assets,
        "help": "Plot amount of assets acquired/disposed",
        "options": COMMON_PLOT_OPTIONS,
    },

    "plot.distribution_trans_codes": {
        "handler": handle_plot_distribution_codes,
        "help": "Plot distribution of transaction codes",
        "options": COMMON_PLOT_OPTIONS,
    },

    "plot.n_most_companies_bs": {
        "handler": handle_plot_n_companies,
        "help": "Plot top N companies bought/sold",
        "options": COMMON_PLOT_OPTIONS + [
            ("--n", {"default": 15, "type": int, "help": "Number of companies"}),
        ],
    },

    "plot.n_most_companies_bs_by_reporter": {
        "handler": handle_plot_n_companies_reporter,
        "help": "Plot top N companies bought/sold by reporter",
        "options": COMMON_PLOT_OPTIONS + [
            ("--n", {"default": 15, "type": int}),
        ],
    },

    "plot.acquired_disposed_line_chart_ticker": {
        "handler": handle_plot_line_chart,
        "help": "Plot acquired/disposed line chart per ticker",
        # handler signature already has (ticker, start, end, save, outpath, show)
        # so COMMON_PLOT_OPTIONS is enough – no need to add a second --ticker
        "options": COMMON_PLOT_OPTIONS + [
            ("--reporter", {"help": "buy/sold from reporter on ticker"}),
        ],
    },

    "plot.sector_statistics": {
        "handler": handle_plot_sector_stats,
        "help": "Plot sector statistics",
        "options": COMMON_PLOT_OPTIONS,
    },
}
