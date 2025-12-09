# structure
# Every command is defined once here.
# No Click decorators, no parser logic.

COMMON_PLOT_OPTIONS = [
    ("--query", {"default": "*:*", "help": "Lucene query string"}),
    ("--start", {"required": True, "help": "Start date YYYY-MM-DD"}),
    ("--end", {"required": True, "help": "End date YYYY-MM-DD"}),
    ("--save", {"is_flag": True, "default": False, "help": "Save plot"}),
    ("--outpath", {"required": False, "help": "Path if saving"}),
    ("--show", {"is_flag": True, "help": "Show plot"}),
]

COMMANDS = {
    "build-dataset": {
        "handler": "build_dataset",
        "help": "Build insider trades dataset",
        "options": [
            ("--query", {"default": "*:*"}),
            ("--start", {"required": True}),
            ("--end", {"required": True}),
            ("--output", {"default": "out/insider_trades.csv"}),
        ],
    },

    # --------------- PLOT COMMANDS ----------------
    "plot.amount-assets-acquired-disposed": {
        "handler": "do_plot_amount_assets_acquired_disposed",
        "help": "Plot amount assets acquired/disposed",
        "options": COMMON_PLOT_OPTIONS,
    },

    "plot.distribution-trans-codes": {
        "handler": "do_plot_distribution_trans_codes",
        "help": "Plot distribution of transaction codes",
        "options": COMMON_PLOT_OPTIONS,
    },

    "plot.n-most-companies-bs": {
        "handler": "do_plot_n_most_companies_bs",
        "help": "Plot N most companies bought/sold",
        "options": COMMON_PLOT_OPTIONS + [
            ("--year", {"required": True, "type": int}),
            ("--n", {"default": 15, "type": int}),
        ],
    },

    "plot.n-most-companies-bs-by-person": {
        "handler": "do_plot_n_most_companies_bs_by_person",
        "help": "Plot N companies bought/sold grouped by person",
        "options": COMMON_PLOT_OPTIONS + [
            ("--year", {"required": True, "type": int}),
            ("--n", {"default": 15, "type": int}),
        ],
    },

    "plot.acquired-disposed-line-chart-ticker": {
        "handler": "do_plot_acquired_disposed_line_chart",
        "help": "Plot acquired/disposed line chart per ticker",
        "options": COMMON_PLOT_OPTIONS + [
            ("--ticker", {"required": True}),
        ],
    },

    "plot.sector-statistics": {
        "handler": "do_plot_sector_statistics",
        "help": "Plot sector statistics",
        "options": COMMON_PLOT_OPTIONS,
    },
}
