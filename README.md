
# Insider Trading Analysis (Python)

Build a local dataset of SEC Forms 3/4/5 via SEC-API, clean & enrich it with exchange/sector information, and run basic analyses/plots.

## Quickstart

1) **Python 3.10+** recommended

2) Install deps:
```bash
pip install -r requirements.txt
```

3) Set your **SEC-API key**:
```bash
export SEC_API_KEY="YOUR_KEY_HERE"
```

4) Build a dataset (example: 2023-01-01 through 2025-01-01, all tickers):
```bash
python build_dataset.py --start 2023-01-01 --end 2025-01-01 -o out/insider_trades.csv
```

Filter to a ticker or two with Lucene query:
```bash
python build_dataset.py --query "issuer.tradingSymbol:TSLA OR issuer.tradingSymbol:AAPL" --start 2023-01-01 --end 2025-01-01 -o out/tsla_aapl.csv
```

5) Summarize & plot:
```bash
python cli.py summary out/insider_trades.csv --outdir out
```

Outputs:
- `out/by_code.png` – total $ value by transaction code (S, P, etc.)
- `out/sector_year.png` – heatmap of sector × year dollar values
- console tables for code summary and top reporters

## Notes

- Uses SEC-API's InsiderTradingApi for filings, and their Mapping API to attach exchange/sector/industry metadata.
- Cleaning removes weird prices, keeps NYSE/NASDAQ by default.
- Flags 10b5‑1 mentions if footnotes contain the string.
