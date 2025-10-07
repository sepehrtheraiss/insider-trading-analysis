import argparse
import pandas as pd
from sec_client import fetch_transactions
from flatten import normalize_transactions
from mapping import load_exchange_mapping
from clean import attach_mapping, filter_valid_exchanges, remove_price_outliers, finalize

def main():
    ap = argparse.ArgumentParser(description="Build insider trades dataset via SEC-API")
    ap.add_argument("--query", default="*:*", help="Lucene query, e.g. issuer.tradingSymbol:TSLA OR issuer.tradingSymbol:OXY")
    ap.add_argument("--start", required=True, help="Start date YYYY-MM-DD for filedAt range")
    ap.add_argument("--end", required=True, help="End date YYYY-MM-DD for filedAt range")
    ap.add_argument("-o","--output", default="out/insider_trades.csv", help="CSV output file")
    args = ap.parse_args()

    raw_iter = fetch_transactions(args.query, start=args.start, end=args.end)
    df = normalize_transactions(raw_iter)
    if df.empty:
        print("No transactions returned.")
        return

    mapping = load_exchange_mapping()
    df = attach_mapping(df, mapping)
    df = filter_valid_exchanges(df)
    df = remove_price_outliers(df)
    df = finalize(df)
    df.sort_values(["filedAt","issuerTicker"], ascending=[False, True], inplace=True)
    print(f"Rows after cleaning: {len(df)}")
    out = args.output
    df.to_csv(out, index=False)
    print(f"Wrote {out}")

if __name__ == "__main__":
    main()
