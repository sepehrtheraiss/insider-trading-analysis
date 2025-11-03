import argparse, os
import pandas as pd
from views.present import summarize_codes, sector_year_pivot, top_reporters
from views.plots import bar_by_code, heatmap_sector_year

def build_dataset():
    pass
def main():
    ap = argparse.ArgumentParser(prog="insider", description="Analyze insider trading data CSV")
    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_sum = sub.add_parser("summary", help="Summarize codes / reporters / sector-year")
    ap_sum.add_argument("csv", help="Path to insider_trades.csv")
    ap_sum.add_argument("--outdir", default="out", help="Output directory for charts")

    ns = ap.parse_args()
    os.makedirs(ns.outdir, exist_ok=True)

    if ns.cmd == "summary":
        df = pd.read_csv(ns.csv, parse_dates=["filedAt","periodOfReport","transactionDate"])
        s = summarize_codes(df)
        print("\n=== Dollar Value by Code & A/D ===\n", s.head(20).to_string(index=False))
        r = top_reporters(df)
        print("\n=== Top reporters ===\n", r.to_string(index=False))
        pv = sector_year_pivot(df)
        bar_by_code(df, os.path.join(ns.outdir, "by_code.png"))
        heatmap_sector_year(pv, os.path.join(ns.outdir, "sector_year.png"))
        print(f"Charts saved in {ns.outdir}/")

if __name__ == "__main__":
    main()
