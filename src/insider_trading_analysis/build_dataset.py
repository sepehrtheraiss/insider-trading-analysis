import argparse
from config import Config
from controllers.core_controller import CoreController

def main():
    ap = argparse.ArgumentParser(description="Build insider trades dataset via SEC-API")
    ap.add_argument("--query", default="*:*", help="Lucene query, e.g. issuer.tradingSymbol:TSLA OR issuer.tradingSymbol:OXY")
    ap.add_argument("--start", required=True, help="Start date YYYY-MM-DD for filedAt range")
    ap.add_argument("--end", required=True, help="End date YYYY-MM-DD for filedAt range")
    ap.add_argument("-o","--output", default="out/insider_trades.csv", help="CSV output file")
    args = ap.parse_args()
    conf = Config()
    ctrl = CoreController(conf)
    ctrl.run_cmd(args)
    

if __name__ == "__main__":
    main()
