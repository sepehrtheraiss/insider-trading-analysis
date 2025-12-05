# run_insider_trading_pipeline.py
from insider_trading.pipeline import InsiderTradingPipeline
from db.db import DB

if __name__ == "__main__":
    pipeline = InsiderTradingPipeline(DB())
    pipeline.run()
