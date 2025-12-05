from insider_trading.pipeline import InsiderTradingPipeline
from config.insider_trading_config import InsiderTradingConfig
from db.db import DB
from utils.logger import Logger

class OrchestratorPipeline:
    def __init__(self):
        self.log = Logger(self.__class__.__name__)

        # Create subpipelines with config injected
        self.insider_pipeline = InsiderTradingPipeline(InsiderTradingConfig(), DB())
        #self.ohlc_pipeline = OhlcPipeline(config=self.ohlc_config)

    def run(self):
        self.log.info("Starting Orchestrator...")
        self.insider_pipeline.run()
        #self.ohlc_pipeline.run()
        self.log.info("All pipelines completed.")

if __name__ == "__main__":
    pipeline = OrchestratorPipeline()
    pipeline.run()
