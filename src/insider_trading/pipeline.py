# insider_pipeline.py
from datetime import datetime, timedelta
from insider_trading.tasks.exchange_mapping_task import ExchangeMappingTask
from insider_trading.tasks.insider_transactions_task import InsiderTransactionsTask
from utils.logger import Logger

class InsiderTradingPipeline:
    """Orchestrates refresh-if-stale ingestion of insider data."""

    MAPPING_REFRESH_DAYS = 30
    TRANSACTION_REFRESH_DAYS = 7

    def __init__(self, config, db):
        self.db = db
        self.mapping_task = ExchangeMappingTask(config, db)
        self.transactions_task = InsiderTransactionsTask(config, db)
        self.log = Logger(self.__class__.__name__)

    def mapping_is_stale(self):
        last = self.db.last_updated("exchange_mapping")
        return not last or (datetime.utcnow() - last).days >= self.MAPPING_REFRESH_DAYS

    def transactions_are_stale(self):
        last = self.db.last_updated("insider_transactions")
        return not last or (datetime.utcnow() - last).days >= self.TRANSACTION_REFRESH_DAYS

    def run(self):
        self.log.info("=== Insider Pipeline Start ===")

        if self.mapping_is_stale():
            self.log.info("[MAPPING] Stale → refreshing.")
            self.mapping_task.run()
        else:
            self.log.info("[MAPPING] Fresh → skipping.")

        if self.transactions_are_stale():
            self.log.info("[INSIDERS] Stale → refreshing.")
            self.transactions_task.run()
        else:
            self.log.info("[INSIDERS] Fresh → skipping.")

        self.log.info("=== Insider Pipeline Complete ===")
