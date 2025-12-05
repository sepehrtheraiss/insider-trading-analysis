# insider_pipeline.py
from datetime import datetime, timedelta
from tasks.exchange_mapping_task import ExchangeMappingTask
from tasks.insider_transactions_task import InsiderTransactionsTask
from ..utils.logger import log


class InsiderTradingPipeline:
    """Orchestrates refresh-if-stale ingestion of insider data."""

    MAPPING_REFRESH_DAYS = 30
    TRANSACTION_REFRESH_DAYS = 7

    def __init__(self, db):
        self.db = db
        self.mapping_task = ExchangeMappingTask(db)
        self.transactions_task = InsiderTransactionsTask(db)

    def mapping_is_stale(self):
        last = self.db.last_updated("exchange_mapping")
        return not last or (datetime.utcnow() - last).days >= self.MAPPING_REFRESH_DAYS

    def transactions_are_stale(self):
        last = self.db.last_updated("insider_transactions")
        return not last or (datetime.utcnow() - last).days >= self.TRANSACTION_REFRESH_DAYS

    def run(self):
        log("=== Insider Pipeline Start ===")

        if self.mapping_is_stale():
            log("[MAPPING] Stale → refreshing.")
            self.mapping_task.run()
        else:
            log("[MAPPING] Fresh → skipping.")

        if self.transactions_are_stale():
            log("[INSIDERS] Stale → refreshing.")
            self.transactions_task.run()
        else:
            log("[INSIDERS] Fresh → skipping.")

        log("=== Insider Pipeline Complete ===")
