# insider_transactions_task.py
from extract.insider_api_source import InsiderAPISource
from extract.raw_writer import RawWriter
from transform.insider_transformer import InsiderTransformer
from load.insider_loader import InsiderLoader
from ..utils.logger import log


class InsiderTransactionsTask:
    """Fetch → raw → transform → load insider transactions."""

    def __init__(self, db):
        self.db = db
        self.api = InsiderAPISource()
        self.raw = RawWriter()
        self.transformer = InsiderTransformer()
        self.loader = InsiderLoader(db)

    def run(self):
        log("[TASK] Fetching insider transactions...")
        raw_data = self.api.fetch_insider_transactions()
        raw_path = self.raw.save("insider_transactions", raw_data)
        normalized = self.transformer.normalize(raw_data)
        self.loader.load(normalized)
        log("[TASK] insider_transactions load complete.")
