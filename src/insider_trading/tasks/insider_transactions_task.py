# insider_transactions_task.py
from insider_trading.extract.sources.insider_api_source import InsiderApiSource
from insider_trading.extract.raw_writer import RawWriter
from insider_trading.transform.insider_transformer import InsiderTransformer
from insider_trading.load.insider_loader import InsiderLoader
from utils.logger import Logger 


class InsiderTransactionsTask:
    """Fetch → raw → transform → load insider transactions."""

    def __init__(self, config, db):
        self.db = db
        self.api = InsiderApiSource(config.base_url, config.api_key)
        self.raw = RawWriter()
        self.transformer = InsiderTransformer()
        self.loader = InsiderLoader(db)
        self.log = Logger(self.__class__.__name__)

    def run(self, params: dict = None):
        self.log.info("[TASK] Fetching insider transactions...")
        raw_data = self.api.fetch_insider_transactions(
            query_string=params["query"],
            start=params["start_date"],
            end=params["end_date"]
        )
        raw_path = self.raw.save("insider_transactions", raw_data)
        normalized = self.transformer.normalize(raw_data)
        self.loader.load(normalized)
        self.log.info("[TASK] insider_transactions load complete.")
