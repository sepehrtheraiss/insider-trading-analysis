# exchange_mapping_task.py
from extract.insider_api_source import InsiderAPISource
from extract.raw_writer import RawWriter
from transform.mapping_transformer import MappingTransformer
from load.mapping_loader import MappingLoader
from ..utils.logger import log


class ExchangeMappingTask:
    """Fetch → raw → transform → load exchange mapping."""

    def __init__(self, db):
        self.db = db
        self.api = InsiderAPISource()
        self.raw = RawWriter()
        self.transformer = MappingTransformer()
        self.loader = MappingLoader(db)

    def run(self):
        log("[TASK] Fetching exchange_mapping...")
        raw_data = self.api.fetch_exchange_mapping()
        raw_path = self.raw.save("exchange_mapping", raw_data)
        normalized = self.transformer.normalize(raw_data)
        self.loader.load(normalized)
        log("[TASK] exchange_mapping load complete.")
