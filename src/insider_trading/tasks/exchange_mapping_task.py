from insider_trading.extract.sources.insider_api_source import InsiderApiSource
from insider_trading.extract.raw_writer import RawWriter
from insider_trading.transform.mapping_transformer import MappingTransformer
from insider_trading.load.mapping_loader import MappingLoader
from utils.logger import Logger

class ExchangeMappingTask:
    """Fetch → raw → transform → load exchange mapping."""

    def __init__(self, config, db):
        self.db = db
        self.api = InsiderApiSource(config.base_url, config.api_key)
        self.raw = RawWriter()
        self.transformer = MappingTransformer()
        self.loader = MappingLoader(db)
        self.log = Logger(self.__class__.__name__)

    def run(self, params: dict = None):
        self.log.info("[TASK] Fetching exchange_mapping...")
        raw_data = self.api.fetch_exchange_mapping()
        raw_path = self.raw.save("exchange_mapping", raw_data)
        normalized = self.transformer.transform(raw_data)
        self.loader.load(normalized)
        self.log.info("[TASK] exchange_mapping load complete.")
