from datetime import datetime, UTC

from insider_trading.tasks.exchange_mapping_task import ExchangeMappingTask
from insider_trading.tasks.insider_transactions_task import InsiderTransactionsTask

from insider_trading.extract.sources.insider_api_source import InsiderApiSource
from insider_trading.transform.mapping_transformer import MappingTransformer
from insider_trading.load.mapping_loader import ExchangeMappingLoader

from insider_trading.extract.sources.insider_transactions_source import InsiderTransactionsSource
from insider_trading.transform.insider_transformer import InsiderTransactionsTransformer
from insider_trading.load.insider_loader import InsiderTransactionsLoader

from utils.logger import Logger

from writers.raw_writer import RawWriter
from writers.staging_writer import StagingWriter
from writers.final_writer import FinalWriter


class InsiderTradingPipeline:
    """
    Orchestrates the entire Insider Trading ETL:
        - Exchange Mapping ETL (monthly)
        - Insider Transactions ETL (weekly)
    """

    MAPPING_REFRESH_DAYS = 30
    TRANSACTION_REFRESH_DAYS = 7

    def __init__(self, config, db):
        self.log = Logger(self.__class__.__name__)

        self.config = config
        self.db = db

        # ------------------------------------------------------------
        # Writers (Bronze → Silver → Gold)
        # ------------------------------------------------------------
        self.raw_writer = RawWriter(directory="data/raw")
        self.staging_writer = StagingWriter(directory="data/staging")

        FINAL_SCHEMA_MAPPING = [
            "issuerTicker",
            "cik",
            "exchange",
            "sector",
            "industry",
            "category",
            "name",
        ]

        self.final_writer_mapping = FinalWriter(
            directory="data/final",
            expected_schema=FINAL_SCHEMA_MAPPING,
            enforce_types={
                "issuerTicker": str,
                "cik": str,
                "exchange": str,
                "sector": str,
                "industry": str,
                "category": str,
                "name": str,
            },
            keep_history=True,
        )

        # For insider transactions you will later define schema:
        FINAL_SCHEMA_TRANSACTIONS = [
            # fill in later based on your transaction model
        ]

        self.final_writer_transactions = FinalWriter(
            directory="data/final",
            expected_schema=FINAL_SCHEMA_TRANSACTIONS,
            enforce_types={},
            keep_history=True,
        )

        # ------------------------------------------------------------
        # Exchange Mapping ETL components
        # ------------------------------------------------------------
        self.mapping_source = InsiderApiSource(http_adapter=config.http_adapter)
        self.mapping_transformer = MappingTransformer()
        self.mapping_loader = ExchangeMappingLoader(db)

        self.mapping_task = ExchangeMappingTask(
            source=self.mapping_source,
            transformer=self.mapping_transformer,
            loader=self.mapping_loader,
            raw_writer=self.raw_writer,
            staging_writer=self.staging_writer,
            final_writer=self.final_writer_mapping,
        )

        # ------------------------------------------------------------
        # Insider Transactions ETL components
        # ------------------------------------------------------------
        self.transactions_source = InsiderTransactionsSource(http_adapter=config.http_adapter)
        self.transactions_transformer = InsiderTransactionsTransformer()
        self.transactions_loader = InsiderTransactionsLoader(db)

        self.transactions_task = InsiderTransactionsTask(
            source=self.transactions_source,
            transformer=self.transactions_transformer,
            loader=self.transactions_loader,
            raw_writer=self.raw_writer,
            staging_writer=self.staging_writer,
            final_writer=self.final_writer_transactions,
        )

    # ================================================================
    #                  REFRESH STALENESS CHECKS
    # ================================================================
    def mapping_is_stale(self):
        last = self.db.last_updated("exchange_mapping")
        return not last or (datetime.now(UTC)() - last).days >= self.MAPPING_REFRESH_DAYS

    def transactions_are_stale(self):
        last = self.db.last_updated("insider_transactions")
        return not last or (datetime.now(UTC) - last).days >= self.TRANSACTION_REFRESH_DAYS

    # ================================================================
    #                           RUN PIPELINE
    # ================================================================
    def run(self):
        self.log.info("=== InsiderTradingPipeline START ===")

        # --------------------------
        # Exchange Mapping ETL
        # --------------------------
        if self.mapping_is_stale():
            self.log.info("[MAPPING] Stale → refreshing")
            self.mapping_task.run()
        else:
            self.log.info("[MAPPING] Fresh → skipping")

        # --------------------------
        # Insider Transactions ETL
        # --------------------------
        if self.transactions_are_stale():
            self.log.info("[TRANSACTIONS] Stale → refreshing")
            self.transactions_task.run()
        else:
            self.log.info("[TRANSACTIONS] Fresh → skipping")

        self.log.info("=== InsiderTradingPipeline COMPLETE ===")
