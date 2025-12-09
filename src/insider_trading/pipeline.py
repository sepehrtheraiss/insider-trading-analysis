from datetime import datetime, timedelta, UTC

from insider_trading.tasks.exchange_mapping_task import ExchangeMappingTask
from insider_trading.tasks.insider_transactions_task import InsiderTransactionsTask

from insider_trading.extract.sources.insider_api_source import InsiderApiSource
from insider_trading.transform.mapping_transformer import MappingTransformer
from insider_trading.transform.insider_transformer import InsiderTransactionsTransformer

from insider_trading.load.mapping_loader import ExchangeMappingLoader
from insider_trading.load.insider_loader import InsiderTransactionsLoader

from writers.raw_writer import RawWriter
from writers.staging_writer import StagingWriter
from writers.final_writer import FinalWriter

from utils.logger import Logger


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

        # ---------------------------
        # FINAL SCHEMAS (exact order)
        # ---------------------------

        FINAL_SCHEMA_MAPPING = [
            "name",
            "issuer_ticker",
            "cik",
            "exchange",
            "is_delisted",
            "category",
            "sector",
            "industry",
            "sic_sector",
            "sic_industry",
        ]

        FINAL_SCHEMA_TRANSACTIONS = [
            "filed_at",
            "period_of_report",
            "document_type",
            "issuer_ticker",
            "issuer_cik",
            "issuer_name",
            "reporter",
            "reporter_cik",
            "is_officer",
            "officer_title",
            "is_director",
            "is_ten_percent_owner",
            "table",
            "code",
            "acquired_disposed",
            "transaction_date",
            "shares",
            "price_per_share",
            "total_value",
            "shares_owned_following",
            "is_10b5_1",
        ]

        # Final writers
        self.final_writer_mapping = FinalWriter(
            directory="data/final",
            expected_schema=FINAL_SCHEMA_MAPPING,
            enforce_types={},  # optional strictness
            keep_history=True,
        )

        self.final_writer_transactions = FinalWriter(
            directory="data/final",
            expected_schema=FINAL_SCHEMA_TRANSACTIONS,
            enforce_types={},  # optional strictness
            keep_history=True,
        )

        # ------------------------------------------------------------
        # Exchange Mapping ETL components
        # ------------------------------------------------------------
        self.mapping_source = InsiderApiSource(base_url=config.base_url, api_key=config.api_key)
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
        self.transactions_source = InsiderApiSource(api_key=config.api_key)
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
        return not last or (datetime.now(UTC) - last).days >= self.MAPPING_REFRESH_DAYS

    def transactions_are_stale(self):
        last = self.db.last_updated("insider_transactions")
        return not last or (datetime.now(UTC) - last).days >= self.TRANSACTION_REFRESH_DAYS


    # ================================================================
    #    ------------------ range computation ----------------------
    # ================================================================
    def _compute_transactions_window(self) -> tuple[str, str]:
        """
        Decide which [start_date, end_date] to pass to SEC API.

        - if no last_updated: fetch last N days (TRANSACTION_REFRESH_DAYS)
        - if last_updated exists: start = last_updated+1, end=today
        """
        today = datetime.now(UTC).date()
        last = self.db.last_updated("insider_transactions")

        if not last:
            # First-time run → take last N days
            start = (today - timedelta(days=self.TRANSACTION_REFRESH_DAYS)).isoformat()
            end = today.isoformat()
            return start, end

        # Normal incremental refresh: from day after last_updated to today
        start = (last.date() + timedelta(days=1)).isoformat()
        end = today.isoformat()
        return start, end

    # ================================================================
    #                           RUN PIPELINE
    # ================================================================
    def run(self, query: str = "*:*"):
        self.log.info("=== InsiderTradingPipeline START ===")

        # 1) Mapping (no params)
        if self.config.test_mode_map:
            self.mapping_task.run(raw_path_override=self.config.test_path_map)
        else:
            if self.mapping_is_stale():
                self.log.info("[MAPPING] Stale → refreshing")
                self.mapping_task.run()
            else:
                self.log.info("[MAPPING] Fresh → skipping")

        # 2) Insider transactions
        if self.config.test_mode_tx:
            self.transactions_task.run(params=None, raw_path_override=self.config.test_path_map)
        else:
            if self.transactions_are_stale():
                start, end = self._compute_transactions_window()

                params = {
                    "query_string": query,
                    "start_date": start,
                    "end_date": end,
                }
                self.log.info(
                    f"[TRANSACTIONS] Running for window: {start} → {end} (query={query})"
                )
                self.transactions_task.run(params)
            else:
                self.log.info("[TRANSACTIONS] Fresh → skipping")

        self.log.info("=== InsiderTradingPipeline COMPLETE ===")