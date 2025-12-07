from utils.logger import Logger


class InsiderTransactionsTask:
    """
    Full medallion ETL:
        Extract → Raw → Transform → Staging → Final → Load
    Handles insider trading ingestion from SEC API.
    """

    def __init__(
        self,
        source,
        transformer,
        loader,
        raw_writer,
        staging_writer,
        final_writer,
    ):
        self.source = source
        self.transformer = transformer
        self.loader = loader

        self.raw_writer = raw_writer
        self.staging_writer = staging_writer
        self.final_writer = final_writer

        self.log = Logger(self.__class__.__name__)
        self.DEFAULT_PARAMS: dict[str, str] = {'query_string': '*:*'}
    
    # ----------------------------------------------------------
    # Main Task Runner
    # ----------------------------------------------------------
    def run(self, params: dict, raw_path_override: str = None):
        """
        Run the Insider Transactions ETL.

        Two modes:
        1. Normal API mode → params provided
        2. Test mode → raw_path_override provided (skip API)

        params should include:
            query_string: str (issuer.tradingSymbol:AMZN)
            start_date: str (YYYY-MM-DD)
            end_date: str   (YYYY-MM-DD)
        """
        self.log.info("=== InsiderTransactionsTask START ===")
        raw_path_override = 'data/raw/insider_transactions_20251207_185328.json' 
        # ------------------------------------------------------
        # 1. EXTRACT (raw JSON from SEC API)
        # ------------------------------------------------------
        # ------------------------------------------------------
        # TEST MODE: use provided raw JSON path
        # ------------------------------------------------------
        if raw_path_override:
            self.log.info(f"[TEST MODE] Loading raw insider data from {raw_path_override}")
            raw = self.raw_writer.load_json(raw_path_override)
        else:
            # ------------------------------------------------------
            # NORMAL MODE: fetch from API
            # ------------------------------------------------------
            if params:
                query_string = params["query_string"]
                start_date = params["start_date"]
                end_date= params["end_date"]
            else:
                query_string = self.DEFAULT_PARAMS["query_string"]
                start_date = '' 
                end_date=  ''

            raw = self.source.fetch_insider_transactions(query_string, start_date, end_date)
            # Convert generator to list
            # task layer is responsible for buffering before writing raw files.
            raw = list(raw)
            self.log.info(f"[EXTRACT] Raw filings = {len(raw)}")

            raw_path = self.raw_writer.save("insider_transactions", raw)
            self.log.info(f"[RAW] Saved → {raw_path}")

        # ------------------------------------------------------
        # 2. TRANSFORM (normalize → clean → dedupe → validate)
        #    This also writes intermediate staging artifacts
        # ------------------------------------------------------
        df_final = self.transformer.transform(raw, staging_writer=self.staging_writer)
        self.log.info(f"[TRANSFORM] Final rows = {len(df_final)}")

        # ------------------------------------------------------
        # 3. STRICT FINAL WRITE (Gold)
        #    Validates schema + types before writing Parquet.
        # ------------------------------------------------------
        if self.final_writer:
            final_path = self.final_writer.save("insider_transactions_final", df_final)
            self.log.info(f"[GOLD] Saved final parquet → {final_path}")

        # ----------------------------
        # 4. LOAD → DB (append-only / slow-changing)
        # ----------------------------
        self.loader.load(df_final)
        self.log.info("[LOAD] DB load complete")

        self.log.info("=== InsiderTransactionsTask COMPLETE ===")
