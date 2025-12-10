from utils.logger import Logger


class ExchangeMappingTask:
    """
    Full medallion ETL:
        Extract → Raw → Transform → Staging → Final → Load
    Handles exchange mapping ingestion from SEC API.
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

    # ----------------------------------------------------------
    # Main Task Runner
    # ----------------------------------------------------------
    def run(self, params: dict | None = None, raw_path_override: str = None):
        """
        Run the exchange  ETL.

        Two modes:
        1. Normal API mode → params provided
        2. Test mode → raw_path_override provided (skip API)

        """
        self.log.info("=== ExchangeMappingTask START ===")
        # ------------------------------------------------------
        # 1. EXTRACT (raw JSON from SEC API)
        # ------------------------------------------------------
        # ------------------------------------------------------
        # TEST MODE: use provided raw JSON path
        # ------------------------------------------------------
        if raw_path_override:
            self.log.info(f"[TEST MODE] Loading raw exchange data from {raw_path_override}")
            raw = self.raw_writer.load_json(raw_path_override)
        else:
            # ------------------------------------------------------
            # NORMAL MODE: fetch from API
            # ------------------------------------------------------
            raw = self.source.fetch_exchange_mapping()
            # Convert generator to list
            # task layer is responsible for buffering before writing raw files.
            raw = list(raw)
            self.log.info(f"[EXTRACT] raw mapping records = {len(raw)}")

            raw_path = self.raw_writer.save("exchange_mapping", raw)
            self.log.info(f"[RAW] Saved to {raw_path}")

        # ------------------------------------------------------
        # 2. TRANSFORM (normalize → clean → dedupe → validate)
        #    This also writes intermediate staging artifacts
        # ------------------------------------------------------
        df_final = self.transformer.transform(
            raw,
            staging_writer=self.staging_writer,   # writes normalized / cleaned / deduped
        )
        self.log.info(f"[TRANSFORM] Final row count = {len(df_final)}")

        # ------------------------------------------------------
        # 3. STRICT FINAL WRITE (Gold)
        #    Validates schema + types before writing Parquet.
        # ------------------------------------------------------
        final_path = self.final_writer.save("exchange_mapping_final", df_final)
        self.log.info(f"[FINAL] Gold-layer Parquet saved to {final_path}")

        # ------------------------------------------------------
        # 4. LOAD → DB (append-only / slow-changing)
        # ------------------------------------------------------
        self.loader.load(df_final)
        self.log.info("[LOAD] Successfully loaded into database")

        self.log.info("=== ExchangeMappingTask COMPLETE ===")
