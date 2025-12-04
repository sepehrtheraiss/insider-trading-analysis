import logging
from pathlib import Path

from extract.sec_api_client import get_all_filings  # or your actual function
from transform.clean import clean_data  # adjust import names if needed
from transform.flatten import flatten_data  # if you have this
from transform.mapping import apply_mappings  # if you use mapping logic
from load.loader import save_raw, save_staging, save_final  # or your loader functions

# Optionally configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)

def run_etl(days: int = 30, output_dir: Path | str = None):
    """
    Runs full ETL pipeline:
      1) Extract raw filings via API
      2) Clean / transform / map
      3) Save raw, staging, and final data CSVs
    Returns: final processed DataFrame (or path to saved file)
    """
    logging.info("Starting ETL pipeline")
    
    # Step 1: extract
    logging.info("Step 1/4 — Extracting raw data")
    raw_df = get_all_filings(days=days)
    raw_path = save_raw(raw_df)
    logging.info(f"Saved raw data to {raw_path}")
    
    # Step 2: clean
    logging.info("Step 2/4 — Cleaning data")
    cleaned = clean_data(raw_df)
    
    # Step 3: flatten / map / normalize
    logging.info("Step 3/4 — Flattening and mapping")
    flat = flatten_data(cleaned)
    mapped = apply_mappings(flat)
    
    staging_path = save_staging(mapped)
    logging.info(f"Saved staging data to {staging_path}")
    
    # Step 4: final save
    logging.info("Step 4/4 — Saving final dataset")
    final_path = save_final(mapped)
    logging.info(f"Saved final data to {final_path}")
    
    logging.info("ETL pipeline completed successfully")
    return mapped  # or return final_path, depending on your workflow

if __name__ == "__main__":
    # For ad-hoc runs
    df = run_etl(days=30)

