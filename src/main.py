"""
main.py

Entry point for the KSU Parking Citations Big Data Pipeline.
Runs all four stages in sequence: Ingest → Transform → Store → Query.

Usage:
    python src/main.py

The pipeline is idempotent: running it multiple times on the same data
produces the same result (no duplicate records in the database).
"""

import logging
import sys
import time
from datetime import datetime
from pathlib import Path

# Make sure src/ is on the path so sibling imports work
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (
    SOURCE_EXCEL,
    RAW_PARQUET,
    PROCESSED_PARQUET,
    DB_PATH,
    REPORTS_DIR,
)
from src.ingestion.ingest       import run_ingestion
from src.processing.transform   import run_transformation
from src.storage.db_handler     import run_storage
from src.queries.analytics      import run_queries


def setup_logging() -> None:
    """Configure structured logging to stdout with timestamps."""
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(asctime)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def main() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)

    start = time.time()
    logger.info("=" * 60)
    logger.info("KSU Parking Citations Pipeline — starting")
    logger.info(f"Run timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    try:
        # Stage 1: Ingestion (Excel → DataFrame + raw Parquet)
        logger.info("STAGE 1: Ingestion")
        df_raw = run_ingestion(SOURCE_EXCEL, RAW_PARQUET)

        # Stage 2: Transformation (clean + enrich DataFrame → processed Parquet)
        logger.info("STAGE 2: Transformation")
        df_clean = run_transformation(df_raw, PROCESSED_PARQUET)

        # Stage 3: Storage (DataFrame → SQLite)
        logger.info("STAGE 3: Storage")
        run_storage(df_clean, DB_PATH)

        # Stage 4: Querying (SQL → CSV reports)
        logger.info("STAGE 4: Querying")
        run_queries(DB_PATH, REPORTS_DIR)

    except FileNotFoundError as e:
        logger.error(f"Pipeline aborted — missing file: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed unexpectedly: {e}", exc_info=True)
        sys.exit(1)

    elapsed = time.time() - start
    logger.info("=" * 60)
    logger.info(f"Pipeline complete. Duration: {elapsed:.1f}s")
    logger.info(f"  Raw Parquet:       {RAW_PARQUET}")
    logger.info(f"  Processed Parquet: {PROCESSED_PARQUET}")
    logger.info(f"  Database:          {DB_PATH}")
    logger.info(f"  Reports:           {REPORTS_DIR}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()