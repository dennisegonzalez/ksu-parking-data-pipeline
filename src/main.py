"""
src/main.py

Entry point for the KSU Parking Citations Big Data Pipeline.
Runs all five stages in sequence:

  Stage 1 — Ingest    : Excel + Open-Meteo API → raw Parquet files
  Stage 2 — Transform : clean, validate, enrich → processed Parquet files
  Stage 3 — Store     : DataFrames → SQLite (citations + daily_weather tables)
  Stage 4 — Query     : SQL → CSV reports  (6 analytical reports)
  Stage 5 — Spark     : PySpark → CSV reports  (2 distributed aggregations)

Usage:
    python src/main.py

The pipeline is idempotent: running it multiple times on the same data
produces the same result. Duplicate citation IDs are silently skipped;
weather records are refreshed in place.
"""

import logging
import sys
import time
from datetime import datetime
from pathlib import Path

# Make sure the project root is on the path so all imports resolve
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (
    SOURCE_EXCEL,
    RAW_PARQUET,
    PROCESSED_PARQUET,
    DB_PATH,
    REPORTS_DIR,
    SPARK_REPORTS_DIR,
)
from src.ingestion.ingest         import run_ingestion
from src.processing.transform     import run_transformation
from src.storage.db_handler       import run_storage
from src.queries.analytics        import run_queries
from src.processing.spark_analysis import run_spark_analysis


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
        # Stage 1: Ingestion
        # Both sources ingested here: citations Excel + Open-Meteo weather API
        logger.info("STAGE 1: Ingestion")
        df_citations, df_weather_raw = run_ingestion(SOURCE_EXCEL, RAW_PARQUET)

        # Stage 2: Transformation
        # Citations cleaned/validated; weather normalised and bucketed
        logger.info("STAGE 2: Transformation")
        df_clean, df_weather_clean = run_transformation(
            df_citations, df_weather_raw, PROCESSED_PARQUET
        )

        # Stage 3: Storage
        # Both DataFrames loaded into SQLite (citations + daily_weather tables)
        logger.info("STAGE 3: Storage")
        run_storage(df_clean, df_weather_clean, DB_PATH)

        # Stage 4: Querying
        # 6 SQL queries → CSV reports (includes weather-citation correlation)
        logger.info("STAGE 4: Querying")
        run_queries(DB_PATH, REPORTS_DIR)

        # Stage 5: Spark
        # 2 PySpark aggregations over processed Parquet → spark/ CSV reports
        logger.info("STAGE 5: Spark Analysis")
        run_spark_analysis()

    except FileNotFoundError as e:
        logger.error(f"Pipeline aborted — missing file: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed unexpectedly: {e}", exc_info=True)
        sys.exit(1)

    elapsed = time.time() - start
    logger.info("=" * 60)
    logger.info(f"Pipeline complete. Duration: {elapsed:.1f}s")
    logger.info(f"  Raw Parquet (citations): {RAW_PARQUET}")
    logger.info(f"  Raw Parquet (weather):   {RAW_PARQUET.parent / 'weather_raw.parquet'}")
    logger.info(f"  Processed Parquet:       {PROCESSED_PARQUET}")
    logger.info(f"  Database:                {DB_PATH}")
    logger.info(f"  SQL Reports:             {REPORTS_DIR}")
    logger.info(f"  Spark Reports:           {SPARK_REPORTS_DIR}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()