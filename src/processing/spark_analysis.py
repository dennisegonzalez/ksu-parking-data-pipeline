"""
src/processing/spark_analysis.py

Stage 5 of the pipeline: distributed aggregations using PySpark.

Reads the processed Parquet files directly (bypassing SQLite) and runs
two analytical aggregations using the Spark DataFrame API. Results are
written to data/reports/spark/ as CSV files.

Why Spark here?
  - SQLite + Pandas (Stages 3-4) handle structured storage and SQL queries well
    at this data volume, but cannot scale horizontally.
  - Spark reads Parquet natively and in parallel — the same processed files
    produced in Stage 2 are reused with no conversion step.
  - This reflects the real-world lambda/kappa architecture pattern: use the
    right tool for each layer. Pandas for cleaning, SQL for structured queries,
    Spark for large-scale aggregation.

Aggregations:
  1. violation_summary_spark   — citation count and total fines by violation
                                  type and campus (mirrors violations_by_type
                                  but split by campus using Spark)
  2. weather_hourly_spark      — average citations per hour grouped by
                                  precipitation bucket (cross-source join
                                  between citations and weather in Spark)

Stack layers addressed:
  - Processing: Distributed batch processing via PySpark
  - Data Model: Spark DataFrames with enforced schemas
"""

import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.settings import (
    PROCESSED_PARQUET,
    WEATHER_PROCESSED_PARQUET,
    SPARK_REPORTS_DIR,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SparkSession factory
# ---------------------------------------------------------------------------

def get_spark_session() -> SparkSession:
    """
    Create or retrieve a local SparkSession.

    master("local[*]") uses all available CPU cores on the local machine.
    This is appropriate for a single-node pipeline; in production this would
    point to a cluster master URL (e.g. spark://host:7077 or yarn).

    The session is configured to suppress verbose Spark INFO logs so only
    pipeline-level logs are visible in the console.
    """
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("KSU-Parking-Citations-Pipeline")
        .config("spark.sql.shuffle.partitions", "8")   # low partition count
                                                        # appropriate for this
                                                        # data volume locally
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    # Suppress Spark's own INFO logs — keep only WARNings and above
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Aggregation 1: violation summary by campus (Spark)
# ---------------------------------------------------------------------------

def run_violation_summary(spark: SparkSession, reports_dir: Path) -> None:
    """
    Read processed citations Parquet and aggregate citation count + total
    fines by violation type and campus using the Spark DataFrame API.

    This mirrors the SQL violations_by_type query in analytics.py but adds
    campus as a grouping dimension and uses Spark instead of SQLite.

    Output: data/reports/spark/violation_summary_spark.csv
    """
    logger.info("  [Spark] Reading citations Parquet...")
    df = spark.read.parquet(str(PROCESSED_PARQUET))

    # Exclude credit adjustments (mirrors the WHERE clause in analytics.py)
    df = df.filter(F.col("is_credit_adjustment").cast("int") == 0)

    result = (
        df.groupBy("violation_type", "campus")
        .agg(
            F.count("citation_id").alias("citation_count"),
            F.sum(
                F.when(F.col("fine_amount") > 0, F.col("fine_amount")).otherwise(0)
            ).alias("total_fines_collected"),
            F.round(
                F.avg(F.when(F.col("fine_amount") > 0, F.col("fine_amount"))), 2
            ).alias("avg_fine"),
        )
        .orderBy(F.desc("citation_count"))
    )

    _save_spark_csv(result, reports_dir, "violation_summary_spark")
    logger.info(f"  [Spark] violation_summary_spark → {reports_dir / 'violation_summary_spark.csv'}")


# ---------------------------------------------------------------------------
# Aggregation 2: hourly citations by weather condition (Spark cross-join)
# ---------------------------------------------------------------------------

def run_weather_hourly(spark: SparkSession, reports_dir: Path) -> None:
    """
    Join citations to weather data in Spark and aggregate average citation
    count by hour of day and precipitation bucket.

    This is a cross-source join that would be expensive in SQLite at scale
    but is handled efficiently by Spark's distributed shuffle join.

    Output: data/reports/spark/weather_hourly_spark.csv
    """
    logger.info("  [Spark] Reading citations and weather Parquet files...")
    df_citations = spark.read.parquet(str(PROCESSED_PARQUET))
    df_weather   = spark.read.parquet(str(WEATHER_PROCESSED_PARQUET))

    # Exclude credit adjustments
    df_citations = df_citations.filter(F.col("is_credit_adjustment").cast("int") == 0)

    # Join on date + campus
    joined = df_citations.join(
        df_weather.select("date", "campus", "precip_bucket", "condition_label"),
        on=["date", "campus"],
        how="left",
    )

    result = (
        joined.groupBy("hour", "precip_bucket")
        .agg(
            F.count("citation_id").alias("citation_count"),
            F.countDistinct("date").alias("days_in_bucket"),
        )
        .withColumn(
            "avg_citations_per_day",
            F.round(F.col("citation_count") / F.col("days_in_bucket"), 2),
        )
        .orderBy("hour", "precip_bucket")
    )

    _save_spark_csv(result, reports_dir, "weather_hourly_spark")
    logger.info(f"  [Spark] weather_hourly_spark → {reports_dir / 'weather_hourly_spark.csv'}")


# ---------------------------------------------------------------------------
# CSV writer helper
# ---------------------------------------------------------------------------

def _save_spark_csv(df, reports_dir: Path, name: str) -> None:
    """
    Write a Spark DataFrame to a single CSV file.

    Spark normally writes one file per partition into a directory.
    coalesce(1) merges partitions first so the output is a single file,
    consistent with the CSV outputs from analytics.py.
    The temp directory is written first, then the part file is renamed.
    """
    import shutil
    import glob

    tmp_dir = reports_dir / f"_{name}_tmp"
    out_path = reports_dir / f"{name}.csv"

    # Write partitions to temp directory
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(str(tmp_dir))
    )

    # Find the single part file Spark wrote and rename it
    part_files = glob.glob(str(tmp_dir / "part-*.csv"))
    if not part_files:
        raise RuntimeError(f"Spark wrote no output files to {tmp_dir}")

    shutil.move(part_files[0], str(out_path))
    shutil.rmtree(str(tmp_dir), ignore_errors=True)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_spark_analysis() -> None:
    """
    Full Spark stage: create session, run both aggregations, stop session.
    Called by main.py as Stage 5.
    """
    logger.info("--- Spark analysis stage starting ---")
    SPARK_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    spark = get_spark_session()
    try:
        run_violation_summary(spark, SPARK_REPORTS_DIR)
        run_weather_hourly(spark, SPARK_REPORTS_DIR)
    finally:
        spark.stop()
        logger.info("  [Spark] Session stopped")

    logger.info(
        f"--- Spark stage complete: 2 reports in {SPARK_REPORTS_DIR} ---"
    )