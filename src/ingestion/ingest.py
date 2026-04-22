"""
src/ingestion/ingest.py

Stage 1 of the pipeline: read both data sources and persist raw Parquet files.

Sources ingested:
  1. Citations  — KSU Parking Services Excel file (open records request)
  2. Weather    — Open-Meteo historical archive API (no key required)

Each source is saved to its own raw Parquet file before any transformation,
preserving the audit trail and avoiding redundant API calls on re-runs.

Stack layers addressed:
  - Syntax/Encoding: Excel (row-oriented) + JSON API → Parquet (columnar)
  - Data Model: DataFrames established with correct dtypes for both sources
"""

import logging
import pandas as pd
from pathlib import Path

from src.ingestion.weather import run_weather_ingestion

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Citations ingestion  (unchanged from M3)
# ---------------------------------------------------------------------------

def read_excel(source_path: Path) -> pd.DataFrame:
    """
    Read the raw citations Excel file.

    Returns a DataFrame with the original columns and dtypes.
    No transformations applied here — raw shape is preserved for auditability.
    """
    logger.info(f"Reading source file: {source_path}")
    if not source_path.exists():
        raise FileNotFoundError(
            f"Source file not found: {source_path}\n"
            "Place the Excel file at data/source/citations.xlsx"
        )

    df = pd.read_excel(
        source_path,
        dtype={"Citation ID ": str},   # keep as string to avoid int precision loss
        parse_dates=["Timestamp"],
    )

    # Standardise column name (strip trailing space from 'Citation ID ')
    df.rename(columns={"Citation ID ": "citation_id"}, inplace=True)

    logger.info(f"Loaded {len(df):,} citation records, {df.shape[1]} columns")
    return df


def save_raw_parquet(df: pd.DataFrame, output_path: Path) -> None:
    """
    Write the raw citations DataFrame to Parquet.

    Parquet is chosen because:
      - Columnar storage means analytical queries only read needed columns
      - Built-in compression reduces file size ~60-70% vs CSV
      - Preserves dtypes exactly (timestamps, integers) without CSV ambiguity
      - Mirrors the 'raw zone' in a data lakehouse architecture
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False, engine="pyarrow", compression="snappy")
    logger.info(
        f"Raw citations Parquet written → {output_path}  "
        f"({output_path.stat().st_size / 1024:.1f} KB)"
    )


# ---------------------------------------------------------------------------
# Combined ingestion entry point
# ---------------------------------------------------------------------------

def run_ingestion(source_path: Path, output_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Full ingestion stage: read citations Excel + fetch weather API data.
    Both sources are saved to their own raw Parquet files.

    Returns
    -------
    df_citations : raw citations DataFrame
    df_weather   : raw weather DataFrame (one row per campus per date)

    Called by main.py.
    """
    # --- Source 1: Citations Excel ---
    logger.info("Ingesting Source 1: KSU Parking Citations (Excel)")
    df_citations = read_excel(source_path)
    save_raw_parquet(df_citations, output_path)

    # --- Source 2: Weather API ---
    # We pass df_citations so the weather fetcher can derive the date range
    # automatically from the actual data rather than hardcoding dates.
    logger.info("Ingesting Source 2: Open-Meteo Historical Weather (API)")
    df_weather = run_weather_ingestion(df_citations)

    return df_citations, df_weather