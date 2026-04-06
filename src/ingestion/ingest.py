"""
This is the first step of the pipeline. It reads the source Excel file to Pandas DataFrame
and persist it as Parquet (columnar format) for the processing stage.

Stack layers addressed:
  - Syntax/Encoding: Excel (row-oriented) --> Parquet (columnar, compressed)
  - Data Model: DataFrame schema established with correct dtypes
"""

import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


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

    logger.info(f"Loaded {len(df):,} records, {df.shape[1]} columns")
    return df


def save_raw_parquet(df: pd.DataFrame, output_path: Path) -> None:
    """
    Write the raw DataFrame to Parquet.

    Parquet is chosen because:
      - Columnar storage means analytical queries only read needed columns
      - Built-in compression reduces file size ~60-70% vs CSV
      - Preserves dtypes exactly (timestamps, integers) without CSV ambiguity
      - Mirrors the 'raw zone' in a data lakehouse architecture
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False, engine="pyarrow", compression="snappy")
    logger.info(f"Raw Parquet written → {output_path}  ({output_path.stat().st_size / 1024:.1f} KB)")


def run_ingestion(source_path: Path, output_path: Path) -> pd.DataFrame:
    """
    Full ingestion stage: read Excel, save raw Parquet, return DataFrame.
    Called by main.py.
    """
    df = read_excel(source_path)
    save_raw_parquet(df, output_path)
    return df