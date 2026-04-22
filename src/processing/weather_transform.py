"""
src/processing/weather_transform.py

Stage 2 (weather branch): clean, validate, and enrich the raw weather DataFrame
fetched from Open-Meteo.

Transformations applied:
  1. Parse date column to proper dtype
  2. Round float columns to sensible precision
  3. Handle missing values (some days may have NaN precipitation)
  4. Map WMO weather codes to human-readable condition labels
  5. Add a precipitation bucket category (None / Light / Moderate / Heavy)
  6. Enforce final schema and save processed Parquet

Stack layers addressed:
  - Data Model: Schema enforced, correct dtypes throughout
  - Processing: Batch transformation over all weather records
"""

import logging
import numpy as np
import pandas as pd
from pathlib import Path

from config.settings import (
    WMO_CODE_LABELS,
    WEATHER_PROCESSED_PARQUET,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. Parse and validate date column
# ---------------------------------------------------------------------------

def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert the 'date' column from string to datetime.date.
    Open-Meteo returns dates as ISO strings ('YYYY-MM-DD').
    We store them as strings in Parquet (matching the citations schema)
    but validate they parse correctly here.
    """
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    logger.info("Weather date column parsed and validated")
    return df


# ---------------------------------------------------------------------------
# 2. Round float columns
# ---------------------------------------------------------------------------

def round_floats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Round temperature and precipitation to sensible precision.
    Open-Meteo returns values like 87.2999999 due to float representation.
    """
    df["temperature_2m_max"] = df["temperature_2m_max"].round(1)
    df["temperature_2m_min"] = df["temperature_2m_min"].round(1)
    df["precipitation_sum"]  = df["precipitation_sum"].round(2)
    logger.info("Weather float columns rounded")
    return df


# ---------------------------------------------------------------------------
# 3. Handle missing values
# ---------------------------------------------------------------------------

def handle_missing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill NaN precipitation with 0.0 — Open-Meteo occasionally returns null
    for days with no measurable precipitation rather than 0.
    Temperature NaNs are left as-is and flagged so downstream queries can
    filter them if needed.
    """
    precip_nulls = df["precipitation_sum"].isna().sum()
    if precip_nulls:
        df["precipitation_sum"] = df["precipitation_sum"].fillna(0.0)
        logger.info(f"Filled {precip_nulls} null precipitation values with 0.0")

    temp_nulls = df[["temperature_2m_max", "temperature_2m_min"]].isna().sum().sum()
    if temp_nulls:
        logger.warning(
            f"{temp_nulls} null temperature values remain — "
            "these rows will be excluded from temperature-based analytics"
        )

    return df


# ---------------------------------------------------------------------------
# 4. Add condition label from WMO weather code
# ---------------------------------------------------------------------------

def add_condition_label(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map the numeric WMO weather code to a human-readable string.
    Codes not found in the mapping are labelled 'Unknown'.

    Example: 63 → 'Moderate rain', 0 → 'Clear sky'
    """
    df["condition_label"] = df["weathercode"].map(WMO_CODE_LABELS).fillna("Unknown")
    unknown_count = (df["condition_label"] == "Unknown").sum()
    if unknown_count:
        logger.warning(f"{unknown_count} weather records have unrecognised WMO codes")
    logger.info("Weather condition labels added")
    return df


# ---------------------------------------------------------------------------
# 5. Add precipitation bucket  (fixed)
# ---------------------------------------------------------------------------

def add_precip_bucket(df: pd.DataFrame) -> pd.DataFrame:
    """
    Categorise daily precipitation into four buckets for use in the
    weather-citations correlation query in analytics.py.

    Buckets:
      None     — exactly 0.0 mm
      Light    — 0.01 – 5.0 mm
      Moderate — 5.01 – 15.0 mm
      Heavy    — > 15.0 mm

    Uses numpy.select directly to avoid the deprecated pd.np accessor
    and ensures 0.0mm days are correctly labelled 'None' rather than NaN.
    """
    p = df["precipitation_sum"]

    conditions = [
        p == 0.0,
        p <= 5.0,
        p <= 15.0,
    ]
    choices = ["None", "Light", "Moderate"]

    df["precip_bucket"] = np.select(conditions, choices, default="Heavy")

    bucket_counts = df["precip_bucket"].value_counts().to_dict()
    logger.info(f"Precipitation buckets assigned: {bucket_counts}")
    return df


# ---------------------------------------------------------------------------
# 6. Enforce final schema
# ---------------------------------------------------------------------------

def finalise_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select and order the final columns for the processed weather dataset.
    Matches the schema documented in docs/data_dictionary.md.
    """
    final_columns = [
        "date",
        "campus",
        "temperature_2m_max",
        "temperature_2m_min",
        "precipitation_sum",
        "weathercode",
        "condition_label",
        "precip_bucket",
    ]
    df = df[final_columns]
    logger.info(f"Weather schema finalised: {len(final_columns)} columns, {len(df):,} records")
    return df


# ---------------------------------------------------------------------------
# 7. Save processed Parquet
# ---------------------------------------------------------------------------

def save_weather_processed_parquet(df: pd.DataFrame, output_path: Path) -> None:
    """
    Write the clean weather DataFrame to processed Parquet.
    Mirrors the pattern in transform.py — raw is never modified.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False, engine="pyarrow", compression="snappy")
    logger.info(
        f"Processed weather Parquet written → {output_path}  "
        f"({output_path.stat().st_size / 1024:.1f} KB)"
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_weather_transformation(df_weather: pd.DataFrame) -> pd.DataFrame:
    """
    Run all weather transformation steps in order.
    Called by transform.py inside run_transformation().
    Returns the cleaned, enriched weather DataFrame.
    """
    logger.info("--- Weather transformation starting ---")
    df_weather = parse_dates(df_weather)
    df_weather = round_floats(df_weather)
    df_weather = handle_missing(df_weather)
    df_weather = add_condition_label(df_weather)
    df_weather = add_precip_bucket(df_weather)
    df_weather = finalise_schema(df_weather)
    save_weather_processed_parquet(df_weather, WEATHER_PROCESSED_PARQUET)
    logger.info(f"--- Weather transformation complete: {len(df_weather):,} records ---")
    return df_weather