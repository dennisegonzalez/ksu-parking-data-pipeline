"""
processing/transform.py

Stage 2 of the pipeline: clean, validate, and enrich the raw DataFrame.

Transformation decisions documented inline — each is explained in the M3 report
under 'Transformation Decisions'.

Stack layers addressed:
  - Data Model: Schema enforced, DataFrames used throughout
  - Processing: Batch transformation applied across all records
"""

import logging
import pandas as pd
from pathlib import Path

from config.settings import (
    EXCLUDED_VIOLATION_TYPES,
    EXPECTED_FINES,
    ZERO_FINE_STATUSES,
    SEMESTERS,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. Filter invalid records
# ---------------------------------------------------------------------------

def remove_test_records(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop rows where Violation Type is in EXCLUDED_VIOLATION_TYPES.

    Decision: 'DO NOT USE/TESTING' (1 record found) is clearly a system artifact,
    not a real citation. Including it would distort violation-type analytics.
    """
    before = len(df)
    df = df[~df["Violation Type"].isin(EXCLUDED_VIOLATION_TYPES)].copy()
    removed = before - len(df)
    if removed:
        logger.info(f"Removed {removed} test/invalid records")
    return df


# ---------------------------------------------------------------------------
# 2. Handle negative fine amounts
# ---------------------------------------------------------------------------

def flag_negative_fines(df: pd.DataFrame) -> pd.DataFrame:
    """
    Negative fine amounts (-$50, -$60) represent credit adjustments or
    overpayment reversals. They appear only on Zero Balance records.

    Decision: preserve the original amount but add a boolean flag
    'is_credit_adjustment' so analytical queries can exclude or include
    these records as needed. We do NOT delete them — they are real events.
    """
    df["is_credit_adjustment"] = df["Fine Amount"] < 0
    count = df["is_credit_adjustment"].sum()
    if count:
        logger.info(f"Flagged {count} credit-adjustment records (negative fine amounts)")
    return df


# ---------------------------------------------------------------------------
# 3. Validate fine amounts against official fee schedule
# ---------------------------------------------------------------------------

def validate_fines(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cross-reference each citation's fine against the official KSU fee schedule.

    Records with Status in ZERO_FINE_STATUSES (Transfer, Zero Balance, Inactive)
    legitimately have $0 fines — these are not anomalies. For all other records,
    flag any fine that doesn't match the expected amount for that violation type.

    Decision: flag rather than correct. We don't know WHY an amount differs
    (partial payment, appeal reduction, data entry error), so we preserve the
    original and let downstream users decide.
    """
    expected = df["Violation Type"].map(EXPECTED_FINES)
    zero_status = df["Status"].isin(ZERO_FINE_STATUSES)

    # Fine is valid if: zero-status record, OR amount matches expected, OR it's a credit
    fine_matches = df["Fine Amount"] == expected
    df["fine_validated"] = zero_status | fine_matches | df["is_credit_adjustment"]

    anomalies = (~df["fine_validated"]).sum()
    if anomalies:
        logger.warning(f"{anomalies} records have unexpected fine amounts — flagged, not removed")
    else:
        logger.info("All fine amounts validated against fee schedule")
    return df


# ---------------------------------------------------------------------------
# 4. Standardise string columns
# ---------------------------------------------------------------------------

def standardise_strings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trim whitespace and normalise casing for string columns.
    Prevents groupby mismatches caused by trailing spaces or inconsistent case.
    """
    for col in ["Violation Type", "Status", "Location", "Campus"]:
        df[col] = df[col].str.strip()
    logger.info("String columns standardised (whitespace trimmed)")
    return df


# ---------------------------------------------------------------------------
# 5. Add derived columns
# ---------------------------------------------------------------------------

def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract temporal features and assign semester labels.

    These derived columns enable the analytical queries without requiring
    repeated date arithmetic at query time — a basic optimisation pattern.
    """
    df["date"]        = df["Timestamp"].dt.date
    df["year"]        = df["Timestamp"].dt.year
    df["month"]       = df["Timestamp"].dt.month
    df["day_of_week"] = df["Timestamp"].dt.day_name()
    df["hour"]        = df["Timestamp"].dt.hour

    # Assign semester label based on date ranges in settings
    df["semester"] = "Unknown"
    for label, start, end in SEMESTERS:
        mask = (df["Timestamp"] >= start) & (df["Timestamp"] <= end)
        df.loc[mask, "semester"] = label

    logger.info("Derived columns added: date, year, month, day_of_week, hour, semester")
    return df


# ---------------------------------------------------------------------------
# 6. Rename and select final columns
# ---------------------------------------------------------------------------

def finalise_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename columns to snake_case and select only the columns that belong
    in the final processed dataset. This enforces the schema documented
    in docs/data_dictionary.md.
    """
    df = df.rename(columns={
        "Violation Type": "violation_type",
        "Status":         "status",
        "Location":       "location",
        "Campus":         "campus",
        "Fine Amount":    "fine_amount",
        "Timestamp":      "timestamp",
    })

    final_columns = [
        "citation_id",
        "timestamp",
        "date",
        "year",
        "month",
        "day_of_week",
        "hour",
        "campus",
        "location",
        "violation_type",
        "status",
        "fine_amount",
        "is_credit_adjustment",
        "fine_validated",
        "semester",
    ]

    df = df[final_columns]
    logger.info(f"Schema finalised: {len(final_columns)} columns, {len(df):,} records")
    return df


# ---------------------------------------------------------------------------
# 7. Save processed Parquet
# ---------------------------------------------------------------------------

def save_processed_parquet(df: pd.DataFrame, output_path: Path) -> None:
    """
    Write the clean DataFrame to a second Parquet file.

    Having both raw and processed Parquet files gives you:
      - Full audit trail (raw is never modified)
      - A checkpoint: if transformation logic changes, you re-read raw Parquet
        rather than re-ingesting from Excel
      - Two examples of Parquet usage to discuss in the report
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False, engine="pyarrow", compression="snappy")
    logger.info(f"Processed Parquet written → {output_path}  ({output_path.stat().st_size / 1024:.1f} KB)")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_transformation(df: pd.DataFrame, output_path: Path) -> pd.DataFrame:
    """
    Run all transformation steps in order. Called by main.py.
    Returns the cleaned, enriched DataFrame.
    """
    logger.info("--- Transformation stage starting ---")
    df = remove_test_records(df)
    df = standardise_strings(df)
    df = flag_negative_fines(df)
    df = validate_fines(df)
    df = add_derived_columns(df)
    df = finalise_schema(df)
    save_processed_parquet(df, output_path)
    logger.info(f"--- Transformation complete: {len(df):,} clean records ---")
    return df