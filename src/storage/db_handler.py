"""
src/storage/db_handler.py

Stage 3 of the pipeline: load both processed DataFrames into SQLite with
enforced schemas.

Tables created:
  - citations     : one row per parking citation (unchanged from M3)
  - daily_weather : one row per campus per date (new for M4)

The two tables join on (date, campus), enabling weather-enriched analytics
in Stage 4.

Stack layers addressed:
  - Data Store: SQLite with defined schemas, primary keys, and indexes
  - Storage: persistent, query-ready storage that survives between pipeline runs
"""

import logging
import sqlite3
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DDL — citations table  (unchanged from M3)
# ---------------------------------------------------------------------------

CREATE_CITATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS citations (
    citation_id          TEXT PRIMARY KEY,
    timestamp            TEXT NOT NULL,
    date                 TEXT,
    year                 INTEGER,
    month                INTEGER,
    day_of_week          TEXT,
    hour                 INTEGER,
    campus               TEXT NOT NULL,
    location             TEXT,
    violation_type       TEXT NOT NULL,
    status               TEXT NOT NULL,
    fine_amount          INTEGER NOT NULL,
    is_credit_adjustment INTEGER DEFAULT 0,
    fine_validated       INTEGER DEFAULT 1,
    semester             TEXT
);

CREATE INDEX IF NOT EXISTS idx_campus         ON citations(campus);
CREATE INDEX IF NOT EXISTS idx_violation_type ON citations(violation_type);
CREATE INDEX IF NOT EXISTS idx_semester       ON citations(semester);
CREATE INDEX IF NOT EXISTS idx_status         ON citations(status);
CREATE INDEX IF NOT EXISTS idx_date           ON citations(date);
"""


# ---------------------------------------------------------------------------
# DDL — daily_weather table  (new for M4)
# ---------------------------------------------------------------------------
# Primary key is (date, campus) — one row per campus per day.
# Indexed on date and campus separately to support the JOIN in analytics.py.
# ---------------------------------------------------------------------------

CREATE_WEATHER_TABLE = """
CREATE TABLE IF NOT EXISTS daily_weather (
    date               TEXT NOT NULL,
    campus             TEXT NOT NULL,
    temperature_2m_max REAL,
    temperature_2m_min REAL,
    precipitation_sum  REAL,
    weathercode        INTEGER,
    condition_label    TEXT,
    precip_bucket      TEXT,
    PRIMARY KEY (date, campus)
);

CREATE INDEX IF NOT EXISTS idx_weather_date   ON daily_weather(date);
CREATE INDEX IF NOT EXISTS idx_weather_campus ON daily_weather(campus);
"""


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_connection(db_path: Path) -> sqlite3.Connection:
    """Return a SQLite connection, creating the DB file if it doesn't exist."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Schema initialisation
# ---------------------------------------------------------------------------

def initialise_schema(conn: sqlite3.Connection) -> None:
    """Create all tables and indexes if they don't already exist."""
    conn.executescript(CREATE_CITATIONS_TABLE)
    conn.executescript(CREATE_WEATHER_TABLE)
    conn.commit()
    logger.info("Database schema initialised (citations + daily_weather tables + indexes)")


# ---------------------------------------------------------------------------
# Citations loader  (unchanged from M3)
# ---------------------------------------------------------------------------

def load_citations(df: pd.DataFrame, conn: sqlite3.Connection) -> int:
    """
    Load the clean citations DataFrame into the citations table.

    Uses INSERT OR IGNORE so that re-running the pipeline on the same data
    is safe — existing citation_ids are silently skipped rather than raising
    an error or creating duplicates. This is the idempotency guarantee.

    Returns the number of new rows inserted.
    """
    before = _count_rows(conn, "citations")

    df_load = df.copy()
    df_load["is_credit_adjustment"] = df_load["is_credit_adjustment"].astype(int)
    df_load["fine_validated"]       = df_load["fine_validated"].astype(int)
    df_load["date"]                 = df_load["date"].astype(str)
    df_load["timestamp"]            = df_load["timestamp"].astype(str)

    df_load.to_sql("citations_staging", conn, if_exists="replace", index=False)
    conn.execute("""
        INSERT OR IGNORE INTO citations
        SELECT * FROM citations_staging
    """)
    conn.execute("DROP TABLE IF EXISTS citations_staging")
    conn.commit()

    inserted = _count_rows(conn, "citations") - before
    logger.info(
        f"Citations: {inserted:,} new records inserted "
        f"(total: {_count_rows(conn, 'citations'):,})"
    )
    return inserted


# ---------------------------------------------------------------------------
# Weather loader  (new for M4)
# ---------------------------------------------------------------------------

def load_weather(df: pd.DataFrame, conn: sqlite3.Connection) -> int:
    """
    Load the clean weather DataFrame into the daily_weather table.

    Uses INSERT OR REPLACE so that if the pipeline is re-run with updated
    weather data (e.g. a late-arriving correction from Open-Meteo), the
    record is refreshed rather than silently skipped.

    The primary key (date, campus) guarantees no duplicate rows per day.

    Returns the number of rows affected.
    """
    before = _count_rows(conn, "daily_weather")

    df_load = df.copy()
    df_load["date"] = df_load["date"].astype(str)

    df_load.to_sql("weather_staging", conn, if_exists="replace", index=False)
    conn.execute("""
        INSERT OR REPLACE INTO daily_weather
        SELECT * FROM weather_staging
    """)
    conn.execute("DROP TABLE IF EXISTS weather_staging")
    conn.commit()

    after = _count_rows(conn, "daily_weather")
    logger.info(
        f"Weather: {after:,} rows in daily_weather table "
        f"(+{after - before:,} vs before this run)"
    )
    return after


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _count_rows(conn: sqlite3.Connection, table: str) -> int:
    """Return the current row count of the given table."""
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_storage(
    df_citations: pd.DataFrame,
    df_weather: pd.DataFrame,
    db_path: Path,
) -> None:
    """
    Full storage stage: connect, initialise schema, load both tables.
    Called by main.py.
    """
    logger.info("--- Storage stage starting ---")
    conn = get_connection(db_path)
    try:
        initialise_schema(conn)
        load_citations(df_citations, conn)
        load_weather(df_weather, conn)
    finally:
        conn.close()
    logger.info(f"--- Storage complete: database at {db_path} ---")