"""
storage/db_handler.py

Stage 3 of the pipeline: load the processed DataFrame into a SQLite database
with an enforced schema.

Stack layers addressed:
  - Data Store: SQLite with defined schema, primary key, and indexes
  - Storage: persistent, query-ready storage that survives between pipeline runs
"""

import logging
import sqlite3
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)

# DDL for the citations table.
# citation_id is the PRIMARY KEY — guarantees no duplicates across runs.
# Indexes on campus, violation_type, and semester speed up the analytical queries.
CREATE_CITATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS citations (
    citation_id         TEXT PRIMARY KEY,
    timestamp           TEXT NOT NULL,
    date                TEXT,
    year                INTEGER,
    month               INTEGER,
    day_of_week         TEXT,
    hour                INTEGER,
    campus              TEXT NOT NULL,
    location            TEXT,
    violation_type      TEXT NOT NULL,
    status              TEXT NOT NULL,
    fine_amount         INTEGER NOT NULL,
    is_credit_adjustment INTEGER DEFAULT 0,
    fine_validated      INTEGER DEFAULT 1,
    semester            TEXT
);

CREATE INDEX IF NOT EXISTS idx_campus          ON citations(campus);
CREATE INDEX IF NOT EXISTS idx_violation_type  ON citations(violation_type);
CREATE INDEX IF NOT EXISTS idx_semester        ON citations(semester);
CREATE INDEX IF NOT EXISTS idx_status          ON citations(status);
CREATE INDEX IF NOT EXISTS idx_date            ON citations(date);
"""


def get_connection(db_path: Path) -> sqlite3.Connection:
    """Return a SQLite connection, creating the DB file if it doesn't exist."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # allows column-name access on query results
    return conn


def initialise_schema(conn: sqlite3.Connection) -> None:
    """Create the citations table and indexes if they don't already exist."""
    conn.executescript(CREATE_CITATIONS_TABLE)
    conn.commit()
    logger.info("Database schema initialised (table + indexes)")


def load_dataframe(df: pd.DataFrame, conn: sqlite3.Connection) -> int:
    """
    Load the clean DataFrame into the citations table.

    Uses INSERT OR IGNORE so that re-running the pipeline on the same data
    is safe — existing citation_ids are silently skipped rather than raising
    an error or creating duplicates. This is the idempotency guarantee.

    Returns the number of new rows inserted.
    """
    before = _count_rows(conn)

    # Convert boolean columns to int for SQLite compatibility
    df_load = df.copy()
    df_load["is_credit_adjustment"] = df_load["is_credit_adjustment"].astype(int)
    df_load["fine_validated"]       = df_load["fine_validated"].astype(int)
    df_load["date"]                 = df_load["date"].astype(str)
    df_load["timestamp"]            = df_load["timestamp"].astype(str)

    # Write to a temp table first, then INSERT OR IGNORE into citations.
    # This avoids pandas' default behaviour (which doesn't support ON CONFLICT).
    df_load.to_sql("citations_staging", conn, if_exists="replace", index=False)

    conn.execute("""
        INSERT OR IGNORE INTO citations
        SELECT * FROM citations_staging
    """)
    conn.execute("DROP TABLE IF EXISTS citations_staging")
    conn.commit()

    after = _count_rows(conn)
    inserted = after - before
    logger.info(f"Loaded {inserted:,} new records into citations table (total: {after:,})")
    return inserted


def _count_rows(conn: sqlite3.Connection) -> int:
    """Return the current row count of the citations table."""
    return conn.execute("SELECT COUNT(*) FROM citations").fetchone()[0]


def run_storage(df: pd.DataFrame, db_path: Path) -> None:
    """
    Full storage stage: connect, initialise schema, load data.
    Called by main.py.
    """
    logger.info("--- Storage stage starting ---")
    conn = get_connection(db_path)
    try:
        initialise_schema(conn)
        load_dataframe(df, conn)
    finally:
        conn.close()
    logger.info(f"--- Storage complete: database at {db_path} ---")