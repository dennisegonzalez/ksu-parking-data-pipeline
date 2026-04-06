"""
queries/analytics.py

Stage 4 (final): run analytical SQL queries against the citations database
and export results as CSV files for downstream use.

Stack layers addressed:
  - Querying: Declarative SQL queries against a structured schema
  - Output: Query results persisted as CSV for portability
"""

import logging
import sqlite3
import pandas as pd
from pathlib import Path

from config.settings import DB_PATH, REPORTS_DIR

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Query definitions
# Each is a dict with a name, SQL, and description for logging/reporting.
# ---------------------------------------------------------------------------

QUERIES = [
    {
        "name": "violations_by_type",
        "description": "Citation count and total revenue by violation type",
        "sql": """
            SELECT
                violation_type,
                COUNT(*)                                        AS citation_count,
                SUM(CASE WHEN fine_amount > 0 THEN fine_amount ELSE 0 END) AS total_fines_collected,
                ROUND(AVG(CASE WHEN fine_amount > 0 THEN fine_amount END), 2) AS avg_fine
            FROM citations
            WHERE is_credit_adjustment = 0
            GROUP BY violation_type
            ORDER BY citation_count DESC
        """,
    },
    {
        "name": "monthly_volume_by_campus",
        "description": "Monthly citation volume split by campus",
        "sql": """
            SELECT
                semester,
                year,
                month,
                campus,
                COUNT(*) AS citation_count,
                SUM(CASE WHEN fine_amount > 0 THEN fine_amount ELSE 0 END) AS monthly_revenue
            FROM citations
            WHERE is_credit_adjustment = 0
            GROUP BY semester, year, month, campus
            ORDER BY year, month, campus
        """,
    },
    {
        "name": "top_locations",
        "description": "Top 20 locations by citation volume",
        "sql": """
            SELECT
                campus,
                location,
                COUNT(*)  AS citation_count,
                SUM(CASE WHEN fine_amount > 0 THEN fine_amount ELSE 0 END) AS total_fines
            FROM citations
            WHERE is_credit_adjustment = 0
            GROUP BY campus, location
            ORDER BY citation_count DESC
            LIMIT 20
        """,
    },
    {
        "name": "status_breakdown",
        "description": "Citation outcome by status and semester",
        "sql": """
            SELECT
                semester,
                status,
                COUNT(*)  AS citation_count,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY semester), 2) AS pct_of_semester
            FROM citations
            GROUP BY semester, status
            ORDER BY semester, citation_count DESC
        """,
    },
    {
        "name": "hourly_patterns",
        "description": "Citations by hour of day (peak enforcement periods)",
        "sql": """
            SELECT
                hour,
                campus,
                COUNT(*) AS citation_count
            FROM citations
            WHERE is_credit_adjustment = 0
            GROUP BY hour, campus
            ORDER BY hour, campus
        """,
    },
]


def run_queries(db_path: Path, reports_dir: Path) -> None:
    """
    Execute all analytical queries and export results to CSV.
    Called by main.py.
    """
    logger.info("--- Query stage starting ---")
    reports_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        for q in QUERIES:
            try:
                df = pd.read_sql_query(q["sql"], conn)
                out_path = reports_dir / f"{q['name']}.csv"
                df.to_csv(out_path, index=False)
                logger.info(f"  [{q['name']}] {len(df)} rows → {out_path.name}")
            except Exception as e:
                logger.error(f"  [{q['name']}] FAILED: {e}")
    finally:
        conn.close()

    logger.info(f"--- Query stage complete: {len(QUERIES)} reports in {reports_dir} ---")