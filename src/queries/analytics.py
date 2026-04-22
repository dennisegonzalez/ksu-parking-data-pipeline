"""
src/queries/analytics.py

Stage 4 of the pipeline: run analytical SQL queries against the citations
database and export results as CSV files for downstream use.

Queries:
  1. violations_by_type              — citation count and revenue by violation (unchanged)
  2. monthly_volume_by_campus        — monthly trends per campus (unchanged)
  3. top_locations                   — top 20 locations by volume (unchanged)
  4. status_breakdown                — outcome distribution by semester (unchanged)
  5. hourly_patterns                 — citations by hour of day (unchanged)
  6. weather_citation_correlation    — citation volume and revenue by weather
                                       condition (new for M4)

Stack layers addressed:
  - Querying: Declarative SQL queries against a structured schema
  - Output: Query results persisted as CSV for portability
"""

import logging
import sqlite3
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Query definitions
# ---------------------------------------------------------------------------

QUERIES = [
    {
        "name": "violations_by_type",
        "description": "Citation count and total revenue by violation type",
        "sql": """
            SELECT
                violation_type,
                COUNT(*)                                                        AS citation_count,
                SUM(CASE WHEN fine_amount > 0 THEN fine_amount ELSE 0 END)     AS total_fines_collected,
                ROUND(AVG(CASE WHEN fine_amount > 0 THEN fine_amount END), 2)  AS avg_fine
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
                COUNT(*)                                                    AS citation_count,
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
                COUNT(*) AS citation_count,
                ROUND(
                    100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY semester),
                    2
                ) AS pct_of_semester
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
    {
        "name": "weather_citation_correlation",
        "description": "Citation volume and revenue by precipitation bucket and condition label",
        "sql": """
            SELECT
                w.precip_bucket,
                w.condition_label,
                w.campus,
                COUNT(c.citation_id)                                            AS citation_count,
                SUM(CASE WHEN c.fine_amount > 0 THEN c.fine_amount ELSE 0 END) AS total_fines,
                ROUND(AVG(w.temperature_2m_max), 1)                             AS avg_high_temp_f,
                ROUND(AVG(w.precipitation_sum), 2)                              AS avg_precipitation_mm
            FROM daily_weather w
            LEFT JOIN citations c
                ON  c.date   = w.date
                AND c.campus = w.campus
                AND c.is_credit_adjustment = 0
            GROUP BY w.precip_bucket, w.condition_label, w.campus
            ORDER BY citation_count DESC
        """,
    },
]


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

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