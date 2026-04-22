# Architecture — KSU Parking Citations Pipeline

## Overview

A five-stage batch data pipeline that ingests university parking citation records
and historical weather data, cleans and validates both sources, persists them in a
structured SQLite database backed by Parquet intermediate files, produces analytical
SQL reports, and runs distributed aggregations using PySpark.

---

## Pipeline Stages

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│                                                                 │
│   citations.xlsx          Open-Meteo Archive API               │
│   (KSU Parking Services)  (free, no key required)              │
└────────────┬──────────────────────┬────────────────────────────┘
             │                      │
             ▼                      ▼
┌─────────────────────────────────────────────────────────────────┐
│  STAGE 1 — INGESTION  (src/ingestion/)                          │
│                                                                 │
│   ingest.py          reads Excel → DataFrame                    │
│   weather.py         fetches API → DataFrame                    │
│                                                                 │
│   Output:  data/raw/citations_raw.parquet                       │
│            data/raw/weather_raw.parquet                         │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  STAGE 2 — TRANSFORMATION  (src/processing/)                    │
│                                                                 │
│   transform.py         clean, validate, enrich citations        │
│   weather_transform.py normalise, bucket, label weather         │
│                                                                 │
│   Output:  data/processed/citations_clean.parquet               │
│            data/processed/weather_clean.parquet                 │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  STAGE 3 — STORAGE  (src/storage/)                              │
│                                                                 │
│   db_handler.py    loads both DataFrames into SQLite            │
│                                                                 │
│   Tables:  citations      (35,948 rows, PK: citation_id)        │
│            daily_weather  (394 rows, PK: date + campus)         │
│                                                                 │
│   Output:  data/citations.db                                    │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  STAGE 4 — QUERYING  (src/queries/)                             │
│                                                                 │
│   analytics.py     6 SQL queries → CSV reports                  │
│                    (includes cross-table weather join)          │
│                                                                 │
│   Output:  data/reports/*.csv  (6 files)                        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  STAGE 5 — SPARK ANALYSIS  (src/processing/)                    │
│                                                                 │
│   spark_analysis.py    reads processed Parquet directly         │
│                        runs 2 distributed aggregations          │
│                        (violation summary + weather/hourly      │
│                         cross-source join)                      │
│                                                                 │
│   Output:  data/reports/spark/*.csv  (2 files)                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

| Layer | Technology | Rationale |
|-------|-----------|-----------|
| Syntax / Encoding | Apache Parquet (pyarrow) | Columnar, compressed intermediate storage. Preserves dtypes exactly. Native format for both Pandas and Spark — no conversion needed between stages. |
| Data Model | Pandas DataFrame | Schema enforcement and batch transformations. Well-suited for the cleaning and validation workload at this data volume. |
| Data Store | SQLite | Persistent, zero-config relational storage. Supports indexed queries and window functions. Appropriate for single-node analytical workloads. |
| Processing | Python batch pipeline | Modular stage-based architecture. Each stage is independently testable and produces auditable intermediate outputs. |
| Querying | SQL (sqlite3) | Declarative cross-table queries. The `daily_weather` JOIN enables weather-enriched analytics without duplicating weather data into the citations table. |
| Distributed Processing | PySpark (local[*]) | Reads Parquet natively. Demonstrates the distributed aggregation layer that would replace SQLite at scale. Cluster-ready: swapping `local[*]` for a cluster master URL requires no code changes. |
| External API | Open-Meteo | Free historical weather archive. No authentication required. Returns daily aggregates (temp, precipitation, WMO code) for any coordinates and date range. |

---

## Data Flow

```
citations.xlsx
    │
    ├── read_excel()           raw DataFrame (7 cols, 35,949 rows)
    │       │
    │       ├── save_raw_parquet()     → citations_raw.parquet
    │       │
    │       └── run_transformation()
    │               │
    │               ├── remove_test_records()     -1 row
    │               ├── standardise_strings()
    │               ├── flag_negative_fines()     +is_credit_adjustment
    │               ├── validate_fines()          +fine_validated
    │               ├── add_derived_columns()     +date, year, month,
    │               │                              day_of_week, hour,
    │               │                              semester
    │               ├── finalise_schema()         15 cols, 35,948 rows
    │               └── save_processed_parquet()  → citations_clean.parquet
    │
Open-Meteo API
    │
    ├── fetch_weather()        raw DataFrame (6 cols, 394 rows)
    │       │
    │       ├── save_weather_raw_parquet()  → weather_raw.parquet
    │       │
    │       └── run_weather_transformation()
    │               │
    │               ├── parse_dates()
    │               ├── round_floats()
    │               ├── handle_missing()
    │               ├── add_condition_label()     +condition_label
    │               ├── add_precip_bucket()       +precip_bucket
    │               ├── finalise_schema()         8 cols, 394 rows
    │               └── save_processed_parquet()  → weather_clean.parquet
    │
SQLite (citations.db)
    │
    ├── citations table        35,948 rows, 5 indexes
    ├── daily_weather table    394 rows, 2 indexes
    │
    ├── SQL queries (Stage 4)
    │       ├── violations_by_type.csv
    │       ├── monthly_volume_by_campus.csv
    │       ├── top_locations.csv
    │       ├── status_breakdown.csv
    │       ├── hourly_patterns.csv
    │       └── weather_citation_correlation.csv  ← cross-table JOIN
    │
    └── PySpark (Stage 5, reads Parquet directly)
            ├── violation_summary_spark.csv
            └── weather_hourly_spark.csv
```

---

## Repository Structure

```
ksu-parking-data-pipeline/
├── config/
│   └── settings.py              # All paths, constants, fee schedule,
│                                #   weather coordinates, WMO labels
├── src/
│   ├── ingestion/
│   │   ├── ingest.py            # Stage 1: Excel → Parquet
│   │   └── weather.py           # Stage 1: Open-Meteo API → Parquet
│   ├── processing/
│   │   ├── transform.py         # Stage 2: clean, validate, enrich citations
│   │   ├── weather_transform.py # Stage 2: normalise, bucket weather data
│   │   └── spark_analysis.py    # Stage 5: PySpark distributed aggregations
│   ├── storage/
│   │   └── db_handler.py        # Stage 3: DataFrames → SQLite
│   ├── queries/
│   │   └── analytics.py         # Stage 4: SQL → CSV reports
│   └── main.py                  # Pipeline orchestrator (all 5 stages)
├── data/
│   ├── source/                  # Input Excel file (gitignored)
│   ├── raw/                     # Raw Parquet files (gitignored)
│   ├── processed/               # Cleaned Parquet files (gitignored)
│   ├── reports/                 # SQL CSV reports (gitignored)
│   │   └── spark/               # PySpark CSV reports (gitignored)
│   └── sample/                  # Small sample for testing (committed)
├── docs/
│   ├── data_dictionary.md       # Schema documentation
│   ├── architecture.md          # This file
│   └── validation.md            # Data quality report
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Idempotency

The pipeline is safe to re-run on the same data:

- **Citations:** `INSERT OR IGNORE` — existing `citation_id` values are silently skipped
- **Weather:** `INSERT OR REPLACE` — weather records are refreshed, allowing for late corrections from the API
- **Parquet files:** overwritten on each run (raw zone is always a faithful snapshot of the current source)
- **CSV reports:** overwritten on each run

---

## Scalability Notes

The current implementation runs on a single machine. The architecture is designed
to scale with minimal code changes:

- **Parquet** is the native storage format for distributed systems (S3, HDFS, GCS)
- **PySpark** already runs in `local[*]` mode — changing the master URL to a cluster
  address (`yarn`, `spark://host:7077`) requires no other code changes
- **SQLite** would be replaced by PostgreSQL or a cloud data warehouse (BigQuery,
  Redshift) at scale — the SQL queries are standard ANSI SQL and would run unchanged
- **Open-Meteo** batches the full date range in two API calls — this pattern scales
  to any date range without modification