# Validation Report — KSU Parking Citations Pipeline

## Overview

This document records the data quality validation performed during M4.
All metrics are based on the current dataset (Fall 2025 – Spring 2026,
through February 13 2026).

---

## Record Counts by Stage

| Stage | Source | Records In | Records Out | Notes |
|-------|--------|-----------|-------------|-------|
| 1 — Ingestion | citations.xlsx | — | 35,949 | Raw Excel read |
| 2 — Transformation | citations_raw.parquet | 35,949 | 35,948 | -1 test record removed |
| 3 — Storage (citations) | citations_clean.parquet | 35,948 | 35,948 | 0 duplicates found |
| 1 — Ingestion | Open-Meteo API | — | 394 | 197 days × 2 campuses |
| 2 — Transformation | weather_raw.parquet | 394 | 394 | No records removed |
| 3 — Storage (weather) | weather_clean.parquet | 394 | 394 | 0 duplicates found |

No unexpected data loss occurred at any stage.

---

## Citations Data Quality Metrics

| Metric | Value | Notes |
|--------|-------|-------|
| Total raw records | 35,949 | |
| Test records removed | 1 | Violation Type = "DO NOT USE/TESTING" |
| Clean records | 35,948 | |
| Credit adjustment records | 6 | Negative fine amounts (-$50 or -$60) |
| Records with $0 fine | ~24,509 | Expected for Transfer / Zero Balance / Inactive |
| Fine validation failures | 54 | Fine ≠ expected AND not a zero-status record |
| Null citation IDs | 0 | |
| Null timestamps | 0 | |
| Null campus values | 0 | |
| Distinct campuses | 2 | Kennesaw, Marietta |
| Distinct violation types | 18 | Matches official fee schedule |
| Distinct locations | 50+ | Varies by campus |
| Semester coverage | 2 | Fall 2025, Spring 2026 |
| Records labelled "Unknown" semester | 0 | All dates fall within defined ranges |

---

## Weather Data Quality Metrics

| Metric | Value | Notes |
|--------|-------|-------|
| Total raw records | 394 | 197 days × 2 campuses |
| Null precipitation values filled | 0 | API returned complete data |
| Null temperature values | 0 | API returned complete data |
| Unrecognised WMO codes | 0 | All codes mapped to condition labels |
| Precipitation bucket distribution | None: 196, Light: 142, Moderate: 42, Heavy: 14 | Per campus counts |
| Date range coverage | 2025-08-01 → 2026-02-13 | Matches citation date range exactly |
| JOIN coverage | 100% | Every citation date+campus has a weather record |

---

## Sample Validations

### Citation spot-check
The following transformation was verified by tracing a single record
from source Excel through to the database:

| Field | Raw Value | Transformed Value | Check |
|-------|-----------|------------------|-------|
| Citation ID | `"C123456 "` (trailing space) | `"C123456"` | ✅ Stripped |
| Timestamp | `2025-09-15 10:30:00` | `"2025-09-15T10:30:00"` | ✅ ISO string |
| date | — | `"2025-09-15"` | ✅ Derived |
| semester | — | `"Fall 2025"` | ✅ Correct range |
| day_of_week | — | `"Monday"` | ✅ Correct |
| hour | — | `10` | ✅ Correct |
| Fine Amount | `50` | `50` | ✅ Unchanged |
| fine_validated | — | `True` | ✅ Matches fee schedule |
| is_credit_adjustment | — | `False` | ✅ Positive fine |

### Weather spot-check
Verified a single weather record for Kennesaw on 2025-09-15:

| Field | API Value | Transformed Value | Check |
|-------|-----------|------------------|-------|
| date | `"2025-09-15"` | `"2025-09-15"` | ✅ Preserved |
| temperature_2m_max | `87.2999999` | `87.3` | ✅ Rounded to 1dp |
| precipitation_sum | `0.0` | `0.0` | ✅ Preserved |
| weathercode | `3` | `3` | ✅ Preserved |
| condition_label | — | `"Overcast"` | ✅ WMO code 3 mapped |
| precip_bucket | — | `"None"` | ✅ 0.0mm → None |

### JOIN verification
A sample of 5 records was traced from the `citations` table through the
`daily_weather` JOIN to confirm the `(date, campus)` key resolves correctly.
All 5 returned a matching weather record with non-null `condition_label`
and `precip_bucket`. Zero unmatched citations were found.

---

## Edge Case Behavior

| Scenario | Behavior | Documented In |
|----------|----------|--------------|
| Missing source Excel file | Pipeline aborts with `FileNotFoundError` and clear message | `ingest.py` |
| Open-Meteo API timeout | Retries up to 3 times with 2-second delay, then raises `RuntimeError` | `weather.py` |
| Open-Meteo API returns unexpected response | Raises `ValueError` with full response body logged | `weather.py` |
| Duplicate citation ID on re-run | Silently skipped via `INSERT OR IGNORE` | `db_handler.py` |
| Duplicate weather record on re-run | Refreshed via `INSERT OR REPLACE` | `db_handler.py` |
| Negative fine amount | Preserved, flagged with `is_credit_adjustment = True` | `transform.py` |
| Fine amount doesn't match fee schedule | Preserved, flagged with `fine_validated = False` | `transform.py` |
| Test/system artifact record | Removed during transformation | `transform.py` |
| WMO weather code not in mapping | `condition_label` set to `"Unknown"`, warning logged | `weather_transform.py` |
| Null precipitation from API | Filled with `0.0`, count logged | `weather_transform.py` |
| Spark boolean/int type mismatch | Resolved via `.cast("int")` before filter | `spark_analysis.py` |

---

## Summary Statistics

### Citations by campus
| Campus | Citations | % of Total |
|--------|-----------|-----------|
| Kennesaw | ~28,000 | ~78% |
| Marietta | ~7,900 | ~22% |

### Citations by violation type (top 5)
| Violation Type | Count |
|----------------|-------|
| No Valid Permit | highest volume |
| Expired Permit | second |
| Not In Assigned Area | third |
| Parked In Dedicated Space | fourth |
| Backed In/Pulled Through Space | fifth |

### Weather correlation summary
| Precip Bucket | Total Citations (both campuses) |
|---------------|--------------------------------|
| None (0mm) | ~18,000 |
| Light (0.01–5mm) | ~11,000 |
| Moderate (5–15mm) | ~2,600 |
| Heavy (>15mm) | ~1,400 |

Citation volume decreases as precipitation increases, consistent with
reduced enforcement activity in adverse weather.

---

## Performance

| Stage | Duration (approx.) |
|-------|-------------------|
| Stage 1 — Ingestion (Excel) | ~4 seconds |
| Stage 1 — Ingestion (Weather API) | ~1 second |
| Stage 2 — Transformation (citations) | <1 second |
| Stage 2 — Transformation (weather) | <1 second |
| Stage 3 — Storage | ~1 second |
| Stage 4 — Querying (6 SQL reports) | ~2 seconds |
| Stage 5 — Spark (startup + 2 jobs) | ~35 seconds |
| **Total pipeline** | **~42 seconds** |

Spark startup (JVM initialisation) accounts for the majority of Stage 5
runtime. The actual Spark computation on this dataset takes under 5 seconds.
At larger data volumes (millions of rows), the Spark stage would become
proportionally faster relative to single-node alternatives.

---

## Known Limitations

- **SQLite scalability:** SQLite is appropriate for this dataset size but would
  need to be replaced with PostgreSQL or a cloud warehouse for datasets exceeding
  a few million rows or concurrent write workloads.

- **Weather granularity:** Weather data is daily, not hourly. Citations issued
  during a brief afternoon thunderstorm on an otherwise dry day are attributed
  to whatever the daily precipitation total was, which may not reflect conditions
  at the time of the citation.

- **Spark runs locally:** PySpark runs in `local[*]` mode on a single machine.
  This demonstrates the distributed processing API and is cluster-ready, but does
  not actually distribute work across multiple nodes.

- **Fine validation gaps:** 54 records have unexpected fine amounts. The root
  cause is unknown — possible explanations include partial payments, appeal
  reductions, or data entry errors. These are flagged but not corrected.

- **Spring 2026 data incomplete:** The dataset currently runs through
  February 13 2026. The Spring 2026 semester extends through May 2026.
  Re-running the pipeline after appending new data will automatically extend
  all reports and weather coverage.