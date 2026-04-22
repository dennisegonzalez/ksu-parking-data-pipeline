<!-- Improved compatibility of back to top link: See: https://github.com/othneildrew/Best-README-Template/pull/73 -->
<a id="readme-top"></a>

<div align="center">
  <h3 align="center">KSU Parking Citations — Big Data Pipeline</h3>
  <p align="center">
    CS 4265 Big Data Analytics
    <br />
    <a href="docs/data_dictionary.md"><strong>Data Dictionary »</strong></a>
    ·
    <a href="docs/architecture.md"><strong>Architecture »</strong></a>
    ·
    <a href="docs/validation.md"><strong>Validation Report »</strong></a>
  </p>
</div>

---

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li><a href="#about-the-project">About The Project</a></li>
    <li><a href="#built-with">Built With</a></li>
    <li><a href="#getting-started">Getting Started</a></li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#output-files">Output Files</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>

---

## About The Project

A five-stage batch data pipeline that ingests KSU parking citation records
and historical weather data, cleans and validates both sources, persists the
results in a structured SQLite database backed by Parquet intermediate files,
produces six analytical SQL reports, and runs distributed aggregations using
PySpark.

**Data Sources:**

| Source | Description | Access |
|--------|-------------|--------|
| KSU Parking Citations | ~35,948 records, Fall 2025 – Spring 2026, two campuses | Open records request |
| Open-Meteo Weather API | Historical daily weather for Kennesaw and Marietta | Free, no API key required |

**Pipeline Stages:**

| Stage | Description | Output |
|-------|-------------|--------|
| 1 — Ingest | Reads Excel + fetches weather API | 2 raw Parquet files |
| 2 — Transform | Cleans, validates, enriches both sources | 2 processed Parquet files |
| 3 — Store | Loads both DataFrames into SQLite | `citations.db` (2 tables) |
| 4 — Query | Runs 6 SQL analytical reports | 6 CSV files |
| 5 — Spark | Runs 2 distributed PySpark aggregations | 2 CSV files |

**Project Structure:**
```
ksu-parking-data-pipeline/
├── config/
│   └── settings.py              # Paths, constants, fee schedule, weather config
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
│   └── main.py                  # Pipeline orchestrator
├── data/
│   ├── source/                  # Place input Excel file here (gitignored)
│   ├── raw/                     # Auto-generated raw Parquet (gitignored)
│   ├── processed/               # Auto-generated clean Parquet (gitignored)
│   ├── reports/                 # Auto-generated SQL reports (gitignored)
│   │   └── spark/               # Auto-generated Spark reports (gitignored)
│   └── sample/                  # Small sample dataset for testing
├── docs/
│   ├── data_dictionary.md       # Schema and field definitions
│   ├── architecture.md          # Pipeline design and data flow
│   └── validation.md            # Data quality metrics and edge cases
├── requirements.txt
├── .gitignore
└── README.md
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Built With

| Stack Layer | Technology | Role |
|-------------|-----------|------|
| Syntax / Encoding | Parquet (pyarrow) | Columnar intermediate storage — preserves dtypes, ~60% smaller than CSV |
| Data Model | Pandas DataFrame | Schema enforcement and batch transformation |
| Data Store | SQLite | Persistent, indexed, queryable relational storage |
| Processing | Python batch pipeline | Modular 5-stage orchestration with structured logging |
| Querying | SQL (sqlite3) | Declarative cross-table analytical reports |
| Distributed Processing | PySpark (local[*]) | Distributed aggregations over Parquet — cluster-ready |
| External API | Open-Meteo | Free historical weather archive, no authentication required |

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Getting Started

### Prerequisites

- Python 3.9+
- Java 17+ (required for PySpark)
- pip

**Installing Java (macOS):**
```sh
brew install openjdk@17
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
```
Add those two export lines to `~/.zshrc` to make them permanent.

### Installation

1. Clone the repository
   ```sh
   git clone https://github.com/dennisegonzalez/ksu-parking-data-pipeline.git
   cd ksu-parking-data-pipeline
   ```

2. Create and activate a virtual environment
   ```sh
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. Install dependencies
   ```sh
   pip install -r requirements.txt
   ```

4. Place the source data
   ```
   data/source/citations.xlsx
   ```
   The filename must match exactly. The weather data is fetched automatically
   from the Open-Meteo API at runtime — no additional setup required.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Usage

Run the full pipeline:
```sh
python src/main.py
```

The pipeline runs all five stages automatically and logs progress at each step:

```
STAGE 1: Ingestion       — reads Excel + fetches weather API
STAGE 2: Transformation  — cleans, validates, enriches both sources
STAGE 3: Storage         — loads citations + weather into SQLite
STAGE 4: Querying        — runs 6 SQL reports → CSV
STAGE 5: Spark Analysis  — runs 2 PySpark aggregations → CSV
```

**The pipeline is idempotent** — running it twice on the same data produces
the same result. Duplicate citation IDs are silently skipped; weather records
are refreshed in place.

**Expected runtime:** ~42 seconds (Spark JVM startup accounts for ~35s of this).

**Adding new semester data:**
1. Append new rows to `data/source/citations.xlsx` (keep same column structure)
2. Add the new semester date range to `config/settings.py → SEMESTERS`
3. Re-run `python src/main.py`

New citation records are inserted; existing records are unchanged.
Weather data is automatically extended to cover the new date range.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Output Files

| File | Stage | Description |
|------|-------|-------------|
| `data/raw/citations_raw.parquet` | 1 | Raw columnar snapshot of source Excel |
| `data/raw/weather_raw.parquet` | 1 | Raw weather data from Open-Meteo API |
| `data/processed/citations_clean.parquet` | 2 | Cleaned, validated, enriched citations |
| `data/processed/weather_clean.parquet` | 2 | Normalised weather with labels and buckets |
| `data/citations.db` | 3 | SQLite database (citations + daily_weather tables) |
| `data/reports/violations_by_type.csv` | 4 | Citation count and revenue by violation |
| `data/reports/monthly_volume_by_campus.csv` | 4 | Monthly trends per campus |
| `data/reports/top_locations.csv` | 4 | Top 20 locations by citation volume |
| `data/reports/status_breakdown.csv` | 4 | Outcome distribution by semester |
| `data/reports/hourly_patterns.csv` | 4 | Citations by hour of day |
| `data/reports/weather_citation_correlation.csv` | 4 | Citation volume by weather condition |
| `data/reports/spark/violation_summary_spark.csv` | 5 | Violation summary by campus (PySpark) |
| `data/reports/spark/weather_hourly_spark.csv` | 5 | Hourly citations by weather bucket (PySpark) |

For full field definitions see [`docs/data_dictionary.md`](docs/data_dictionary.md).

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Roadmap

- [x] Stage 1: Ingestion (Excel → Parquet)
- [x] Stage 1: Second source (Open-Meteo Weather API → Parquet)
- [x] Stage 2: Transformation — citations (clean, validate, enrich)
- [x] Stage 2: Transformation — weather (normalise, label, bucket)
- [x] Stage 3: Storage (Parquet → SQLite, two tables)
- [x] Stage 4: Analytical queries (SQL → 6 CSV reports)
- [x] Stage 5: Distributed processing (PySpark → 2 CSV reports)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Contact

Project Link: [https://github.com/dennisegonzalez/ksu-parking-data-pipeline](https://github.com/dennisegonzalez/ksu-parking-data-pipeline)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Acknowledgments

* [KSU Parking Services](https://parking.kennesaw.edu) — source citation data
* [Open-Meteo](https://open-meteo.com) — free historical weather API
* [pandas documentation](https://pandas.pydata.org/docs/)
* [Apache Arrow / pyarrow](https://arrow.apache.org/docs/python/)
* [Apache Spark / PySpark](https://spark.apache.org/docs/latest/api/python/)
* [SQLite documentation](https://www.sqlite.org/docs.html)

<p align="right">(<a href="#readme-top">back to top</a>)</p>