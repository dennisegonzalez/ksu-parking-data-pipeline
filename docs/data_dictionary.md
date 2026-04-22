# Data Dictionary — KSU Parking Citations Pipeline

---

## Data Sources

### Source 1: `data/source/citations.xlsx`
KSU Parking Services citation records obtained via open records request.

| Original Column | Type | Notes |
|-----------------|------|-------|
| Citation ID | String | Unique identifier per citation (trailing space stripped) |
| Timestamp | Datetime | Date and time citation was issued |
| Location | String | Parking lot or structure name |
| Campus | String | Kennesaw or Marietta |
| Violation Type | String | Category of parking violation |
| Status | String | Current payment/disposition status |
| Fine Amount | Integer | Dollar amount (may be 0 or negative — see Data Quality Notes) |

### Source 2: Open-Meteo Historical Weather API
Free historical weather archive. No API key required. Fetched automatically
at pipeline runtime for the exact date range present in the citations data.

- **Endpoint:** `https://archive-api.open-meteo.com/v1/archive`
- **Granularity:** Daily aggregates (one row per campus per date)
- **Campuses:** Kennesaw (34.0285°N, 84.5742°W) and Marietta (33.9526°N, 84.5499°W)
- **Temperature unit:** Fahrenheit
- **Timezone:** America/New_York

| API Field | Type | Notes |
|-----------|------|-------|
| time | String | ISO date string (YYYY-MM-DD) |
| temperature_2m_max | Float | Daily high temperature (°F) |
| temperature_2m_min | Float | Daily low temperature (°F) |
| precipitation_sum | Float | Total daily precipitation (mm) |
| weathercode | Integer | WMO weather interpretation code |

---

## Final Schemas

### `citations` table / `citations_clean.parquet`

| Column | Type | Description |
|--------|------|-------------|
| citation_id | TEXT (PK) | Unique citation identifier |
| timestamp | TEXT | ISO datetime string |
| date | TEXT | Date only (YYYY-MM-DD) — join key to daily_weather |
| year | INTEGER | Calendar year |
| month | INTEGER | Month (1–12) |
| day_of_week | TEXT | Day name (e.g., Monday) |
| hour | INTEGER | Hour of day (0–23) |
| campus | TEXT | Kennesaw or Marietta — join key to daily_weather |
| location | TEXT | Parking area name |
| violation_type | TEXT | Standardised violation category |
| status | TEXT | Payment/disposition status |
| fine_amount | INTEGER | Fine in dollars (may be 0 or negative) |
| is_credit_adjustment | BOOLEAN | True if fine_amount < 0 (credit/reversal record) |
| fine_validated | BOOLEAN | True if fine matches official fee schedule (or is a zero-status record) |
| semester | TEXT | Academic semester label |

**Indexes:** `campus`, `violation_type`, `semester`, `status`, `date`

---

### `daily_weather` table / `weather_clean.parquet`

One row per campus per date. Joins to `citations` on `(date, campus)`.

| Column | Type | Description |
|--------|------|-------------|
| date | TEXT (PK part) | ISO date string (YYYY-MM-DD) |
| campus | TEXT (PK part) | Kennesaw or Marietta |
| temperature_2m_max | REAL | Daily high temperature (°F), rounded to 1 decimal |
| temperature_2m_min | REAL | Daily low temperature (°F), rounded to 1 decimal |
| precipitation_sum | REAL | Total daily precipitation (mm), rounded to 2 decimals |
| weathercode | INTEGER | WMO weather interpretation code |
| condition_label | TEXT | Human-readable weather description (e.g., "Moderate rain") |
| precip_bucket | TEXT | Precipitation category: None / Light / Moderate / Heavy |

**Primary key:** `(date, campus)` — composite, guarantees one row per campus per day.
**Indexes:** `date`, `campus`

#### Precipitation Bucket Definitions

| Bucket | Precipitation Range |
|--------|-------------------|
| None | Exactly 0.0 mm |
| Light | 0.01 – 5.0 mm |
| Moderate | 5.01 – 15.0 mm |
| Heavy | > 15.0 mm |

#### WMO Weather Code Labels (subset)

| Code | Label |
|------|-------|
| 0 | Clear sky |
| 1 | Mainly clear |
| 2 | Partly cloudy |
| 3 | Overcast |
| 45 | Fog |
| 51 / 53 / 55 | Light / Moderate / Heavy drizzle |
| 61 / 63 / 65 | Slight / Moderate / Heavy rain |
| 71 / 73 / 75 | Slight / Moderate / Heavy snow |
| 80 / 81 / 82 | Slight / Moderate / Violent showers |
| 95 | Thunderstorm |

Full mapping defined in `config/settings.py → WMO_CODE_LABELS`.

---

## Data Quality Notes

### Negative fine amounts
Six records have negative fine amounts (-$50 or -$60). These appear exclusively
on Zero Balance records and represent credit adjustments or overpayment reversals.
They are preserved with `is_credit_adjustment = True` and excluded from all
revenue aggregations.

### Zero fine amounts
Records with a fine amount of $0 are expected for three status types:
- **Transfer** — citation transferred to another party
- **Zero Balance** — balance cleared (paid, appealed, or waived)
- **Inactive** — citation no longer active

### Test records
One record with Violation Type = "DO NOT USE/TESTING" was removed during
transformation. It is a system artifact with no corresponding real-world event.

### Fine validation
Each citation's fine was compared against the official KSU Parking Services fee
schedule. Records where the fine does not match the expected amount AND the status
is not a zero-fine status are flagged with `fine_validated = False`. 54 such
records were found and flagged — they are preserved, not removed.

### Weather data coverage
Weather is fetched for the full date range of the citations dataset
(2025-08-01 through the most recent citation date). Both campuses are fetched
independently since Kennesaw and Marietta are approximately 15 miles apart and
can experience meaningfully different local weather conditions.

---

## Reports: `data/reports/`

All SQL reports exclude credit adjustment records (`is_credit_adjustment = 0`)
unless noted. Revenue figures count only positive fine amounts.

---

### `violations_by_type.csv`
One row per violation type across the full dataset.

| Column | Description |
|--------|-------------|
| violation_type | The violation category (e.g., No Valid Permit) |
| citation_count | Total number of citations issued for this violation type |
| total_fines_collected | Sum of all positive fine amounts for this violation type (dollars) |
| avg_fine | Average fine amount among citations with a positive fine (dollars) |

---

### `monthly_volume_by_campus.csv`
One row per semester + year + month + campus combination.

| Column | Description |
|--------|-------------|
| semester | Academic semester label (e.g., Fall 2025) |
| year | Calendar year |
| month | Month number (1–12) |
| campus | Kennesaw or Marietta |
| citation_count | Number of citations issued that month on that campus |
| monthly_revenue | Sum of positive fine amounts issued that month on that campus (dollars) |

---

### `top_locations.csv`
Top 20 parking locations by citation volume, across both campuses.

| Column | Description |
|--------|-------------|
| campus | Kennesaw or Marietta |
| location | Parking lot or structure name (e.g., East Deck, Lot B) |
| citation_count | Total citations issued at this location |
| total_fines | Sum of positive fine amounts issued at this location (dollars) |

---

### `status_breakdown.csv`
One row per semester + status combination, showing how citations were resolved.

| Column | Description |
|--------|-------------|
| semester | Academic semester label |
| status | Citation disposition (Unpaid, Transfer, Zero Balance, Inactive, Appeal Balance Due) |
| citation_count | Number of citations with this status in this semester |
| pct_of_semester | Percentage of all citations in that semester with this status |

**Status definitions:**

| Status | Meaning |
|--------|---------|
| Unpaid | Fine has been issued and not yet paid |
| Transfer | Citation transferred to another party (fine amount becomes $0) |
| Zero Balance | Balance cleared — paid, appealed, or waived |
| Inactive | Citation is no longer active |
| Appeal Balance Due | Appeal was filed; a reduced balance remains |

---

### `hourly_patterns.csv`
Citation volume broken down by hour of day and campus.

| Column | Description |
|--------|-------------|
| hour | Hour of day the citation was issued (0–23, 24-hour clock) |
| campus | Kennesaw or Marietta |
| citation_count | Number of citations issued during this hour on this campus |

---

### `weather_citation_correlation.csv` *(new — M4)*
Citation volume and revenue grouped by weather condition and precipitation bucket.
Joins `citations` to `daily_weather` on `(date, campus)`.
Uses a LEFT JOIN from weather so that days with zero citations still appear.

| Column | Description |
|--------|-------------|
| precip_bucket | Precipitation category: None / Light / Moderate / Heavy |
| condition_label | Human-readable weather description (e.g., "Moderate rain") |
| campus | Kennesaw or Marietta |
| citation_count | Number of citations issued under this weather condition |
| total_fines | Sum of positive fine amounts under this condition (dollars) |
| avg_high_temp_f | Average daily high temperature on these days (°F) |
| avg_precipitation_mm | Average precipitation on these days (mm) |

**Key finding:** Citation volume is highest on dry/overcast days and drops
significantly as precipitation increases. Snow days produced zero citations at
both campuses.

---

## Spark Reports: `data/reports/spark/`

Generated by Stage 5 (`src/processing/spark_analysis.py`) using PySpark.
These reports read the processed Parquet files directly, bypassing SQLite,
and demonstrate distributed aggregation on the same dataset.

---

### `violation_summary_spark.csv`
Mirrors `violations_by_type.csv` but adds campus as a grouping dimension.
Produced using the PySpark DataFrame API instead of SQL.

| Column | Description |
|--------|-------------|
| violation_type | The violation category |
| campus | Kennesaw or Marietta |
| citation_count | Number of citations for this violation type on this campus |
| total_fines_collected | Sum of positive fine amounts (dollars) |
| avg_fine | Average fine among citations with a positive fine (dollars) |

---

### `weather_hourly_spark.csv`
Cross-source join between citations and weather in Spark. Shows average
citations per day by hour of day and precipitation bucket.

| Column | Description |
|--------|-------------|
| hour | Hour of day (0–23) |
| precip_bucket | Precipitation category: None / Light / Moderate / Heavy |
| citation_count | Total citations in this hour + weather bucket combination |
| days_in_bucket | Number of distinct days in this precipitation bucket |
| avg_citations_per_day | citation_count / days_in_bucket — normalises for unequal bucket sizes |

---

## Official Fee Schedule

Source: KSU Parking Services Violation Descriptions and Costs

| Violation Type | Expected Fine |
|----------------|--------------|
| Area Not Designed For Parking | $50 |
| Boot Fee | $60 |
| Failed To Pay At Visitor Lot | $35 |
| Fire Lane - Blocking/Impeding | $60 |
| No Valid Permit | $50 |
| Parked In Dedicated Space | $50 |
| Parked In Loading Zone | $35 |
| Parked In Tow Zone | $50 |
| Parked In Wrong Direction | $35 |
| Tow Fee | $60 |
| Unauthorized Use of Permit | $60 |
| Not Parked Between Lines | $50 |
| Not In Assigned Area | $50 |
| Parking For Clinic Patients Only | $50 |
| Abandoned Vehicle | $50 |
| Backed In/Pulled Through Space | $35 |
| Expired Permit | $50 |
| Expired Hourly Parking | $35 |
| No License Plate Displayed | $35 |
| Parked In Visitor Space | $50 |