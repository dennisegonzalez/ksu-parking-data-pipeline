from pathlib import Path

# Root of the project (two levels up from this file)
ROOT_DIR = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Data directories
# ---------------------------------------------------------------------------
RAW_DIR       = ROOT_DIR / "data" / "raw"
PROCESSED_DIR = ROOT_DIR / "data" / "processed"
REPORTS_DIR   = ROOT_DIR / "data" / "reports"
DB_PATH       = ROOT_DIR / "data" / "citations.db"

# ---------------------------------------------------------------------------
# Citations source paths
# ---------------------------------------------------------------------------

# Source file (update this path when new semester data arrives)
SOURCE_EXCEL      = ROOT_DIR / "data" / "source" / "citations.xlsx"

# Raw Parquet output (Stage 1 output)
RAW_PARQUET       = RAW_DIR / "citations_raw.parquet"

# Processed Parquet output (Stage 2 output)
PROCESSED_PARQUET = PROCESSED_DIR / "citations_clean.parquet"

# ---------------------------------------------------------------------------
# Weather source paths  (Open-Meteo — no API key required)
# ---------------------------------------------------------------------------

# Raw weather Parquet written immediately after the API fetch (Stage 1)
WEATHER_RAW_PARQUET       = RAW_DIR / "weather_raw.parquet"

# Processed weather Parquet after cleaning/normalisation (Stage 2)
WEATHER_PROCESSED_PARQUET = PROCESSED_DIR / "weather_clean.parquet"

# Open-Meteo historical weather API endpoint
OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"

# Campus coordinates used to fetch weather.
# Kennesaw and Marietta are ~15 miles apart so we fetch for each separately
# and join to citations on (date, campus).
CAMPUS_COORDINATES = {
    "Kennesaw": {"latitude": 34.0285, "longitude": -84.5742},
    "Marietta": {"latitude": 33.9526, "longitude": -84.5499},
}

# Open-Meteo variables to request.
# daily granularity is enough — citations timestamps are spread across the day
# so we join on date rather than hour.
WEATHER_VARIABLES = [
    "temperature_2m_max",       # °F high for the day
    "temperature_2m_min",       # °F low for the day
    "precipitation_sum",        # total precipitation in mm
    "weathercode",              # WMO weather interpretation code
]

# Temperature unit returned by the API
WEATHER_TEMPERATURE_UNIT = "fahrenheit"

# ---------------------------------------------------------------------------
# Spark output directory  (Stage 5 — PySpark aggregations)
# ---------------------------------------------------------------------------
SPARK_REPORTS_DIR = REPORTS_DIR / "spark"

# ---------------------------------------------------------------------------
# Violation types to exclude (test/invalid records)
# ---------------------------------------------------------------------------
EXCLUDED_VIOLATION_TYPES = {"DO NOT USE/TESTING"}

# ---------------------------------------------------------------------------
# Official fee schedule — used for fine validation
# Source: KSU Parking Services violation schedule (see docs/data_dictionary.md)
# ---------------------------------------------------------------------------
EXPECTED_FINES = {
    "Area Not Designed For Parking":  50,
    "Boot Fee":                       60,
    "Failed To Pay At Visitor Lot":   35,
    "Fire Lane - Blocking/Impeding":  60,
    "No Valid Permit":                50,
    "Parked In Dedicated Space":      50,
    "Parked In Loading Zone":         35,
    "Parked In Tow Zone":             50,
    "Parked In Wrong Direction":      35,
    "Tow Fee":                        60,
    "Unauthorized Use of Permit":     60,
    "Not Parked Between Lines":       50,
    "Not In Assigned Area":           50,
    "Parking For Clinic Patients Only": 50,
    "Abandoned Vehicle":              50,
    "Backed In/Pulled Through Space": 35,
    "Expired Permit":                 50,
    "Expired Hourly Parking":         35,
    "No License Plate Displayed":     35,
    "Parked In Visitor Space":        50,
}

# Statuses where a $0 fine is expected and valid
ZERO_FINE_STATUSES = {"Transfer", "Zero Balance", "Inactive"}

# ---------------------------------------------------------------------------
# Semester date boundaries (used to label citation records by semester)
# ---------------------------------------------------------------------------
SEMESTERS = [
    ("Fall 2025",   "2025-08-01", "2025-12-31"),
    ("Spring 2026", "2026-01-01", "2026-05-31"),
]

# ---------------------------------------------------------------------------
# WMO weather code → human-readable label mapping
# Used by weather_transform.py to add a condition_label column.
# Source: https://open-meteo.com/en/docs (WMO Weather interpretation codes)
# ---------------------------------------------------------------------------
WMO_CODE_LABELS = {
    0:  "Clear sky",
    1:  "Mainly clear",
    2:  "Partly cloudy",
    3:  "Overcast",
    45: "Fog",
    48: "Icy fog",
    51: "Light drizzle",
    53: "Moderate drizzle",
    55: "Heavy drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    71: "Slight snow",
    73: "Moderate snow",
    75: "Heavy snow",
    80: "Slight showers",
    81: "Moderate showers",
    82: "Violent showers",
    95: "Thunderstorm",
    96: "Thunderstorm w/ hail",
    99: "Thunderstorm w/ heavy hail",
}

# Precipitation buckets used in the weather-citations correlation query.
# Labels must match the CASE expression in analytics.py exactly.
PRECIP_BUCKETS = {
    "None":     (0.0,  0.0),
    "Light":    (0.01, 5.0),
    "Moderate": (5.01, 15.0),
    "Heavy":    (15.01, float("inf")),
}