"""
src/ingestion/weather.py

Fetches historical daily weather data from Open-Meteo for every unique date
present in the citations dataset, for each campus independently.

Open-Meteo archive API:
  - Free, no API key required
  - Returns daily aggregates (max temp, min temp, precipitation, weather code)
  - Historical data available from 1940 onward

This module is called from ingest.py as part of Stage 1, after the citations
Excel file has been read, so we know the exact date range to request.
"""

import logging
import time
import requests
import pandas as pd
from pathlib import Path

from config.settings import (
    OPEN_METEO_URL,
    CAMPUS_COORDINATES,
    WEATHER_VARIABLES,
    WEATHER_TEMPERATURE_UNIT,
    WEATHER_RAW_PARQUET,
)

logger = logging.getLogger(__name__)

# Retry settings for the HTTP requests
MAX_RETRIES = 3
RETRY_DELAY = 2   # seconds between retries


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _fetch_campus_weather(campus: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Call the Open-Meteo archive API for one campus over the given date range.

    Parameters
    ----------
    campus     : "Kennesaw" or "Marietta"
    start_date : ISO date string  "YYYY-MM-DD"
    end_date   : ISO date string  "YYYY-MM-DD"

    Returns a DataFrame with columns:
        date, campus, temperature_2m_max, temperature_2m_min,
        precipitation_sum, weathercode
    """
    coords = CAMPUS_COORDINATES[campus]

    params = {
        "latitude":         coords["latitude"],
        "longitude":        coords["longitude"],
        "start_date":       start_date,
        "end_date":         end_date,
        "daily":            ",".join(WEATHER_VARIABLES),
        "temperature_unit": WEATHER_TEMPERATURE_UNIT,
        "timezone":         "America/New_York",   # KSU is Eastern Time
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(
                f"  Fetching weather for {campus} "
                f"({start_date} → {end_date}), attempt {attempt}"
            )
            response = requests.get(OPEN_METEO_URL, params=params, timeout=30)
            response.raise_for_status()
            break   # success — exit retry loop
        except requests.exceptions.RequestException as e:
            logger.warning(f"  Open-Meteo request failed (attempt {attempt}): {e}")
            if attempt == MAX_RETRIES:
                raise RuntimeError(
                    f"Open-Meteo API unavailable after {MAX_RETRIES} attempts. "
                    "Check your internet connection or try again later."
                ) from e
            time.sleep(RETRY_DELAY)

    payload = response.json()

    # The API returns a 'daily' dict where each key is a list of values
    # indexed by the 'time' list.
    daily = payload.get("daily", {})
    if not daily or "time" not in daily:
        raise ValueError(
            f"Unexpected Open-Meteo response for {campus}: 'daily' key missing. "
            f"Response: {payload}"
        )

    df = pd.DataFrame({
        "date":               daily["time"],
        "campus":             campus,
        "temperature_2m_max": daily.get("temperature_2m_max"),
        "temperature_2m_min": daily.get("temperature_2m_min"),
        "precipitation_sum":  daily.get("precipitation_sum"),
        "weathercode":        daily.get("weathercode"),
    })

    logger.info(f"  Retrieved {len(df)} daily weather records for {campus}")
    return df


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def fetch_weather(df_citations: pd.DataFrame) -> pd.DataFrame:
    """
    Derive the date range from the citations DataFrame and fetch weather
    for all campus/date combinations present in the data.

    Parameters
    ----------
    df_citations : raw citations DataFrame (must have 'Timestamp' and 'Campus'
                   columns — the raw shape from ingest.py before transformation)

    Returns a raw weather DataFrame (one row per campus per date).
    """
    # Derive date range from actual citation timestamps
    min_date = df_citations["Timestamp"].min().strftime("%Y-%m-%d")
    max_date = df_citations["Timestamp"].max().strftime("%Y-%m-%d")

    logger.info(f"Citation date range: {min_date} → {max_date}")
    logger.info(f"Fetching weather for campuses: {list(CAMPUS_COORDINATES.keys())}")

    frames = []
    for campus in CAMPUS_COORDINATES:
        df_campus = _fetch_campus_weather(campus, min_date, max_date)
        frames.append(df_campus)

    df_weather = pd.concat(frames, ignore_index=True)
    logger.info(
        f"Total raw weather records fetched: {len(df_weather):,} "
        f"({len(frames)} campuses × date range)"
    )
    return df_weather


def save_weather_raw_parquet(df_weather: pd.DataFrame, output_path: Path) -> None:
    """
    Persist the raw weather DataFrame to Parquet.

    Mirrors the pattern in ingest.py: raw zone file preserved before any
    transformation so the API does not need to be called again if
    transformation logic changes.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_weather.to_parquet(output_path, index=False, engine="pyarrow", compression="snappy")
    logger.info(
        f"Raw weather Parquet written → {output_path}  "
        f"({output_path.stat().st_size / 1024:.1f} KB)"
    )


def run_weather_ingestion(df_citations: pd.DataFrame) -> pd.DataFrame:
    """
    Full weather ingestion stage: fetch from API, save raw Parquet, return DataFrame.
    Called by ingest.py inside run_ingestion().
    """
    df_weather = fetch_weather(df_citations)
    save_weather_raw_parquet(df_weather, WEATHER_RAW_PARQUET)
    return df_weather