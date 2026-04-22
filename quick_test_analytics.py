# quick_test_analytics.py
import sys, logging
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

from config.settings import SOURCE_EXCEL, RAW_PARQUET, PROCESSED_PARQUET, DB_PATH, REPORTS_DIR
from src.ingestion.ingest import run_ingestion
from src.processing.transform import run_transformation
from src.storage.db_handler import run_storage
from src.queries.analytics import run_queries

df_citations, df_weather_raw = run_ingestion(SOURCE_EXCEL, RAW_PARQUET)
df_clean, df_weather_clean   = run_transformation(df_citations, df_weather_raw, PROCESSED_PARQUET)
run_storage(df_clean, df_weather_clean, DB_PATH)
run_queries(DB_PATH, REPORTS_DIR)

# Preview the new weather report
import pandas as pd
df_weather_report = pd.read_csv(REPORTS_DIR / "weather_citation_correlation.csv")
print(f"\n--- Weather-Citation Correlation Report ---")
print(df_weather_report.to_string(index=False))