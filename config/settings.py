from pathlib import Path

# Root of the project (two levels up from this file)
ROOT_DIR = Path(__file__).resolve().parent.parent

# Data directories
RAW_DIR       = ROOT_DIR / "data" / "raw"
PROCESSED_DIR = ROOT_DIR / "data" / "processed"
REPORTS_DIR   = ROOT_DIR / "data" / "reports"
DB_PATH       = ROOT_DIR / "data" / "citations.db"

# Source file (update this path when new semester data arrives)
SOURCE_EXCEL  = ROOT_DIR / "data" / "source" / "citations.xlsx"

# Raw Parquet output filename
RAW_PARQUET   = RAW_DIR / "citations_raw.parquet"

# Processed Parquet output filename
PROCESSED_PARQUET = PROCESSED_DIR / "citations_clean.parquet"

# Violation types to exclude (test/invalid records)
EXCLUDED_VIOLATION_TYPES = {"DO NOT USE/TESTING"}

# Official fee schedule — used for validation
# Source: KSU Parking Services violation schedule (see docs/data_dictionary.md)
EXPECTED_FINES = {
    "Area Not Designed For Parking":                    50,
    "Boot Fee":                                         60,
    "Failed To Pay At Visitor Lot":                     35,
    "Fire Lane - Blocking/Impeding":                    60,
    "No Valid Permit":                                  50,
    "Parked In Dedicated Space":                        50,
    "Parked In Loading Zone":                           35,
    "Parked In Tow Zone":                               50,
    "Parked In Wrong Direction":                        35,
    "Tow Fee":                                          60,
    "Unauthorized Use of Permit":                       60,
    "Not Parked Between Lines":                         50,
    "Not In Assigned Area":                             50,
    "Parking For Clinic Patients Only":                 50,
    "Abandoned Vehicle":                                50,
    "Backed In/Pulled Through Space":                   35,
    "Expired Permit":                                   50,
    "Expired Hourly Parking":                           35,
    "No License Plate Displayed":                       35,
    "Parked In Visitor Space":                          50,
}

# Statuses where a $0 fine is expected and valid
ZERO_FINE_STATUSES = {"Transfer", "Zero Balance", "Inactive"}

# Semester date boundaries (used to label records by semester)
SEMESTERS = [
    ("Fall 2025",   "2025-08-01", "2025-12-31"),
    ("Spring 2026", "2026-01-01", "2026-05-31"),
]