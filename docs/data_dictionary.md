# Data Dictionary — KSU Parking Citations Pipeline

## Source: `data/source/citations.xlsx`

| Original Column | Type | Notes |
|-----------------|------|-------|
| Citation ID | String | Unique identifier per citation (trailing space stripped) |
| Timestamp | Datetime | Date and time citation was issued |
| Location | String | Parking lot or structure name |
| Campus | String | Kennesaw or Marietta |
| Violation Type | String | Category of parking violation |
| Status | String | Current payment/disposition status |
| Fine Amount | Integer | Dollar amount (may be 0 or negative — see below) |

---

## Final Schema: `citations` table / `citations_clean.parquet`

| Column | Type | Description |
|--------|------|-------------|
| citation_id | TEXT (PK) | Unique citation identifier |
| timestamp | TEXT | ISO datetime string |
| date | TEXT | Date only (YYYY-MM-DD) |
| year | INTEGER | Calendar year |
| month | INTEGER | Month (1–12) |
| day_of_week | TEXT | Day name (e.g., Monday) |
| hour | INTEGER | Hour of day (0–23) |
| campus | TEXT | Kennesaw or Marietta |
| location | TEXT | Parking area name |
| violation_type | TEXT | Standardised violation category |
| status | TEXT | Payment/disposition status |
| fine_amount | INTEGER | Fine in dollars (may be 0 or negative) |
| is_credit_adjustment | INTEGER | 1 if fine_amount < 0 (credit/reversal record) |
| fine_validated | INTEGER | 1 if fine matches official fee schedule (or is a zero-status record) |
| semester | TEXT | Academic semester label |

---

## Data Quality Notes

### Negative fine amounts
Six records have negative fine amounts (-$50 or -$60). These appear exclusively on Zero Balance records and represent credit adjustments or overpayment reversals. They are preserved with `is_credit_adjustment = 1` and excluded from revenue aggregations.

### Zero fine amounts
24,509 records have a fine amount of $0. This is expected for three status types:
- **Transfer** — citation transferred to another party
- **Zero Balance** — balance cleared (paid, appealed, or waived)
- **Inactive** — citation no longer active

### Test records
One record with Violation Type = "DO NOT USE/TESTING" was removed. It is a system artifact.

### Fine validation
Each citation's fine was compared against the official KSU Parking Services fee schedule. Records where the fine does not match the expected amount AND the status is not a zero-fine status are flagged with `fine_validated = 0`.

---

## Reports: `data/reports/`

All reports exclude credit adjustment records (`is_credit_adjustment = 0`) unless noted. Revenue figures only count positive fine amounts.

---

### `violations_by_type.csv`
One row per violation type across the full dataset.

| Column | Description |
|--------|-------------|
| violation_type | The violation category (e.g., No Valid Permit) |
| citation_count | Total number of citations issued for this violation type |
| total_fines_collected | Sum of all positive fine amounts for this violation type (dollars) |
| avg_fine | Average fine amount among citations that had a positive fine (dollars) |

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
| pct_of_semester | Percentage of all citations in that semester with this status (e.g., 32.5 means 32.5%) |

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
Citation volume broken down by hour of day and campus, useful for identifying peak enforcement periods.

| Column | Description |
|--------|-------------|
| hour | Hour of day the citation was issued (0–23, 24-hour clock) |
| campus | Kennesaw or Marietta |
| citation_count | Number of citations issued during this hour on this campus |

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