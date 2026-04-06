<!-- Improved compatibility of back to top link: See: https://github.com/othneildrew/Best-README-Template/pull/73 -->
<a id="readme-top"></a>

<!-- PROJECT SHIELDS -->
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]
[![LinkedIn][linkedin-shield]][linkedin-url]



<!-- PROJECT LOGO -->
<br />
<div align="center">
  <h3 align="center">KSU Parking Citations — Big Data Pipeline</h3>

  <p align="center">
    CS 4265 Big Data Analytics | Milestone 3
    <br />
    <a href="docs/data_dictionary.md"><strong>Explore the docs »</strong></a>
    <br />
    <br />
    <a href="https://github.com/your_username/repo_name/issues/new?labels=bug&template=bug-report---.md">Report Bug</a>
    ·
    <a href="https://github.com/your_username/repo_name/issues/new?labels=enhancement&template=feature-request---.md">Request Feature</a>
  </p>
</div>



<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project

A batch data pipeline that ingests university parking citation records, cleans and validates them against the official KSU fee schedule, persists the results in a structured SQLite database backed by Parquet intermediate files, and produces analytical reports via SQL.

**Dataset:**
- **Source:** KSU Parking Services citation records (Fall 2025 – Spring 2026)
- **Volume:** ~35,948 records (growing — new semesters appended to source file)
- **Campuses:** Kennesaw, Marietta
- **Columns:** Citation ID, Timestamp, Location, Campus, Violation Type, Status, Fine Amount

**Project Structure:**
```
parking-citations-pipeline/
├── config/
│   └── settings.py          # All paths, constants, fee schedule
├── src/
│   ├── ingestion/
│   │   └── ingest.py        # Stage 1: Excel → Parquet
│   ├── processing/
│   │   └── transform.py     # Stage 2: clean, validate, enrich
│   ├── storage/
│   │   └── db_handler.py    # Stage 3: DataFrame → SQLite
│   ├── queries/
│   │   └── analytics.py     # Stage 4: SQL → CSV reports
│   └── main.py              # Pipeline orchestrator
├── data/
│   ├── source/              # Place input Excel file here
│   ├── raw/                 # Auto-generated
│   ├── processed/           # Auto-generated
│   └── reports/             # Auto-generated
├── docs/
│   └── data_dictionary.md
├── requirements.txt
├── .gitignore
└── README.md
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>



### Built With

| Stack Layer      | Technology            | Role |
|------------------|-----------------------|------|
| Syntax/Encoding  | Parquet (pyarrow)     | Columnar intermediate storage |
| Data Model       | Pandas DataFrame      | Schema enforcement, transformations |
| Data Store       | SQLite                | Persistent, queryable storage |
| Processing       | Python batch pipeline | Clean, validate, enrich |
| Querying         | SQL                   | Analytical reports |

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- GETTING STARTED -->
## Getting Started

### Prerequisites

- Python 3.9+
- pip

### Installation

1. Clone the repository
   ```sh
   git clone https://github.com/your_username/repo_name.git
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
4. Place the source data — copy the citations Excel file to:
   ```
   data/source/citations.xlsx
   ```
   The filename must match exactly. Update `config/settings.py → SOURCE_EXCEL` if it differs.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- USAGE EXAMPLES -->
## Usage

Run the full pipeline:
```sh
python src/main.py
```

The pipeline runs all four stages automatically:
1. **Ingest** — reads Excel, writes `data/raw/citations_raw.parquet`
2. **Transform** — cleans data, writes `data/processed/citations_clean.parquet`
3. **Store** — loads into `data/citations.db` (SQLite)
4. **Query** — runs 5 analytical queries, exports CSVs to `data/reports/`

**The pipeline is idempotent** — running it twice on the same data produces the same result. Duplicate citation IDs are silently skipped.

**Output files:**

| File | Description |
|------|-------------|
| `data/raw/citations_raw.parquet` | Raw columnar snapshot of source data |
| `data/processed/citations_clean.parquet` | Cleaned, enriched, schema-enforced records |
| `data/citations.db` | SQLite database with indexed citations table |
| `data/reports/violations_by_type.csv` | Citation count and revenue by violation |
| `data/reports/monthly_volume_by_campus.csv` | Monthly trends per campus |
| `data/reports/top_locations.csv` | Top 20 locations by citation volume |
| `data/reports/status_breakdown.csv` | Outcome distribution by semester |
| `data/reports/hourly_patterns.csv` | Citations by hour of day |

**Adding new semester data:**
1. Append new rows to `data/source/citations.xlsx` (keep same column structure)
2. Add the new semester date range to `config/settings.py → SEMESTERS`
3. Re-run `python src/main.py`

New records are inserted; existing records are unchanged.

_For field definitions, transformation decisions, and data quality notes, see [`docs/data_dictionary.md`](docs/data_dictionary.md)_

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- ROADMAP -->
## Roadmap

- [x] Stage 1: Ingestion (Excel → Parquet)
- [x] Stage 2: Transformation (clean, validate, enrich)
- [x] Stage 3: Storage (Parquet → SQLite)
- [x] Stage 4: Analytical queries (SQL → CSV reports)
- [ ] Automated semester detection from source data

See the [open issues](https://github.com/your_username/repo_name/issues) for a full list of proposed features (and known issues).

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- CONTACT -->
## Contact

Your Name - [@your_twitter](https://twitter.com/your_username) - email@example.com

Project Link: [https://github.com/your_username/repo_name](https://github.com/your_username/repo_name)

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- ACKNOWLEDGMENTS -->
## Acknowledgments

* [KSU Parking Services](https://parking.kennesaw.edu) — source citation data
* [pandas documentation](https://pandas.pydata.org/docs/)
* [Apache Arrow / pyarrow](https://arrow.apache.org/docs/python/)
* [SQLite documentation](https://www.sqlite.org/docs.html)
* [Img Shields](https://shields.io)
* [Choose an Open Source License](https://choosealicense.com)

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- MARKDOWN LINKS & IMAGES -->
[contributors-shield]: https://img.shields.io/github/contributors/your_username/repo_name.svg?style=for-the-badge
[contributors-url]: https://github.com/your_username/repo_name/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/your_username/repo_name.svg?style=for-the-badge
[forks-url]: https://github.com/your_username/repo_name/network/members
[stars-shield]: https://img.shields.io/github/stars/your_username/repo_name.svg?style=for-the-badge
[stars-url]: https://github.com/your_username/repo_name/stargazers
[issues-shield]: https://img.shields.io/github/issues/your_username/repo_name.svg?style=for-the-badge
[issues-url]: https://github.com/your_username/repo_name/issues
[license-shield]: https://img.shields.io/github/license/your_username/repo_name.svg?style=for-the-badge
[license-url]: https://github.com/your_username/repo_name/blob/master/LICENSE.txt
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]: https://linkedin.in/your_username
