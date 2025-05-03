# Books ETL Jump‑Start 📚🚀

A quick‑start, end‑to‑end data pipeline that ingests raw book metadata, cleans and enriches it through a Medallion-style workflow (Bronze → Silver → Gold), and finally loads the curated data to a small SQLite “warehouse”.  
Everything is pure **Python** and can run:

- directly on your host with a single `python ...` command per stage, or  
- inside Apache Airflow (Docker Compose example provided).

---

## Repository layout
```bash

├── 1_source/unknown/books.csv # raw dump you start from
├── bronze.py # Bronze: minimal ingestion + checksum
├── silver.py # Silver: data‑quality & standardisation
├── gold.py # Gold: feature engineering
├── load.py # Loads curated data into SQLite
├── dags/pipeline.py # One Airflow DAG that chains the 4 scripts
├── {2_bronze,3_silver,4_gold}/ # generated layer folders
├── manifests/ # de‑duplication manifests
├── meta_data/ # metadata snapshots per layer
└── docker-compose.yaml # optional Airflow stack

```


## Execution order

```lua
bronze.py  →  silver.py  →  gold.py  →  load.py
```

## Manifests vs Metadata – What Are They?

These files are lightweight CSVs that support transparency and reproducibility in your ETL workflow:

| File                      | Purpose                                                                 |
|---------------------------|-------------------------------------------------------------------------|
| `manifests/*_manifest.csv` | Tracks file hashes & row counts to avoid duplicate processing.          |
| `meta_data/*_md.csv`       | Captures schema summaries (e.g., column stats, null %, timestamps).     |

---

## Cleaning & Feature Engineering Highlights

| Layer  | Key Actions                                                                   |
|--------|--------------------------------------------------------------------------------|
| Bronze | Initial ingestion, compute checksums, and store raw records.                  |
| Silver | Data validation, de-duplication, standardization (e.g., authors, dates).      |
| Gold   | Feature extraction: `book_age`, `popularity_score`, structured author lists.  |

---

## Contributing & Next Steps

- ✅ Add tests to validate transformation logic.
- 🛠️ Use a PostgreSQL + Redis setup for production-scale scheduling.
- 🔁 Automate with CI tools like GitHub Actions or pre-commit hooks.

