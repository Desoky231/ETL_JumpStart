
# Bike Store Data Pipeline

This project demonstrates an end-to-end data ingestion and transformation pipeline for a fictional bike store business. The goal is to extract, clean, and transform raw operational data into a dimensional model suitable for analytics.

---

## 📁 Project Structure

```
use case/
├── 1_source/           # Raw source data
├── 2_landing/          # Cleaned & structured data per date
├── 3_profiling/        # Data profiling outputs (HTML reports)
├── 4_cleaned/          # Cleaned tables ready for modeling
├── 5_datamart/         # Final dimensional model (star schema)
├── meta_data.csv       # Metadata rules (types, PKs, nullability, etc.)
├── utils.py            # Helper functions
└── *.ipynb             # Notebooks for each pipeline stage
```

---

## 🧱 Data Model

The dimensional model is based on a **star schema** with:

- 4 dimension tables: `Customer_DIM`, `Product_DIM`, `Store_DIM`, `Staff_DIM`
- 1 calendar table: `Date_DIM`
- 2 fact tables: `Sales_Fact`, `Stock_Fact`

![ERD](BIKE%20STORE%20ERD.png)

---

## 🔄 Pipeline Flow

1. **Ingest**
   - Raw CSVs are copied from the source directory.

2. **Clean**
   - Apply validation based on `meta_data.csv`:
     - Remove nulls in non-nullable columns
     - Enforce data types
     - Drop duplicates based on primary and unique keys

3. **Profile**
   - Generate visual profiling reports using `ydata-profiling`

4. **Build Dimensional Model**
   - Construct star schema using cleaned data
   - Create surrogate keys and map date fields to `Date_DIM`

5. **Export**
   - Final tables saved to `5_datamart/` as flat files (CSV)

---

## ✅ Output Tables (example)

- `Customer_DIM.csv`
- `Product_DIM.csv`
- `Sales_Fact.csv`
- `Date_DIM.csv`
- `Stock_Fact.csv`

---

## 📌 Notes

- All data is processed locally using Python (Pandas)
- Designed for reproducibility and clarity in ETL design
