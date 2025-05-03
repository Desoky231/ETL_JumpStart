import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime
import json



# Define paths
GOLD_DIR = Path("4_gold")
DB_PATH = Path("book_warehouse.db")

# Create or connect to database
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Create the target table
cursor.execute("""
CREATE TABLE IF NOT EXISTS book_facts (
    surrogate_key TEXT PRIMARY KEY,
    bookID TEXT,
    src TEXT,
    time_stamp TEXT,
    title TEXT,
    series TEXT,
    primary_author TEXT,
    all_authors TEXT,  -- store as JSON array
    average_rating FLOAT,
    ratings_count INTEGER,
    text_reviews_count INTEGER,
    isbn TEXT,
    isbn13 TEXT,
    num_pages INTEGER,
    publication_date DATE,
    publisher TEXT,
    language TEXT,
    book_age INTEGER,
    rating_popularity_score FLOAT
)
""")
conn.commit()

print("✅ Database and table ready.")

# Scan all gold files
gold_files = list(GOLD_DIR.glob("**/books.csv"))

if not gold_files:
    print("⚠️ No gold CSVs found.")
else:
    print(f"➡️ Found {len(gold_files)} gold CSVs to ingest.")

# Loop through each file
for path in gold_files:
    try:
        # Capture hierarchy: source / year / month / day
        parts = path.parts
        # Expect something like: 4_gold/source/year=YYYY/month=MM/day=DD/books_gold.csv
        src_name = parts[1]  # after 4_gold/
        year = parts[2].split("=")[1]
        month = parts[3].split("=")[1]
        day = parts[4].split("=")[1]
        ingestion_timestamp = f"{year}-{month}-{day}"

        print(f"📖 Processing: {path} | Source: {src_name} | Timestamp: {ingestion_timestamp}")

        # Read CSV
        df = pd.read_csv(path)

        # Add columns for src and ingestion timestamp
        df["src"] = src_name
        df["time_stamp"] = ingestion_timestamp

        # Build surrogate key
        df["surrogate_key"] = df.apply(
            lambda row: f"{row['src']}_{row['bookID']}_{row['time_stamp']}", axis=1
        )

        # Convert 'all_authors' list into JSON text
        if "all_authors" in df.columns:
            df["all_authors"] = df["all_authors"].apply(
                lambda x: json.dumps(x) if not pd.isna(x) else None
            )

        # Subset to only needed columns (safe for missing columns)
        expected_cols = [
            "surrogate_key", "bookID", "src", "time_stamp",
            "title", "series", "primary_author", "all_authors",
            "average_rating", "ratings_count", "text_reviews_count",
            "isbn", "isbn13", "num_pages", "publication_date",
            "publisher", "language", "book_age", "rating_popularity_score"
        ]
        df = df[[col for col in expected_cols if col in df.columns]]

        # Insert into SQLite
        df.to_sql("book_facts", conn, if_exists="append", index=False)
        print(f"✅ Inserted {len(df)} records from {path}")

    except Exception as e:
        print(f"❌ Failed on {path}: {e}")

# Finalize
conn.commit()
conn.close()
print("🏁 Ingestion complete. Database closed.")
