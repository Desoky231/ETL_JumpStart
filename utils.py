from typing import Sequence
import os, re, logging
import pandas as pd
from pathlib import Path
from datetime import datetime
import json
import csv
import numpy as np



# ------------------------------------------------------------------
#  helper registry for custom UDF checks 
# ------------------------------------------------------------------
def validate_isbn10_checksum(val: str) -> bool:
    """Return True if val is a valid 10-digit ISBN checksum."""
    if not re.fullmatch(r"\d{10}", str(val)):
        return False
    total = sum((10 - i) * int(d) for i, d in enumerate(val[:9]))
    check = (11 - (total % 11)) % 11
    return str(check if check < 10 else "X") == val[-1]

def validate_isbn13_checksum(val: str) -> bool:
    if not re.fullmatch(r"\d{13}", str(val)):
        return False
    total = sum((1 if i % 2 == 0 else 3) * int(d) for i, d in enumerate(val[:12]))
    check = (10 - (total % 10)) % 10
    return check == int(val[-1])

UDF_REGISTRY = {
    "validate_isbn10_checksum": validate_isbn10_checksum,
    "validate_isbn13_checksum": validate_isbn13_checksum,
}

# ------------------------------------------------------------------
#  DataCleaning class  (adds flexible-rule engine)
# ------------------------------------------------------------------
class DataCleaning:
    """
    Generic Silver-layer cleaner driven by:
      • silver_md.csv  (schema / PK / NON_NULL / UNIQUE)
      • <table>_rules.json (ranges, regex, mappings, UDFs)
    """

    def __init__(self, df_path: str, meta_data_path: str, rules_path: str | None = None):
        # ── raw load ─────────────────────────────────────────────
        self.df = pd.read_csv(
            df_path,
            sep=",",
            engine="python",
            quotechar='"',
            on_bad_lines="skip"
        )
        self.df.columns = self.df.columns.str.strip()

        # infer logical table name
        self.table_name = Path(df_path).name.split('_')[-1]

        # ── structural metadata (CSV) ────────────────────────────
        meta = pd.read_csv(meta_data_path)
        self.meta_data_table = meta[meta["table_name"] == self.table_name].copy()
        if self.meta_data_table.empty:
            raise ValueError(f"No metadata found for table '{self.table_name}'")

        # ── flexible JSON rules (optional) ───────────────────────
        if rules_path and Path(rules_path).exists():
            with open(rules_path) as f:
                self.flex_rules = json.load(f)
        else:
            self.flex_rules = {}

    # ─────────────────────────────────────────────────────────────
    #  Primary-key validation
    # ─────────────────────────────────────────────────────────────
    def validate_primary_keys(self):
        pk_cols = self.meta_data_table[self.meta_data_table["PK"] == 1]["column_name"]
        pk_cols = pk_cols.tolist()

        if pk_cols:
            self.df = self.df[self.df[pk_cols].notna().all(axis=1)]
            self.df = self.df.drop_duplicates(subset=pk_cols, keep="first")
        else:
            logging.warning("No PK defined for %s", self.table_name)

        return self

    # ─────────────────────────────────────────────────────────────
    #  Non-nullable columns
    # ─────────────────────────────────────────────────────────────
    def validate_non_nulls(self):
        nn_cols = self.meta_data_table[self.meta_data_table["NON_NULLABLE"] == 1]["column_name"]
        nn_cols = nn_cols.tolist()

        if nn_cols:
            self.df = self.df[self.df[nn_cols].notna().all(axis=1)]

        return self

    # ─────────────────────────────────────────────────────────────
    #  Unique column enforcement
    # ─────────────────────────────────────────────────────────────
    def validate_unique(self):
        uniq_cols = self.meta_data_table[self.meta_data_table["UNIQUE"] == 1]["column_name"]
        uniq_cols = uniq_cols.tolist()

        for col in uniq_cols:
            
            self.df = self.df.drop_duplicates(subset=[col], keep="first")

        return self

    # ─────────────────────────────────────────────────────────────
    #  Data-type coercion  (int, float, date, string) + pruning
    # ─────────────────────────────────────────────────────────────
    def validate_datatype(self):
        df = self.df.copy()

        for _, row in self.meta_data_table.iterrows():
            col, dtype = row["column_name"], str(row["dtype"]).lower()

            if col not in df.columns:
                logging.warning("Column '%s' missing in data – skipped", col)
                continue

            # ---------- Integer ----------
            if dtype in {"int", "integer"}:
                coerced = pd.to_numeric(df[col], errors="coerce", downcast="integer")
                df = df[~(coerced.isna() & df[col].notna())]
                df[col] = coerced.astype("Int64")

            # ---------- Float ----------
            elif dtype in {"float", "double"}:
                coerced = pd.to_numeric(df[col], errors="coerce")
                df = df[~(coerced.isna() & df[col].notna())]
                df[col] = coerced.astype(float)

            # ---------- Date / Datetime ----------
            elif dtype in {"date", "datetime"}:
                coerced = pd.to_datetime(df[col], errors="coerce")
                df = df[~(coerced.isna() & df[col].notna())]
                df[col] = coerced

            # ---------- String / fallback ----------
            else:
                df[col] = df[col].astype(str)

        self.df = df.reset_index(drop=True)
        return self
    # -----------------------------------------------------------
    #  ranges & date_not_future
    # -----------------------------------------------------------
    def apply_ranges(self):
        for rule in self.flex_rules.get("ranges", []):
            col = rule["col"]
            if col not in self.df.columns:
                continue

            if rule.get("type") == "date_not_future":
                today = pd.Timestamp.today().normalize()
                self.df = self.df[self.df[col] <= today]
            else:
                lo, hi = rule.get("min"), rule.get("max")
                if lo is not None:
                    self.df = self.df[self.df[col] >= lo]
                if hi is not None:
                    self.df = self.df[self.df[col] <= hi]
        return self

    # -----------------------------------------------------------
    #  regex patterns
    # -----------------------------------------------------------
    def apply_regex(self):
        for rule in self.flex_rules.get("regex", []):
            col, pattern = rule["col"], rule["pattern"]
            if col in self.df.columns:
                self.df = self.df[self.df[col].astype(str).str.match(pattern, na=False)]
        return self

    # -----------------------------------------------------------
    #  defaults (fill-na before other checks)
    # -----------------------------------------------------------
    def apply_defaults(self):
        for col, val in self.flex_rules.get("defaults", {}).items():
            if col in self.df.columns:
                self.df[col] = self.df[col].fillna(val)
        return self

    # -----------------------------------------------------------
    #  mappings / alias fixes
    # -----------------------------------------------------------
    def apply_mappings(self):
        for mapping_name, mapping_dict in self.flex_rules.get("mappings", {}).items():
            col = mapping_name.replace("_fix", "")  # e.g., publisher_fix → publisher
            if col in self.df.columns:
                self.df[col] = self.df[col].replace(mapping_dict)
        return self

    # -----------------------------------------------------------
    #  custom UDF checks
    # -----------------------------------------------------------
    def apply_custom_udfs(self):
        for rule in self.flex_rules.get("custom_checks", []):
            col, udf_name = rule["col"], rule["udf"]
            if col not in self.df.columns:
                continue
            func = UDF_REGISTRY.get(udf_name)
            if func:
                self.df = self.df[self.df[col].apply(func)]
            else:
                logging.warning("UDF %s not found in registry", udf_name)
        return self

    # -----------------------------------------------------------
    #  Save cleaned dataframe (hierarchy ready)
    # -----------------------------------------------------------
    def save(self, out_dir: str | Path):
        """
        Write cleaned DataFrame as CSV in the given directory.
        """
        out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        # build output path with .csv extension
        out_file = out_dir / f"{self.table_name}"

        # write CSV
        self.df.to_csv(out_file, index=False)

        print(f"✔️  Clean data written to {out_file}")
        return out_file



    
# ──────────────────────────────────────────────────────────────
# generic manifest-append helper (dynamic header, creates folder)
# ──────────────────────────────────────────────────────────────
def append_manifest_row(record: dict, manifest_path: str | Path, header: Sequence[str]):
    manifest_path = Path(manifest_path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)      # make folder if missing
    file_exists = manifest_path.is_file()
    with manifest_path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(header))
        if not file_exists:
            writer.writeheader()
        writer.writerow(record)


class BaseGoldTransformer:
    def __init__(self, df_path: str, rules: dict | None = None):
        self.df = pd.read_csv(
            df_path,
            sep=",",
            engine="python",
            quotechar='"',
            on_bad_lines="skip"
        )
        self.rules = rules or {}

    # ----- generic helpers -----
    def map_column(self, col, mapping):
        if col in self.df:
            self.df[col] = self.df[col].replace(mapping)
        return self

    def drop_invalid(self, mask, reason):
        dropped = self.df[~mask]
        if not dropped.empty:
            logging.info("Dropped %d rows %s", len(dropped), reason)
        self.df = self.df[mask]
        return self

    def save(self, out_dir: str | Path):
        """
        Write cleaned DataFrame as CSV in the given directory.
        """
        out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        # build output path with .csv extension
        out_file = out_dir / f"{self.table_name}"

        # write CSV
        print(out_file)
        self.df.to_csv(out_file, index=False)

        print(f"✔️  Transformed data written to {out_file}")
        return out_file


class BooksGoldTransformer(BaseGoldTransformer):
    
    def clean_publisher_name(name: str) -> str:
        # List of common formal words to remove
        formal_words = ["inc.", "inc", "ltd", "llc", "press", "publishing", "books", "company", "corp.", "corporation", "limited", "verlag"]

        # Make name lowercase to avoid case mismatches
        name = name.lower()

        # Remove any formal words at the end of the name (case insensitive)
        for word in formal_words:
            name = re.sub(r"\s*" + re.escape(word) + r"\s*$", "", name)

        # Capitalize the name and return it
        return name.strip().title()  # title() capitalizes each word in the name
    
    
    def __init__(self, silver_df: pd.DataFrame, table_name: str, rules: dict | None = None, language_json: str | None = None):
        
        super().__init__(silver_df, rules)
        
        # Save the logical table name (for naming outputs, etc.)
        self.table_name = table_name

        # Load language map at init so we can use it later
        if language_json and Path(language_json).exists():
            with open(language_json) as f:
                self.language_map = json.load(f)
        else:
            self.language_map = {}
    def transform(self):
        # -------------------------------------------------------
        # title / series
        # -------------------------------------------------------
        self.df["series"] = self.df["title"].str.extract(r"\((.*?)\)")
        self.df["title"]  = self.df["title"].str.replace(r"\s*\(.*?\)$", "", regex=True)

        # -------------------------------------------------------
        # authors
        # -------------------------------------------------------
        self.df["primary_author"] = self.df["authors"].str.split("/").str[0]
        self.df["all_authors"]    = self.df["authors"].str.split("/")

        # -------------------------------------------------------
        # ISBN checksum validation
        #   • cast to string
        #   • zero-pad to correct length
        # -------------------------------------------------------
        self.df["isbn"]   = self.df["isbn"].astype(str).str.zfill(10)
        self.df["isbn13"] = self.df["isbn13"].astype(str).str.zfill(13)

        mask10 = self.df["isbn"].apply(validate_isbn10_checksum)
        mask13 = self.df["isbn13"].apply(validate_isbn13_checksum)

        # keep rows that pass **both** checks; drop the rest
        self.drop_invalid(mask10 & mask13, "invalid ISBN")

        # -------------------------------------------------------
        # publisher standardize
        # -------------------------------------------------------
        self.df["publisher"] = self.df["publisher"].apply(BooksGoldTransformer.clean_publisher_name)

        # -------------------------------------------------------
        # book age
        # -------------------------------------------------------
        today_year = datetime.utcnow().year
        pub_year   = pd.to_datetime(self.df["publication_date"], errors="coerce").dt.year
        self.df["book_age"] = today_year - pub_year

        # -------------------------------------------------------
        # rating popularity score
        # -------------------------------------------------------
        self.df["rating_popularity_score"] = (
            self.df["average_rating"] * np.log1p(self.df["ratings_count"])
        )

        # -------------------------------------------------------
        # language full name
        # -------------------------------------------------------
        self.map_column("language_code", self.language_map)
        self.df = self.df.rename(columns={"language_code": "language"})

        return self