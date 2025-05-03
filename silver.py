import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from utils import append_manifest_row, DataCleaning





# ───────────────────────────────────────────────────────────────
# Main Bronze → Silver function that ALSO logs to gold_manifest
# ───────────────────────────────────────────────────────────────
def ingest_to_silver(
    manifest_path: str,
    md_rules_path: str,
    bronze_root: str | Path,
    silver_output: str | Path = "manifests/silver_manifest.csv",
    flex_rules_path: str | None = None,
    gold_manifest_path: str | Path = "manifests/gold_manifest.csv",
):
    BRONZE_ROOT = Path(bronze_root)
    SILVER_OUTPUT = Path(silver_output)

    # Read silver manifest
    manifest = pd.read_csv(manifest_path)
    for col in ["silver_processed_at", "rows_out", "status", "error_msg"]:
        manifest[col] = manifest[col].astype("string")

    # Filter for READY rows (to be processed)
    ready = manifest[manifest["status"] == "READY"].copy()
    if ready.empty:
        print("✔️ Nothing to process; no rows in READY state.")
        return
    print(f"➡️ Found {len(ready)} Bronze file(s) to clean.")

    # Gold manifest header (same structure as silver_manifest)
    GOLD_HEADER = [
        "table_name", "source_name", "silver_partition", "silver_file",
        "ingestion_timestamp", "gold_processed_at", "rows_in", "rows_out", "status", "error_msg"
    ]

    for idx, row in ready.iterrows():
        try:
            bronze_path = BRONZE_ROOT / row["bronze_partition"] / row["bronze_file"]
            if not bronze_path.exists():
                raise FileNotFoundError(f"{bronze_path} missing")

            # Run the Silver cleaning process
            cleaner = (
                DataCleaning(str(bronze_path), md_rules_path, flex_rules_path)
                .validate_primary_keys()
                .validate_non_nulls()
                .validate_unique()
                .validate_datatype()
                .apply_defaults()
                .apply_mappings()
                .apply_ranges()
                .apply_regex()
                .apply_custom_udfs()
            )

            # Silver output directory
            silver_dir = SILVER_OUTPUT / row["bronze_partition"]
            out_path = cleaner.save(out_dir=silver_dir)

            # Update Silver manifest
            manifest.loc[idx, "silver_processed_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            manifest.loc[idx, "rows_out"] = str(len(cleaner.df))
            manifest.loc[idx, "status"] = "SUCCESS"
            manifest.loc[idx, "error_msg"] = ""

            print(f"✅ Cleaned → {out_path}")

            # Append to the Gold manifest (log the transformation details)
            gold_record = {
                "table_name": row["table_name"],
                "source_name": row["source_name"],
                "silver_partition": row["bronze_partition"],
                "silver_file": row["bronze_file"],
                "ingestion_timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "gold_processed_at": "",  # Filled when Gold job runs
                "rows_in": str(len(cleaner.df)),
                "rows_out": "",  # Will be filled by Gold job later
                "status": "READY",  # Ready for the Gold job
                "error_msg": ""
            }
            append_manifest_row(gold_record, gold_manifest_path, GOLD_HEADER)

        except Exception as e:
            # Mark Silver manifest row as failed if an error occurs
            manifest.loc[idx, "silver_processed_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            manifest.loc[idx, "status"] = "FAILED"
            manifest.loc[idx, "error_msg"] = str(e)
            logging.error("❌ Failed on %s : %s", row["bronze_file"], e)

    # Persist updates to the Silver manifest
    manifest.to_csv(manifest_path, index=False)
    print("📝 Silver manifest updated.")
    print("📜 Gold manifest rows appended (status=READY).")  
    
    
    
ingest_to_silver(
    manifest_path="manifests/silver_manifest.csv",
    md_rules_path="meta_data/silver_md.csv",
    bronze_root="2_bronze",
    silver_output="3_silver",
    flex_rules_path="rules/books_rules.json"
)