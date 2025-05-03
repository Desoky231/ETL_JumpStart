import logging
from pathlib import Path
from datetime import datetime
import pandas as pd
from utils import BooksGoldTransformer
import traceback



def ingest_to_gold(
    manifest_path: str | Path,
    silver_root: str | Path,
    gold_output: str | Path,
    gold_md_path : str | Path,
    flex_rules_path: str | Path
):
    """
    Process Silver files into Gold layer and update the Gold manifest.

    Parameters
    ----------
    manifest_path : str or Path
        Path to the gold_manifest.csv.
    silver_root : str or Path
        Root folder of the Silver layer files.
    gold_output : str or Path
        Root folder where Gold outputs will be saved.
    flex_rules_path : str or Path
        Path to the rules JSON file for Gold transformations.
    """
    
    # Ensure Path objects
    SILVER_ROOT = Path(silver_root)
    GOLD_OUTPUT = Path(gold_output)
    flex_rules_path = Path(flex_rules_path)
    gold_manifest_path = Path(manifest_path)

    # Load gold manifest
    manifest = pd.read_csv(gold_manifest_path)

    # Load gold metadata
    
    gold_md = pd.read_csv(gold_md_path)
    
    
    # Make sure all important columns are strings
    for col in ["silver_partition", "rows_out", "status", "error_msg"]:
        manifest[col] = manifest[col].astype("string")

    ready = manifest[manifest["status"] == "READY"].copy()
    if ready.empty:
        print("✔️  Nothing to process; no rows in READY state.")
        return

    print(f"➡️  Found {len(ready)} Silver file(s) to Transform.")

    for idx, row in ready.iterrows():
        try:
            silver_path = SILVER_ROOT / row["silver_partition"] / row["silver_file"]
            table_name = row["silver_file"]

            print(silver_path)

            if not silver_path.exists():
                raise FileNotFoundError(f"{silver_path} missing")

            # Run Gold transformation
            gold_transformer = (
                BooksGoldTransformer(
                    silver_df=silver_path,  
                    table_name=table_name,
                    rules=None,                          # no global rules in this run
                    language_json=flex_rules_path        # load flexible rules (publisher/language mappings)
                )
                .transform()
            )
            
            
                        # 👉 Reorder columns based on gold metadata
            # Extract the expected column order for this table
            expected_cols = (
                gold_md[gold_md["table_name"] == table_name]
                .sort_values("order")["column_name"]
                .tolist()
            )

            # Only reorder if we actually have a defined schema
            missing_cols = [col for col in expected_cols if col not in gold_transformer.df.columns]
            if missing_cols:
                print(f"⚠️ Warning: missing columns in {table_name}: {missing_cols}")

            # Reorder columns (skip missing ones safely)
            final_cols = [col for col in expected_cols if col in gold_transformer.df.columns]
            gold_transformer.df = gold_transformer.df[final_cols]


            gold_dir = GOLD_OUTPUT / row["silver_partition"]
            out_path = gold_transformer.save(out_dir=gold_dir)

            # Update Gold manifest on success
            manifest.loc[idx, "gold_processed_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            manifest.loc[idx, "rows_out"] = str(len(gold_transformer.df))
            manifest.loc[idx, "status"] = "SUCCESS"
            manifest.loc[idx, "error_msg"] = ""

            print(f"✅ Cleaned → {out_path}")

        except Exception as e:
            # Update Gold manifest on failure
            manifest.loc[idx, "gold_processed_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            manifest.loc[idx, "status"] = "FAILED"
            manifest.loc[idx, "error_msg"] = str(e)

            # Also log full traceback to the logs
            full_trace = traceback.format_exc()
            logging.error("❌ Failed on %s : %s", row.get("silver_file", "Unknown file"), full_trace)

    # Persist manifest updates
    manifest.to_csv(manifest_path, index=False)
    print("📝 Gold manifest updated.")
    
    
    
ingest_to_gold(
    manifest_path="manifests/gold_manifest.csv",
    silver_root="3_silver",
    gold_output="4_gold",
    gold_md_path = 'meta_data/gold_md.csv',
    flex_rules_path="rules/books_gold_rules.json"
)