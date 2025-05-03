import os
import shutil
import logging
import pandas as pd
from datetime import datetime
from ydata_profiling import ProfileReport
from pathlib import Path
from utils import append_manifest_row


# Setup logging
log_file = "ingestion_log.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# constant header for silver manifest
SILVER_HEADER = [
    "table_name", "source_name", "bronze_partition", "bronze_file",
    "ingestion_timestamp", "silver_processed_at",
    "rows_in", "rows_out", "status", "error_msg"
]

# ──────────────────────────────────────────────────────────────
#   main Bronze ingestion function
# ──────────────────────────────────────────────────────────────
def ingest_to_bronze(
        ingest_md_path: str,
        table_name: str | None = None,
        ingestion_date: str | None = None,
        manifest_path: str = "manifests\silver_manifest.csv"
):

    df = pd.read_csv(ingest_md_path)
    if table_name:
        df = df[df["table_name"] == table_name]
        
    if df.empty:
        logging.warning("No rows match provided filters; nothing to ingest.")
        return

    for idx, row in df.iterrows():
        try:
            # ── core vars
            
            # Read base columns normally
            src_dir  = row["source_path"]
            dst_dir  = row["destination_path"]
            fname    = row["file_name"]
            fmt      = row["file_format"]
            delim    = row["delimiter"]
            src_name = row["source_name"]

            # Get today's date dynamically
            now = datetime.now()
            ingestion_date = f"{now.month}/{now.day}/{now.year}"
            
            
            # extract the day, month and year
            dt_obj = datetime.strptime(ingestion_date, "%m/%d/%Y")

            year = dt_obj.year
            month = f"{dt_obj.month:02}"  # zero-padded month
            day = f"{dt_obj.day:02}"      # zero-padded day

            # Override destination path to today's date format
            dst_dir = Path(dst_dir) 
            src_file = os.path.join(src_dir, fname)
            if not os.path.exists(src_file):
                logging.error("Missing file '%s' in '%s'", fname, src_dir)
                continue

            # ── quick validation
            if fmt.lower() != "csv":
                logging.warning("Unsupported format '%s' for '%s'. Skipped.", fmt, fname)
                continue
            try:
                pd.read_csv(src_file, delimiter=delim, nrows=5,
                            engine="python", quotechar='"', on_bad_lines="skip")
            except Exception as e:
                logging.error("CSV validation failed for '%s': %s", fname, e)
                continue

            # ── folder naming (hierarchy)
            dt_obj = datetime.strptime(ingestion_date, "%m/%d/%Y")
            
            year, month, day = dt_obj.year, f"{dt_obj.month:02}", f"{dt_obj.day:02}"

            bronze_sub = os.path.join( dst_dir,src_name,
                                      f"year={year}", f"month={month}", f"day={day}")
            archive_dir = os.path.join(src_dir, "processed",
                                       f"year={year}", f"month={month}", f"day={day}")
            os.makedirs(bronze_sub, exist_ok=True)
            os.makedirs(archive_dir,  exist_ok=True)

            # ── full read & row count
            df_full = pd.read_csv(src_file, sep=",", engine="python",
                                  quotechar='"', on_bad_lines="skip")
            rows_in = len(df_full)

            # ── profiling report
            try:
                profile = ProfileReport(df_full,
                                         title=f"{fname} Profiling",
                                         explorative=True,
                                         minimal=True)
                report_path = os.path.join(bronze_sub, f"{Path(fname).stem}_profile.html")
                profile.to_file(report_path)
            except Exception as e:
                logging.error("Profiling failed for '%s': %s", fname, e)

            # ── copy to Bronze & archive
            bronze_path  = os.path.join(bronze_sub, fname)
            archive_path = os.path.join(archive_dir, fname)
            print(bronze_path)
            shutil.copy(src_file, bronze_path)
            shutil.move(src_file, archive_path)
            logging.info("Ingested '%s' → Bronze & archived.", fname)

            # ── build manifest row & append
            manifest_row = {
                "table_name": row["table_name"],
                "source_name": src_name,
                "bronze_partition": f"{src_name}/year={year}/month={month}/day={day}",
                "bronze_file": fname,
                "ingestion_timestamp": datetime.utcnow().isoformat(timespec="seconds")+"Z",
                "silver_processed_at": "",
                "rows_in": rows_in,
                "rows_out": "",
                "status": "READY",
                "error_msg": ""
            }
            append_manifest_row(manifest_row, manifest_path, SILVER_HEADER)

        except Exception as err:
            logging.error("Row %s processing error: %s", idx, err)

    print("✔️  Ingestion finished; profiling reports & silver_manifest.csv updated.")
    
    
# 3.1  Ingest everything
ingest_to_bronze(r"meta_data/ingestion_md.csv")
