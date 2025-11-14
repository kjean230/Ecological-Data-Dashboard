# ====== 2015 TABLE HAS ALL NECESSARY INFORMATION ========= #

import math
import pandas as pd
import mysql.connector
from mysql.connector import errorcode
from pathlib import Path
from datetime import datetime

from config import DB_CONFIG  # expects host, port, user, password, database

# ==== USER SETTINGS ====
# creates a path inside MacBook 'Finder' to 'find' the folder containing the CSV file
# accesses the table in SQL Server and imports them in batches '10k each'
CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "2015_Street_Tree_Census_-_Tree_Data_20251014_copy.csv"
TABLE = "nyc_open_source_database_2015"
BATCH_SIZE = 10000
FILE_NAME_FOR_PROVENANCE = CSV_PATH.name
# ========================

# generate all columns that will be used in DBeaver
# database normalization
INSERT_COLUMNS = [
    "tree_id", "block_id", "created_at", "tree_dbh", "stump_diam",
    "curb_loc", "status", "health", "health_3cat",
    "spc_latin", "spc_common", "steward",
    "guards", "sidewalk", "user_type", "problems", "root_stone",
    "root_grate", "root_other", "trunk_wire", "trnk_light",
    "trnk_other", "brch_light", "brch_shoe", "brch_other", "address",
    "postcode", "zip_city", "community_board", "borocode",
    "borough", "cncldist", "st_assem", "st_senate", "nta",
    "nta_name", "boro_ct", "state", "latitude", "longitude", "x_sp", "y_sp",
    "council_district", "census_tract", "bin", "bbl", "file_name"
]

# function that standardizes all columns to certain requirements
def normalize_headers(cols):
    return [c.strip().lower().replace(" ", "_") for c in cols]

# === Parse 2015 dates like 8/27/15 into ISO YYYY-MM-DD ===
def to_iso_date(s):
    if s is None:
        return None
    t = str(s).strip()
    if t == "" or t.lower() in {"na", "n/a", "null"}:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y", "%m-%d-%y"):
        try:
            return datetime.strptime(t, fmt).date().isoformat()
        except ValueError:
            continue
    return None

# === Normalize health to 3-category ===
def health_to_health_3cat(s):
    if s is None:
        return None
    t = str(s).strip().lower()
    if t == "good": return "Good"
    if t == "fair": return "Fair"
    if t == "poor": return "Poor"
    return None

def main():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV not found at: {CSV_PATH.resolve()}")

    # 1) Read CSV
    df = pd.read_csv(CSV_PATH, dtype=str, keep_default_na=True, na_values=["", "NA", "N/A"])
    df.columns = normalize_headers(df.columns)

    # 2) Drop id if present
    if "id" in df.columns:
        df = df.drop(columns=["id"])

    # 3) Parse date + normalize health
    df["created_at"] = df["created_at"].map(to_iso_date)
    df["health_3cat"] = df["health"].map(health_to_health_3cat)  # Normalizing the health values
    df["file_name"] = FILE_NAME_FOR_PROVENANCE

    # 4) Trim whitespace
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # 5) Order columns
    df = df[INSERT_COLUMNS]

    # 6) Replace NaN/None
    df = df.where(pd.notnull(df), None)

    # 7) Convert to records
    records = df.values.tolist()
    total = len(records)
    print(f"Prepared {total:,} rows from {CSV_PATH.name}")

    # 8) Build INSERT
    placeholders = ", ".join(["%s"] * len(INSERT_COLUMNS))
    col_list = ", ".join(f"`{c}`" for c in INSERT_COLUMNS)
    insert_sql = f"INSERT INTO {TABLE} ({col_list}) VALUES ({placeholders})"

    try:
        cnx = mysql.connector.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            database=DB_CONFIG["database"],
            autocommit=False
        )
        cur = cnx.cursor()

        if total == 0:
            print("Nothing to insert.")
        else:
            batches = math.ceil(total / BATCH_SIZE)
            for b in range(batches):
                start, end = b * BATCH_SIZE, min((b + 1) * BATCH_SIZE, total)
                batch = records[start:end]
                print(f"Inserting rows {start}..{end - 1} ({len(batch)} rows)")
                try:
                    cur.executemany(insert_sql, batch)
                    cnx.commit()
                except Exception as e:
                    cnx.rollback()
                    print(f"Batch {b + 1}/{batches} failed ({start}-{end - 1}): {e}")
                    raise

        # Validation
        cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
        (count,) = cur.fetchone()
        print(f"✅ Table {TABLE} now has {count:,} rows.")

        cur.execute(f"SELECT health_3cat, COUNT(*) FROM {TABLE} GROUP BY health_3cat;")
        print("health_3cat distribution:", cur.fetchall())

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Access denied: check user/password.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist.")
        else:
            print(f"MySQL error: {err}")
        raise
    finally:
        try: cur.close()
        except: pass
        try: cnx.close()
        except: pass

if __name__ == "__main__":
    main()
