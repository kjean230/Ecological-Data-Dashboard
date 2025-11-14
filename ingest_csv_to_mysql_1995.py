# ===== PYTHON QUERY TO IMPORT DATA FROM 1995 CSV FILE INTO MYSQL (with health_3cat) ===== #
# ==== TABLE 1995 IN SQL SERVER HAS ALL DATA NORMALIZED ==== ##

import math
import re
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import errorcode
from pathlib import Path
from config import DB_CONFIG  # expects: host, port, user, password, database

# ==== USER SETTINGS ====
CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "1995_Street_Tree_Census_20251014 copy.csv"
TABLE = "nyc_open_source_database_1995"
BATCH_SIZE = 10000
# =======================

# Target columns in DB (order matters; exclude auto 'id')
INSERT_COLUMNS = [
    "record_id", "address", "house_number", "street", "postcode_original",
    "community_board_original", "site", "species", "diameter", "condition",
    "health_3cat",  # new normalized health column
    "wires", "sidewalk_condition", "support_structure", "borough",
    "x", "y", "longitude", "latitude",
    "cb_new", "zip_new", "censustract_2010", "censusblock_2010",
    "nta_2010", "segmentid", "spc_common", "spc_latin", "location",
    "council_district", "bin", "bbl",
    "file_name"  # provenance
]

SRC_COLUMNS = [
    "recordid","address","house_number","street","postcode_original",
    "community_board_original","site","species","diameter","condition",
    "wires","sidewalk_condition","support_structure","borough",
    "x","y","longitude","latitude",
    "cb_new","zip_new","censustract_2010","censusblock_2010",
    "nta_2010","segmentid","spc_common","spc_latin","location",
    "council_district","bin","bbl"
]

def normalize_headers(cols):
    return [c.strip().lower().replace(" ", "_") for c in cols]

def to_bool(s):
    if s is None: return None
    t = str(s).strip().lower()
    if t in {"y","yes","1","true","t"}: return 1
    if t in {"n","no","0","false","f"}: return 0
    return None

def to_int(s):
    if s is None: return None
    t = re.sub(r"[,\s]", "", str(s))
    return int(t) if t != "" and re.fullmatch(r"-?\d+", t) else None

def to_int_bounded(s, low=0, high=400):
    v = to_int(s)
    if v is None: return None
    return v if (low <= v <= high) else None

def to_dec(s):
    if s is None: return None
    t = re.sub(r"[,\s]", "", str(s))
    try:
        return float(t) if t != "" else None
    except ValueError:
        return None

# === NEW: map 1995 condition -> normalized 3-bucket health ===
def condition_to_health_3cat(s):
    if s is None: return None
    t = str(s).strip().lower()
    if t in {"excellent", "e"}: return "Good"
    if t in {"good", "g"}:     return "Fair"
    if t in {"poor", "p", "dead", "d", "fair", "f"}: return "Poor"
    return None

def main():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV not found at: {CSV_PATH.resolve()}")

    # Read CSV and normalize headers
    df = pd.read_csv(CSV_PATH, dtype=str, keep_default_na=True, na_values=["", "NA", "N/A"])
    df.columns = normalize_headers(df.columns)

    # Verify the expected columns exist
    missing = [c for c in SRC_COLUMNS if c not in df.columns]
    if missing:
        print("CSV columns after normalization:", list(df.columns))
        raise ValueError(f"CSV is missing expected columns: {missing}")

    # Trim whitespace
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # Build columns used downstream
    df["record_id"] = df["recordid"].map(to_int)
    df["diameter"] = df["diameter"].map(lambda x: to_int_bounded(x, 0, 400))
    df["wires"] = df["wires"].map(to_bool)

    # Numerics
    for col in ["x", "y", "longitude", "latitude"]:
        df[col] = df[col].map(to_dec)

    # Council/bin/bbl numeric-ish
    df["council_district"] = df["council_district"].map(to_int)
    df["bin"] = df["bin"].map(to_int)
    df["bbl"] = df["bbl"].map(to_int)

    # Provenance + health
    df["file_name"] = CSV_PATH.name
    df["health_3cat"] = df["condition"].map(condition_to_health_3cat)

    # Arrange in exact insert order
    df_insert = df[INSERT_COLUMNS]

    # Robust NaN cleanup -> None
    df_insert = df_insert.replace({np.nan: None, np.inf: None, -np.inf: None, "nan": None, "NaN": None})
    df_insert = df_insert.applymap(lambda x: None if (x == "" or str(x).lower() == "nan") else x)

    # Convert to plain Python objects
    records = df_insert.astype(object).where(pd.notnull(df_insert), None).values.tolist()
    total = len(records)
    print(f"Prepared {total:,} rows from {CSV_PATH.name}")

    # Build INSERT (use backticks for safety)
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
            autocommit=False,
            charset="utf8mb4",
            use_unicode=True
        )
        cur = cnx.cursor()

        if total == 0:
            print("Nothing to insert.")
        else:
            batches = math.ceil(total / BATCH_SIZE)
            start = 0
            for b in range(batches):
                end = min(start + BATCH_SIZE, total)
                batch = records[start:end]
                print(f"Inserting rows {start}..{end-1} ({len(batch)} rows)")
                try:
                    cur.executemany(insert_sql, batch)
                    cnx.commit()
                except Exception as e:
                    cnx.rollback()
                    print(f"Batch {b+1}/{batches} failed ({start}-{end-1}): {e}")
                    raise
                start = end

        cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
        (count,) = cur.fetchone()
        print(f"✅ Table {TABLE} now has {count:,} rows.")

        # Quick sanity check
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
