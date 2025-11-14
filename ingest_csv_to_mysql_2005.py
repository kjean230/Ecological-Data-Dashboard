# ===== PYTHON QUERY TO IMPORT DATA FROM 2005 CSV FILE INTO MYSQL (with health_3cat) ===== #
# ===== 2005 SQL TABLE HAS ALL ROWS IMPORTED ALONGSIDE GEOM UPDATE ======= #

import math
import re
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import errorcode
from pathlib import Path
from config import DB_CONFIG  # host, port, user, password, database

# ==== USER SETTINGS ====
CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "2005_Street_Tree_Census_20251014 copy.csv"
TABLE = "nyc_open_source_database_2005"
BATCH_SIZE = 10000
# =======================

# Columns to insert (exclude auto-increment 'objectid')
# NOTE: includes health_3cat as the LAST column we insert
INSERT_COLUMNS = [
    "cen_year", "tree_dbh", "address", "tree_loc", "pit_type", "soil_lvl",
    "status", "spc_latin", "spc_common",
    "vert_other", "vert_pgrd", "vert_tgrd", "vert_wall",
    "horz_blck", "horz_grate", "horz_plant", "horz_other",
    "sidw_crack", "sidw_raise",
    "wire_htap", "wire_prime", "wire_2nd", "wire_other",
    "inf_canopy", "inf_guard", "inf_wires", "inf_paving", "inf_outlet", "inf_shoes",
    "inf_lights", "inf_other",
    "trunk_dmg", "zipcode", "zip_city", "cb_num", "borocode", "boroname",
    "cncldist", "st_assem", "st_senate", "nta", "nta_name", "boro_ct", "state",
    "latitude", "longitude", "x_sp", "y_sp",
    "objectid_1", "census_tract", "bin", "bbl", "location_1",
    "file_name",         # provenance
    "health_3cat"        # normalized 3-bucket health (computed from status)
]

# Source CSV columns after normalization (lowercase + underscores)
SRC_COLUMNS = [
    "objectid","cen_year","tree_dbh","address","tree_loc","pit_type","soil_lvl",
    "status","spc_latin","spc_common",
    "vert_other","vert_pgrd","vert_tgrd","vert_wall",
    "horz_blck","horz_grate","horz_plant","horz_other",
    "sidw_crack","sidw_raise",
    "wire_htap","wire_prime","wire_2nd","wire_other",
    "inf_canopy","inf_guard","inf_wires","inf_paving","inf_outlet","inf_shoes",
    "inf_lights","inf_other",
    "trunk_dmg","zipcode","zip_city","cb_num","borocode","boroname",
    "cncldist","st_assem","st_senate","nta","nta_name","boro_ct","state",
    "latitude","longitude","x_sp","y_sp","objectid_1","census_tract","bin","bbl","location_1"
]

# Column groups for conversions
BOOL_COLS = [
    "vert_other","vert_pgrd","vert_tgrd","vert_wall",
    "horz_blck","horz_grate","horz_plant","horz_other",
    "sidw_crack","sidw_raise",
    "wire_htap","wire_prime","wire_2nd","wire_other",
    "inf_canopy","inf_guard","inf_wires","inf_paving","inf_outlet","inf_shoes",
    "inf_lights","inf_other"
]

INT_COLS = ["tree_dbh","borocode","cncldist","st_assem","st_senate","objectid_1"]
DEC_COLS = ["latitude","longitude","x_sp","y_sp"]
YEAR_COLS = ["cen_year"]

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

def to_year(s):
    v = to_int(s)
    return v if v is not None else None

# === NEW: map 2005 status -> normalized 3-bucket health ===
# Your rule: Excellent -> Good, Good -> Fair, Poor/Dead/Fair -> Poor
def status_to_health_3cat(s):
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

    # Validate columns
    missing = [c for c in SRC_COLUMNS if c not in df.columns]
    if missing:
        print("CSV columns after normalization:", list(df.columns))
        raise ValueError(f"CSV missing expected columns: {missing}")

    # Drop CSV's own objectid (DB auto_increment)
    if "objectid" in df.columns:
        df = df.drop(columns=["objectid"])

    # Trim whitespace
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Type conversions
    for c in BOOL_COLS:
        if c in df: df[c] = df[c].map(to_bool)

    for c in INT_COLS:
        if c == "tree_dbh":
            df[c] = df[c].map(lambda x: to_int_bounded(x, 0, 400))
        else:
            df[c] = df[c].map(to_int)

    for c in DEC_COLS:
        if c in df: df[c] = df[c].map(to_dec)

    for c in YEAR_COLS:
        if c in df: df[c] = df[c].map(to_year)

    # Provenance + normalized health
    df["file_name"] = CSV_PATH.name
    df["health_3cat"] = df["status"].map(status_to_health_3cat)

    # Reorder to match insert columns
    df = df[INSERT_COLUMNS]

    # Robust NaN cleanup -> None
    df = df.replace({np.nan: None, np.inf: None, -np.inf: None, "nan": None, "NaN": None})
    df = df.applymap(lambda x: None if (x == "" or str(x).lower() == "nan") else x)

    records = df.astype(object).where(pd.notnull(df), None).values.tolist()
    total = len(records)
    print(f"Prepared {total:,} rows from {CSV_PATH.name}")

    placeholders = ", ".join(["%s"] * len(INSERT_COLUMNS))
    col_list = ", ".join(INSERT_COLUMNS)
    insert_sql = f"INSERT INTO {TABLE} ({col_list}) VALUES ({placeholders})"

    # Insert batches
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
        print(f"Table {TABLE} now has {count:,} rows.")

        # quick sanity check on health buckets
        cur.execute(f"SELECT health_3cat, COUNT(*) FROM {TABLE} GROUP BY health_3cat")
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
