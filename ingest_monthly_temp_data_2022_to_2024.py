# ===== Ingest NOAA CDO CGSM monthly CSV into MySQL =====
import math
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import errorcode
from pathlib import Path
from config import DB_CONFIG  # {host, port, user, password, database}

# --- user settings ---
CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "monthly_temp_data_2022_to_2024.csv"
TABLE = "cdo_cgsm_monthly"
BATCH_SIZE = 5000
FILE_NAME = CSV_PATH.name
# ---------------------

# expected headers (case-insensitive)
REQUIRED = ["station", "name", "date", "cdsd", "emnt", "emxt", "hdsd", "tavg", "tmax", "tmin"]

# normalize columns
def norm_cols(cols):
    return [c.strip().lower().replace(" ", "_") for c in cols]

def to_float(x):
    try:
        if pd.isna(x): return None
        s = str(x).strip()
        if s == "": return None
        return float(s)
    except Exception:
        return None

def to_month_start(val):
    # input like '2023-07' or '2023-7'
    if pd.isna(val): return None
    s = str(val).strip()
    if len(s) == 7 and s[4] == "-":
        y, m = s.split("-")
    else:
        parts = s.split("-")
        if len(parts) < 2: return None
        y, m = parts[0], parts[1]
    try:
        y = int(y); m = int(m)
        return f"{y:04d}-{m:02d}-01"
    except Exception:
        return None

def main():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    # read as strings; let us control conversion
    df = pd.read_csv(CSV_PATH, dtype=str, keep_default_na=True)
    df.columns = norm_cols(df.columns)

    # check headers
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise ValueError(f"CSV missing columns: {missing}. Found: {list(df.columns)}")

    # derive month_start and clean numerics
    df["month_start"] = df["date"].map(to_month_start)
    for col in ["cdsd", "hdsd", "emnt", "emxt", "tavg", "tmax", "tmin"]:
        df[col] = df[col].map(to_float)

    # trim text
    for col in ["station", "name"]:
        df[col] = df[col].astype(str).str.strip()

    # provenance
    df["file_name"] = FILE_NAME

    # keep only the insert columns in order
    insert_cols = [
        "station", "name", "month_start",
        "cdsd", "hdsd", "emxt", "emnt", "tavg", "tmax", "tmin",
        "file_name"
    ]
    df = df[insert_cols]

    # drop rows without a station or month_start
    before = len(df)
    df = df[ df["station"].ne("").fillna(False) & df["month_start"].notna() ]
    after = len(df)
    if after < before:
        print(f"Dropped {before - after} rows missing station/month_start.")

    # convert NaN -> None for MySQL
    records = df.astype(object).where(pd.notnull(df), None).values.tolist()
    total = len(records)
    print(f"Prepared {total:,} rows from {FILE_NAME}")

    placeholders = ", ".join(["%s"] * len(insert_cols))
    col_list = ", ".join(f"`{c}`" for c in insert_cols)

    # UPSERT to avoid duplicates on (station, month_start)
    update_clause = ", ".join(
        f"`{c}`=VALUES(`{c}`)" for c in
        ["name", "cdsd", "hdsd", "emxt", "emnt", "tavg", "tmax", "tmin", "file_name"]
    )
    insert_sql = (
        f"INSERT INTO {TABLE} ({col_list}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_clause}"
    )

    try:
        cnx = mysql.connector.connect(**DB_CONFIG, autocommit=False)
        cur = cnx.cursor()

        if total == 0:
            print("Nothing to insert.")
        else:
            batches = math.ceil(total / BATCH_SIZE)
            for b in range(batches):
                start = b * BATCH_SIZE
                end = min((b + 1) * BATCH_SIZE, total)
                batch = records[start:end]
                print(f"Inserting rows {start}..{end-1} ({len(batch)})")
                try:
                    cur.executemany(insert_sql, batch)
                    cnx.commit()
                except Exception as e:
                    cnx.rollback()
                    print(f"Batch {b+1}/{batches} failed: {e}")
                    raise

        # quick check
        cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
        (n_rows,) = cur.fetchone()
        print(f"✅ {TABLE} now has {n_rows:,} rows.")

        cur.execute(
            f"""SELECT station, MIN(month_start), MAX(month_start)
                  FROM {TABLE}
              GROUP BY station"""
        )
        for row in cur.fetchall():
            print("Coverage:", row)

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Access denied.")
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
