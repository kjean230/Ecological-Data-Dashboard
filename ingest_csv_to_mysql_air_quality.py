# ===== AIR QUALITY INGESTION SCRIPT (FINAL VERSION) ===== #
# ==== information about air quality is imported into sql server ==== #

import os
import math
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import errorcode
from pathlib import Path
from config import DB_CONFIG  # expects host, port, user, password, database

# ========== USER SETTINGS ==========
CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "Air_Quality_20251018 copy.csv"
TABLE = "nyc_open_source_database_air_quality"
BATCH_SIZE = 1000
FILE_NAME_FOR_PROVENANCE = CSV_PATH.name
# ===================================

# Target columns (must match table)
INSERT_COLUMNS = [
    "unique_id", "indicator_id", "name", "measure", "measure_info", "geo_type_name",
    "geo_type_id", "geo_place_name", "borough_norm_air", "geo_level", "time_period",
    "start_date", "data_value", "message", "file_name"
]

# ---- Normalize headers ----
def normalize_headers(cols):
    """Lowercase, strip, replace spaces with underscores"""
    return [c.strip().lower().replace(" ", "_") for c in cols]


# ---- Secondary borough mapping ----
BOROUGH_MAP_SECONDARY = {
    # Manhattan
    "washington heights": "Manhattan",
    "stuyvesant town": "Manhattan",
    "turtle bay": "Manhattan",
    "new york city": "Manhattan",

    # Bronx
    "throgs neck": "Bronx",
    "co-op city": "Bronx",
    "hunts point": "Bronx",
    "longwood": "Bronx",

    # Brooklyn
    "downtown": "Brooklyn",
    "heights": "Brooklyn",
    "slope": "Brooklyn",
    "carroll gardens": "Brooklyn",
    "park slope": "Brooklyn",
    "east new york": "Brooklyn",
    "greenpoint": "Brooklyn",
    "williamsburg": "Brooklyn",

    # Queens
    "rockaway": "Queens",
    "broad channel": "Queens",
    "jackson heights": "Queens",
    "fresh meadows": "Queens",
    "hillcrest": "Queens",
    "east new york and starrett city": "Brooklyn",
    "rockaways": "Queens",

    # Staten Island
    "southern si": "Staten Island",
    "northern si": "Staten Island",
}

# ---- Borough inference ----
def infer_borough(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        return "Unknown"
    n = name.lower().strip()

    # Step 1: Check extended dictionary first
    for key, borough in BOROUGH_MAP_SECONDARY.items():
        if key in n:
            return borough

    # Step 2: Fallback to general keyword inference
    if any(k in n for k in [
        "bronx", "fordham", "tremont", "crotona", "morris", "mott haven",
        "pelham", "riverdale", "soundview", "williamsbridge", "concourse",
        "parkchester", "highbridge"
    ]):
        return "Bronx"

    if any(k in n for k in [
        "brooklyn", "flatbush", "bushwick", "bedford", "crown heights",
        "borough park", "bensonhurst", "bay ridge", "brownsville", "canarsie",
        "sheepshead", "coney", "flatlands", "midwood", "prospect", "sunset park"
    ]):
        return "Brooklyn"

    if any(k in n for k in [
        "queens", "jamaica", "astoria", "flushing", "elmhurst",
        "forest hills", "corona", "far rockaway", "ridgewood", "kew gardens",
        "bayside", "woodside", "rego park", "little neck", "howard beach",
        "ozone park"
    ]):
        return "Queens"

    if any(k in n for k in [
        "manhattan", "harlem", "upper west side", "upper east side", "chelsea",
        "soho", "village", "midtown", "gramercy", "financial district",
        "tribeca", "morningside", "battery park", "inwood", "lower east side"
    ]):
        return "Manhattan"

    if any(k in n for k in [
        "staten", "tottenville", "st. george", "staten island", "stapleton",
        "willowbrook", "great kills", "new dorp", "richmond", "south beach"
    ]):
        return "Staten Island"

    return "Unknown"


# ---- Geo-level inference ----
def infer_geo_level(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        return "Unknown"
    clean = name.lower().strip()
    boroughs = ["bronx", "brooklyn", "queens", "manhattan", "staten island"]
    if clean in boroughs:
        return "Borough"
    return "Neighborhood"


def main():
    if not Path(CSV_PATH).exists():
        raise FileNotFoundError(f"CSV not found at: {CSV_PATH.resolve()}")

    # 1️⃣ Read CSV
    df = pd.read_csv(CSV_PATH, dtype=str, keep_default_na=True, na_values=["", "NA", "N/A"])
    df.columns = normalize_headers(df.columns)

    # 2️⃣ Trim whitespace
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # 3️⃣ Parse dates
    if "start_date" in df.columns:
        df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce").dt.date
    else:
        df["start_date"] = None

    # 4️⃣ Numeric conversions
    df["unique_id"] = pd.to_numeric(df.get("unique_id"), errors="coerce").astype("Int64")
    df["indicator_id"] = pd.to_numeric(df.get("indicator_id"), errors="coerce").astype("Int64")
    df["geo_type_id"] = pd.to_numeric(df.get("geo_join_id"), errors="coerce").astype("Int64")
    df["data_value"] = pd.to_numeric(df.get("data_value"), errors="coerce")

    # 5️⃣ Add provenance, borough, and geo level
    df["borough_norm_air"] = df["geo_place_name"].map(infer_borough)
    df["geo_level"] = df["geo_place_name"].map(infer_geo_level)
    df["file_name"] = FILE_NAME_FOR_PROVENANCE

    # 6️⃣ Reorder columns
    df = df[INSERT_COLUMNS]

    # 7️⃣ Clean NaNs
    df = df.replace(
        {pd.NA: None, np.nan: None, "nan": None, "NaN": None, "": None, "None": None}
    )

    # 8️⃣ Convert to records
    records = df.astype(object).where(pd.notnull(df), None).values.tolist()
    total = len(records)
    print(f"Prepared {total:,} rows from {CSV_PATH.name}")

    # 9️⃣ Insert query
    placeholders = ", ".join(["%s"] * len(INSERT_COLUMNS))
    col_list = ", ".join(INSERT_COLUMNS)
    insert_sql = f"INSERT INTO {TABLE} ({col_list}) VALUES ({placeholders})"

    # 🔟 Connect and insert
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

        # ✅ Verification
        cur.execute(f"SELECT COUNT(*), COUNT(CASE WHEN borough_norm_air='Unknown' THEN 1 END) FROM {TABLE}")
        total_count, unknown_count = cur.fetchone()
        print(f"✅ {TABLE} has {total_count:,} rows, {unknown_count:,} unknowns remaining.")

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
