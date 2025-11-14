# ===== PYTHON QUERY TO IMPORT CDO MONTHLY TEMPERATURE DATA INTO MYSQL ===== #

import math
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import errorcode
from pathlib import Path
from config import DB_CONFIG  # expects host, user, password, database

# ==== USER SETTINGS ====
CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "monthly_temp_data_2022_to_2024.csv"
TABLE = "weather_monthly"
BATCH_SIZE = 10000
FILE_NAME_FOR_PROVENANCE = CSV_PATH.name
# ========================

# Fahrenheit → Celsius conversion
def f_to_c(f):
    if f is None or pd.isna(f):
        return None
    try:
        return (float(f) - 32) * 5/9
    except:
        return None

# Clean column headers
def normalize_headers(cols):
    out = []
    for col in cols:
        col = col.strip().lower().replace(" ", "_")
        out.append(col)
    return out

def main():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    # Load CSV
    df = pd.read_csv(CSV_PATH, dtype=str)
    df.columns = normalize_headers(df.columns)

    print("Raw columns:", list(df.columns))

    # Clean DATE into MySQL DATE (YYYY-MM-01)
    df["date_month"] = (
        df["date"]
        .str.strip()
        .apply(lambda x: f"{x}-01" if pd.notna(x) else None)
    )

    # Numeric conversions
    numeric_cols = ["cdsd", "emnt", "emxt", "hdsd", "tavg", "tmax", "tmin"]
    for col in numeric_cols:
        df[col] = df[col].apply(lambda x: float(x) if pd.notna(x) and x != "" else None)

    # Celsius conversions
    df["tavg_c"] = df["tavg"].apply(f_to_c)
    df["tmax_c"] = df["tmax"].apply(f_to_c)
    df["tmin_c"] = df["tmin"].apply(f_to_c)

    # Add provenance
    df["file_name"] = FILE_NAME_FOR_PROVENANCE

    # Define insert order
    INSERT_COLUMNS = [
        "station_id", "station_name", "date_month",
        "cdsd", "emnt", "emxt", "hdsd",
        "tavg", "tmax", "tmin",
        "tavg_c", "tmax_c", "tmin_c",
        "file_name"
    ]

    # Reorder df
    df = df.rename(columns={
        "station": "station_id",
        "name": "station_name"
    })

    df = df[INSERT_COLUMNS]

    # Replace NaN with None
    df = df.replace({np.nan: None})

    records = df.values.tolist()
    total = len(records)
    print(f"Prepared {total:,} rows for insertion.")

    # Build insert SQL
    placeholders = ", ".join(["%s"] * len(INSERT_COLUMNS))
    col_list = ", ".join(f"`{c}`" for c in INSERT_COLUMNS)
    insert_sql = f"INSERT INTO {TABLE} ({col_list}) VALUES ({placeholders})"

    # Insert into MySQL
    try:
        cnx = mysql.connector.connect(
            host=DB_CONFIG["host"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            database=DB_CONFIG["database"],
            autocommit=False
        )
        cur = cnx.cursor()

        batches = math.ceil(total / BATCH_SIZE)
        for b in range(batches):
            start = b * BATCH_SIZE
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

        cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
        (count,) = cur.fetchone()
        print(f"✅ Table {TABLE} now has {count:,} rows.")

    except mysql.connector.Error as err:
        print("MySQL error:", err)
        raise
    finally:
        try: cur.close()
        except: pass
        try: cnx.close()
        except: pass


if __name__ == "__main__":
    main()
