# ===== PYTHON INGEST FOR NOAA MONTHLY WEATHER INTO weather_monthly ===== #

import math
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import errorcode
from pathlib import Path
from datetime import datetime

from config import DB_CONFIG   # expects host, port, user, password, database

# === USER SETTINGS ===
CSV_DIR = Path(__file__).resolve().parent.parent / "data" / "weather"
TABLE = "weather_monthly"
BATCH_SIZE = 5000
# =====================

# Fahrenheit → Celsius
def f_to_c(f):
    if f is None:
        return None
    try:
        return (float(f) - 32) * 5.0 / 9.0
    except:
        return None

def to_date_month(s):
    """Convert NOAA date like 2024-03 into YYYY-MM-01"""
    if not isinstance(s, str):
        return None
    try:
        dt = datetime.strptime(s.strip(), "%Y-%m")
        return dt.strftime("%Y-%m-01")
    except:
        return None

def clean_numeric(val):
    """Convert '', NaN, or strings to None"""
    if val is None:
        return None
    if isinstance(val, float) and np.isnan(val):
        return None
    val = str(val).strip()
    if val == "" or val.lower() in {"nan", "na", "null"}:
        return None
    try:
        return float(val)
    except:
        return None

def load_csv_files():
    """Return list of all CSV files inside data/weather directory."""
    return sorted(CSV_DIR.glob("*.csv"))

def main():

    csv_files = load_csv_files()
    print(f"Found {len(csv_files)} CSV files to import.")

    if not csv_files:
        return

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

        for csv_path in csv_files:
            print(f"\n=== Loading {csv_path.name} ===")

            df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)

            # Normalize column names
            df.columns = [c.strip().lower() for c in df.columns]

            # Ensure required columns exist
            required = ["station", "name", "date"]
            for col in required:
                if col not in df.columns:
                    raise ValueError(f"Column '{col}' missing in {csv_path.name}")

            # Convert types
            df["station_id"] = df["station"]
            df["station_name"] = df["name"]
            df["date_month"] = df["date"].map(to_date_month)

            # Numeric fields
            for col in ["cdsd", "hdsd"]:
                if col in df.columns:
                    df[col] = df[col].map(lambda x: int(clean_numeric(x)) if clean_numeric(x) is not None else None)
                else:
                    df[col] = None

            for col in ["emnt", "emxt", "tavg", "tmax", "tmin"]:
                if col in df.columns:
                    df[col] = df[col].map(clean_numeric)
                else:
                    df[col] = None

            # Celsius conversions
            df["tavg_c"] = df["tavg"].map(f_to_c)
            df["tmax_c"] = df["tmax"].map(f_to_c)
            df["tmin_c"] = df["tmin"].map(f_to_c)

            df["file_name"] = csv_path.name

            # Keep only columns required by SQL
            INSERT_COLUMNS = [
                "station_id","station_name","date_month",
                "cdsd","emnt","emxt","hdsd","tavg","tmax","tmin",
                "tavg_c","tmax_c","tmin_c",
                "file_name"
            ]

            df = df[INSERT_COLUMNS]

            # Drop rows missing date or station_id
            df = df[df["date_month"].notnull()]
            df = df[df["station_id"].notnull()]

            # Convert to Python objects
            df = df.replace({np.nan: None})
            records = df.values.tolist()
            total = len(records)

            print(f"Prepared {total} clean rows from {csv_path.name}")

            # UPSERT logic — prevents duplicates
            placeholders = ", ".join(["%s"] * len(INSERT_COLUMNS))
            col_list = ", ".join(f"`{c}`" for c in INSERT_COLUMNS)
            update_list = ", ".join(f"{c}=VALUES({c})" for c in INSERT_COLUMNS if c not in {"station_id","date_month"})

            insert_sql = f"""
                INSERT INTO {TABLE} ({col_list})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE
                {update_list}
            """

            # Insert
            batches = math.ceil(total / BATCH_SIZE)
            start = 0
            for batch_num in range(batches):
                end = min(start + BATCH_SIZE, total)
                batch = records[start:end]
                print(f"Inserting rows {start}..{end-1}")

                try:
                    cur.executemany(insert_sql, batch)
                    cnx.commit()
                except Exception as e:
                    cnx.rollback()
                    print(f"FAILED during {csv_path.name} batch {batch_num+1}: {e}")
                    raise

                start = end

        print("\n✅ All CSVs imported successfully.")

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
