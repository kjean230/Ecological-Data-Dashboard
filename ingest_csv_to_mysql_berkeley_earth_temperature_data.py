# ===== PYTHON QUERY TO IMPORT BERKELEY EARTH TEMPERATURE DATA INTO MYSQL ===== #

import math
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import errorcode
from pathlib import Path
from config import DB_CONFIG  # expects: host, port, user, password, database

# ==== USER SETTINGS ====
CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "berkeley-earth-temperature-data.csv"
TABLE = "berkeley_earth_north_america"
BATCH_SIZE = 10000
FILE_NAME_FOR_PROVENANCE = CSV_PATH.name
# ========================

# Constants
BASELINE_GLOBAL = 2.23  # 1951–1980 North America mean
MONTH_BASELINES = {
    1: -11.90, 2: -9.79, 3: -5.83, 4: 0.98,
    5: 8.04, 6: 13.45, 7: 16.23, 8: 15.04,
    9: 10.23, 10: 3.67, 11: -3.75, 12: -9.64
}

INSERT_COLUMNS = [
    "year", "month",
    "monthly_anomaly", "monthly_uncertainty",
    "annual_anomaly", "annual_uncertainty",
    "fiveyear_anomaly", "fiveyear_uncertainty",
    "tenyear_anomaly", "tenyear_uncertainty",
    "twentyyear_anomaly", "twentyyear_uncertainty",
    "abs_monthly_temp", "abs_annual_temp",
    "abs_5y_temp", "abs_10y_temp", "abs_20y_temp",
    "yearly_mean_temp",
    "abs_monthly_temp_f", "abs_annual_temp_f",
    "abs_5y_temp_f", "abs_10y_temp_f", "abs_20y_temp_f",
    "yearly_mean_temp_f",
    "file_name"
]

def to_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None

def c_to_f(x):
    return x * 9 / 5 + 32 if pd.notna(x) else None

def main():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV not found at: {CSV_PATH.resolve()}")

    # === 1. Detect where the real data header starts ===
    with open(CSV_PATH, "r") as f:
        lines = f.readlines()

    start_line = 0
    for i, line in enumerate(lines):
        if line.strip().startswith("Year"):
            start_line = i
            break

    # === 2. Load CSV ===
    df = pd.read_csv(CSV_PATH, engine="python", dtype=str, skiprows=start_line)
    print("Raw columns:", list(df.columns))

    # === 3. Rename columns by position ===
    rename_by_pos = {
        df.columns[0]: "year",
        df.columns[1]: "month",
        df.columns[2]: "monthly_anomaly",
        df.columns[3]: "monthly_uncertainty",
        df.columns[4]: "annual_anomaly",
        df.columns[5]: "annual_uncertainty",
        df.columns[6]: "fiveyear_anomaly",
        df.columns[7]: "fiveyear_uncertainty",
        df.columns[8]: "tenyear_anomaly",
        df.columns[9]: "tenyear_uncertainty",
        df.columns[10]: "twentyyear_anomaly",
        df.columns[11]: "twentyyear_uncertainty"
    }
    df = df.rename(columns=rename_by_pos)
    df = df[list(rename_by_pos.values())]

    # === 4. Convert numeric columns ===
    for c in df.columns:
        df[c] = df[c].map(to_float)

    # === 5. Drop any non-numeric metadata rows ===
    before = len(df)
    df = df.dropna(subset=["year", "month"])
    after = len(df)
    print(f"Dropped {before - after} non-data rows (missing year/month).")

    # === 6. Compute absolute monthly/smoothed temperatures (°C) ===
    def abs_month_temp(row):
        m = int(row["month"]) if pd.notna(row["month"]) else None
        base = MONTH_BASELINES.get(m)
        if pd.notna(row["monthly_anomaly"]) and base is not None:
            return base + row["monthly_anomaly"]
        return np.nan

    df["abs_monthly_temp"] = df.apply(abs_month_temp, axis=1)
    df["abs_annual_temp"] = BASELINE_GLOBAL + df["annual_anomaly"]
    df["abs_5y_temp"] = BASELINE_GLOBAL + df["fiveyear_anomaly"]
    df["abs_10y_temp"] = BASELINE_GLOBAL + df["tenyear_anomaly"]
    df["abs_20y_temp"] = BASELINE_GLOBAL + df["twentyyear_anomaly"]

    # === 7. Calendar-year mean of monthly absolute temps ===
    yearly_mean = (
        df.groupby("year", as_index=False)["abs_monthly_temp"]
          .mean()
          .rename(columns={"abs_monthly_temp": "yearly_mean_temp"})
    )
    df = df.merge(yearly_mean, on="year", how="left")

    # === 8. Convert °C → °F ===
    df["abs_monthly_temp_f"] = df["abs_monthly_temp"].map(c_to_f)
    df["abs_annual_temp_f"] = df["abs_annual_temp"].map(c_to_f)
    df["abs_5y_temp_f"] = df["abs_5y_temp"].map(c_to_f)
    df["abs_10y_temp_f"] = df["abs_10y_temp"].map(c_to_f)
    df["abs_20y_temp_f"] = df["abs_20y_temp"].map(c_to_f)
    df["yearly_mean_temp_f"] = df["yearly_mean_temp"].map(c_to_f)

    # === 9. Add provenance and clean NaNs ===
    df["file_name"] = FILE_NAME_FOR_PROVENANCE
    df = df.replace({np.nan: None})
    df = df[INSERT_COLUMNS]

    records = df.astype(object).values.tolist()
    total = len(records)
    print(f"Prepared {total:,} clean rows for insertion.")

    # === 10. Insert into MySQL ===
    placeholders = ", ".join(["%s"] * len(INSERT_COLUMNS))
    col_list = ", ".join(f"`{c}`" for c in INSERT_COLUMNS)
    insert_sql = f"INSERT INTO {TABLE} ({col_list}) VALUES ({placeholders})"

    try:
        cnx = mysql.connector.connect(**DB_CONFIG)
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

        cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
        (count,) = cur.fetchone()
        print(f"✅ Table {TABLE} now has {count:,} rows.")

        cur.execute("SELECT MAX(abs_monthly_temp), MAX(abs_monthly_temp_f) FROM berkeley_earth_north_america;")
        max_c, max_f = cur.fetchone()
        print(f"🌡️ Hottest absolute monthly temp: {max_c:.2f} °C / {max_f:.2f} °F")

    except mysql.connector.Error as err:
        print(f"MySQL error: {err}")
        raise
    finally:
        try: cur.close()
        except: pass
        try: cnx.close()
        except: pass

if __name__ == "__main__":
    main()
