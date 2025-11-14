# ===== PYTHON FILE TO EXPORT CSV FILE DATA TO DATABASE  ===== #

import math
import re
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import errorcode
from pathlib import Path
from config import DB_CONFIG  # expects: host, port, user, password, database
from src.ingest_csv_to_mysql_2015 import FILE_NAME_FOR_PROVENANCE

# ==== USER SETTINGS ====
CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "Heat_Vulnerability_Index_Rankings_20251018 copy.csv"
TABLE = "heat_vulnerability_index_rankings"
BATCH_SIZE = 10
FILE_NAME_FOR_PROVENANCE = CSV_PATH.name
# =======================

# Columns for database
INSERT_COLUMNS = [
    "zip_code_tabulation_area",
    "heat_vulerability_index"
]

def normalize_headers(cols):
    # Lowercase + spaces -> underscores (matches our SRC_COLUMNS)
    return [c.strip().lower().replace(" ", "_") for c in cols]

def main ():
    if not Path(CSV_PATH).exists():
        raise FileNotFoundError(f"CSV not found at {CSV_PATH.resolve()}")

    # == reads the csv file using read_csv method from pandas
    # == also makes all columns lowercase to make seemless transition of information easy
    df = pd.read_csv(CSV_PATH, dtype=str, keep_default_na=True, na_values=["", "NA", "N/A"])
    df.columns = normalize_headers(df.columns)

    # == excludes the id column within the table
    # == similar to skipping over
    if "id" in df.columns:
        df = df.drop(columns=["id"])

    # == check if all columns are present
    # == translates for loop into simple one liner code
    expected_without_file = [c for c in INSERT_COLUMNS if c != "file_name"]
    missing = [c for c in expected_without_file if c not in df.columns]
    if missing:
        print("CSV columns after normalization:", list(df.columns))
        raise ValueError(f"Your CSV is missing these columns: {missing}")

    df["file_name"] = FILE_NAME_FOR_PROVENANCE

    # == similar to the regular map function
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # == applies the lambda function to
    df = df[INSERT_COLUMNS]

    records = df.where(pd.notnull(df), None).values.tolist()
    total = len(records)
    print(f"Prepared {total} rows from {CSV_PATH}")

    # == inserts values into the table's columns
    placeholders = ", ".join(["%s"] * len(INSERT_COLUMNS))
    col_list = ", ".join(INSERT_COLUMNS)
    insert_sql = f"INSERT INTO {TABLE} ({col_list}) VALUES ({placeholders})"

    # == connecting to mysql server using config py file
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
                print(f"Inserting rows {start}..{end - 1} ({len(batch)} rows)")
                try:
                    cur.executemany(insert_sql, batch)
                    cnx.commit()
                except Exception as e:
                    cnx.rollback()
                    print(f"Batch {b + 1}/{batches} failed on rows {start}-{end - 1}: {e}")
                    raise
                start = end

        cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
        count, = cur.fetchone()
        print(f"Table {TABLE} has {count} rows.")

    # == may need to raise multiple errors for each possible error when
    # == trying to access database server
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(f"MySQL error: {err}")
        raise
    finally:
        try:
            cnx.close()
        except Exception:
            pass
        try:
            cnx.close()
        except Exception:
            pass

# == runs main function
if __name__ == "__main__":
    main()