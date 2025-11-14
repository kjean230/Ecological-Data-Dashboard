# ===== PYTHON FILE TO EXPORT CSV FILE DATA TO DATABASE  ===== #

import os
import math
import pandas as pd
import mysql.connector
from mysql.connector import errorcode
from pathlib import Path

from config import DB_CONFIG # expects keys: port, root, password, user and the database name

# ==== USER SETTING FOR IMPORTING DATA FROM CSV FILE COPY ==== #
CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "Hyperlocal_Temperature_Monitoring_20251018 copy.csv"
TABLE = "hyperlocal_temperature_monitoring"
BATCH_SIZE = 10000
FILE_NAME_FOR_PROVENANCE = CSV_PATH.name
# ============================

INSERT_COLUMNS = [
    "senor_id", "air_temp", "day", "hour", "latitude", "longitude",
    "year", "install_type", "borough", "ntacode"
]

def normalize_headers(cols):
    out = []
    for col in cols:
        col = col.strip().lower().replace(" ", "_")
        out.append(col)
    return out

def main():
    if not Path(CSV_PATH).exists():
        raise FileNotFoundError(f"CSV not found at: {CSV_PATH.resolve()}")

    df = pd.read_csv(CSV_PATH, dtype=str, keep_default_na=True, na_values=["", "NA", "N/A"])
    df.columns = normalize_headers(df.columns)

    missing = [c for c in SRC_COLUMNS if c not in df.columns]
    if missing:
        print("CSV columns after normalization:", list(df.columns))
        raise ValueError(f"CSV missing expected columns: {missing}")

    if "objectid" in df.columns:
        df = df.drop(columns=["objectid"])

    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

