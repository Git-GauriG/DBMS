#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
HW8 - Azure SQL Ingestion
- Reads two CSVs from ./HW8/data (script-relative).
- Uploads to Azure SQL as dbo.brand and dbo.daily_spend.
- Works on hosted agents by trying ODBC Driver 18 -> 17 -> legacy.
"""

import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# ---------- CONFIG: hard-coded per your setup ----------
SERVER   = "hwdbms-server.database.windows.net"
DATABASE = "hwdbms"
USERNAME = "hwdbms"
PASSWORD = "Init@123"

BASE_DIR  = Path(__file__).parent
BRAND_CSV = str((BASE_DIR / "data" / "brand-detail-url-etc_0_0_0.csv").resolve())
SPEND_CSV = str((BASE_DIR / "data" / "2021-01-19--data_01be88c2-0306-48b3-0042-fa0703282ad6_1304_5_0.csv").resolve())
# -------------------------------------------------------


def build_engine():
    """
    Try multiple ODBC drivers so the Azure DevOps hosted agent works.
    """
    candidates = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server",  # legacy last-resort
    ]

    last_err = None
    for driver in candidates:
        try:
            print(f"[INFO] Trying ODBC driver: {driver}")
            odbc_str = (
                f"DRIVER={{{driver}}};"
                f"SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};"
                "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
            )
            url = URL.create("mssql+pyodbc", query={"odbc_connect": odbc_str})
            eng = create_engine(url, fast_executemany=True)
            # quick probe
            with eng.connect() as conn:
                conn.exec_driver_sql("SELECT 1")
            print(f"[INFO] Using driver: {driver}")
            return eng
        except Exception as e:
            last_err = e
            print(f"[WARN] Driver failed: {driver} -> {type(e).__name__}: {e}")

    raise RuntimeError(f"No usable ODBC driver found on agent. Last error: {last_err}")


def read_csv_any(path: str) -> pd.DataFrame:
    """
    Read CSV robustly (handles UTF-8 or latin-1), keep strings, no NAs.
    """
    try:
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    except UnicodeDecodeError:
        return pd.read_csv(path, dtype=str, keep_default_na=False, encoding="latin-1")


def main():
    print("[INFO] Reading CSV files...")
    brand_df = read_csv_any(BRAND_CSV)
    spend_df = read_csv_any(SPEND_CSV)

    # Normalize column names (lowercase) for consistency
    brand_df.columns = [c.lower() for c in brand_df.columns]
    spend_df.columns = [c.lower() for c in spend_df.columns]

    print("[INFO] Connecting to Azure SQL...")
    eng = build_engine()

    print("[INFO] Writing brand table...")
    brand_df.to_sql("brand", eng, schema="dbo", if_exists="replace", index=False)

    print("[INFO] Writing daily_spend table...")
    spend_df.to_sql("daily_spend", eng, schema="dbo", if_exists="replace", index=False)

    # Quick sanity counts
    with eng.connect() as conn:
        for t in ("brand", "daily_spend"):
            cnt = conn.execute(text(f"SELECT COUNT(*) FROM dbo.{t}")).scalar_one()
            print(f"[OK] {t}: {cnt} rows")

    print("[DONE] Ingestion complete.")


if __name__ == "__main__":
    main()
