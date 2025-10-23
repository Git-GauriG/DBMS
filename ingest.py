#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
HW8 - Azure SQL Ingestion (Mac-friendly)
- Reads two CSVs from ./HW8/data (script-relative) by default.
- Uploads to Azure SQL as dbo.brand and dbo.daily_spend.
- Tries ODBC Driver 18 (preferred) then 17 as fallback.
- Works on macOS (Homebrew msodbcsql18 installed) and Windows agents.
"""

from __future__ import annotations
import os
from pathlib import Path
from typing import Optional
import urllib.parse

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# ---------- CONFIG: set per your setup (overridable by env) ----------
SERVER   = os.getenv("AZURE_SQL_SERVER",   "hwdbms-server.database.windows.net")
DATABASE = os.getenv("AZURE_SQL_DATABASE", "hwdbms")
USERNAME = os.getenv("AZURE_SQL_USER",     "hwdbms")
PASSWORD = os.getenv("AZURE_SQL_PASSWORD", "Init@123")

BASE_DIR  = Path(__file__).parent
# Defaults match the filenames from the assignment; override with env if needed
BRAND_CSV = os.getenv("BRAND_CSV", str((BASE_DIR / "data" / "brand-detail-url-etc_0_0_0.csv").resolve()))
SPEND_CSV = os.getenv("SPEND_CSV", str((BASE_DIR / "data" / "2021-01-19--data_01be88c2-0306-48b3-0042-fa0703282ad6_1304_5_0.csv").resolve()))
# ---------------------------------------------------------------------


def build_engine():
    """Create a SQLAlchemy engine using pyodbc and ODBC Driver 18/17."""
    candidates = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
    ]
    last_err: Optional[Exception] = None
    for driver in candidates:
        try:
            print(f"[INFO] Trying ODBC driver: {driver}")
            odbc = (
                f"DRIVER={{{driver}}};"
                f"SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};"
                "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
            )
            url = URL.create("mssql+pyodbc", query={"odbc_connect": odbc})
            eng = create_engine(url, fast_executemany=True)
            # Probe quickly
            with eng.connect() as conn:
                conn.exec_driver_sql("SELECT 1")
            print(f"[INFO] Using driver: {driver}")
            return eng
        except Exception as e:
            last_err = e
            print(f"[WARN] Driver failed: {driver} -> {type(e).__name__}: {e}")
    raise RuntimeError(f"No usable ODBC driver found. Last error: {last_err}")

def read_csv_any(path: str) -> pd.DataFrame:
    """Read CSV robustly with strings preserved; supports latin-1 fallback."""
    try:
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    except UnicodeDecodeError:
        return pd.read_csv(path, dtype=str, keep_default_na=False, encoding="latin-1")

def ensure_exists(path: str) -> bool:
    p = Path(path)
    if not p.exists():
        print(f"[WARN] File not found: {p}. Set BRAND_CSV/SPEND_CSV env vars if paths differ.")
        return False
    return True

def main():
    print("[INFO] Connecting to Azure SQL...")
    eng = build_engine()

    # Optional smoke test
    with eng.connect() as conn:
        ver = conn.execute(text("SELECT @@VERSION")).scalar()
        print(f"[OK] Connected to SQL Server:\n{ver}\n")

    # Ingest CSVs if present
    if ensure_exists(BRAND_CSV):
        print(f"[INFO] Reading brand CSV: {BRAND_CSV}")
        brand_df = read_csv_any(BRAND_CSV)
        brand_df.columns = [c.lower() for c in brand_df.columns]
        print("[INFO] Writing dbo.brand ...")
        brand_df.to_sql("brand", eng, schema="dbo", if_exists="replace", index=False)

    if ensure_exists(SPEND_CSV):
        print(f"[INFO] Reading spend CSV: {SPEND_CSV}")
        spend_df = read_csv_any(SPEND_CSV)
        spend_df.columns = [c.lower() for c in spend_df.columns]
        print("[INFO] Writing dbo.daily_spend ...")
        spend_df.to_sql("daily_spend", eng, schema="dbo", if_exists="replace", index=False)

    # Sanity counts (if tables were created)
    with eng.connect() as conn:
        for t in ("brand", "daily_spend"):
            try:
                cnt = conn.execute(text(f"SELECT COUNT(*) FROM dbo.{t}")).scalar()
                print(f"[OK] {t}: {cnt} rows")
            except Exception:
                pass

    print("[DONE] Ingestion complete.")

if __name__ == "__main__":
    main()
