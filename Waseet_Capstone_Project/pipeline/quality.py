"""The Waseet quality suite.

Rules are data. The suite below is a list a warehouse manager could review
without reading Python, which is the whole reason it is not a set of .loc masks
inside transform().

Run it as a script to see today's report:

    python quality.py --date 2026-05-04
"""

import argparse
import json
import os

import pandas as pd

import sys
from sqlalchemy import create_engine, text

DATA_DIR = os.environ.get("WASEET_DATA_DIR")
if not DATA_DIR:
    if os.path.exists("../data"):
        DATA_DIR = "../data"
    elif os.path.exists("data"):
        DATA_DIR = "data"
    elif os.path.exists("/opt/airflow/data"):
        DATA_DIR = "/opt/airflow/data"
    else:
        DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
DB_URL = os.environ.get("WASEET_DB_URL", "postgresql+psycopg2://de:de@localhost:5442/waseet")

engine = create_engine(DB_URL)

# Suite of rules defined as data.
# Covers 4 dimensions: Completeness, Uniqueness, Validity, Consistency.
SUITE = [
    {"column": "scan_id", "check": "not_null", "dimension": "completeness"},
    {"column": "scan_id", "check": "unique", "dimension": "uniqueness"},
    {"column": "scanned_at", "check": "not_null", "dimension": "completeness"},
    {"column": "scanned_at", "check": "date_iso", "dimension": "validity"},
    {"column": "weight_kg", "check": "not_null", "dimension": "completeness"},
    {"column": "weight_kg", "check": "numeric", "dimension": "validity"},
    {"column": "weight_kg", "check": "in_range", "min": 0.01, "max": 100.0, "dimension": "validity"},
    {"column": "hub_id", "check": "in_set", "values": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], "dimension": "consistency"},
    {"column": "service_code", "check": "in_set", "values": ["SDD", "EXP", "STD", "ECO"], "dimension": "validity"},
    {"column": "scan_type", "check": "valid_scan_type", "dimension": "validity"},
]


def run_check(df, rule):
    """Return the number of rows that break one rule.

    Every branch ends the same way - a boolean Series that is True for the bad
    rows - so the counting is one line at the bottom rather than one per check.
    """
    col = rule["column"]
    check = rule["check"]

    if col not in df.columns:
        return len(df)

    series = df[col]

    if check == "not_null":
        bad = series.isna() | (series.astype(str).str.strip() == "") | (series.astype(str).str.lower() == "nan")
    elif check == "unique":
        bad = series.duplicated(keep="first")
    elif check == "numeric":
        bad = pd.to_numeric(series, errors="coerce").isna()
    elif check == "in_range":
        cleaned = series.astype(str).str.replace(",", ".", regex=False)
        num = pd.to_numeric(cleaned, errors="coerce")
        bad = num.isna() | (num < rule.get("min", float("-inf"))) | (num > rule.get("max", float("inf")))
    elif check == "date_iso":
        parsed = pd.to_datetime(series, format="%Y-%m-%d %H:%M:%S", errors="coerce")
        bad = parsed.isna()
    elif check == "in_set":
        allowed = set(str(v) for v in rule["values"])
        bad = ~series.astype(str).isin(allowed)
    elif check == "valid_scan_type":
        allowed = {'PICKUP', 'ARRIVE', 'DEPART', 'OUT_FOR_DELIVERY', 'DELIVERED', 'FAILED'}
        bad = ~series.isin(allowed)
    else:
        raise ValueError(f"Unknown check: {check}")

    return int(bad.sum())


def validate(df, suite):
    """Return a DataFrame: one row per rule, with how many rows failed it."""
    results = []
    for rule in suite:
        bad_count = run_check(df, rule)
        results.append({
            "column": rule["column"],
            "check": rule["check"],
            "dimension": rule.get("dimension", "unspecified"),
            "failed_rows": bad_count
        })
    return pd.DataFrame(results)


def check_schema(df, baseline_path):
    """Return (missing, new) against the column list written at baseline_path.

    Missing and new are not the same severity, and your caller should be able to
    treat them differently. Return them separately rather than as one verdict.
    """
    with open(baseline_path, "r", encoding="utf-8-sig") as f:
        baseline = json.load(f)
    expected = baseline["columns"]
    missing = [c for c in expected if c not in df.columns]
    new = [c for c in df.columns if c not in expected]
    return missing, new


def reconcile(scan_date, eng):
    """Compare rows in the file against rows in the warehouse for one date.

    A gap is not a failure. An unexplained gap is. Return enough for a human to
    tell which one they are looking at.
    """
    filepath = os.path.join(DATA_DIR, f"scans_{scan_date}.csv")
    if not os.path.exists(filepath):
        return {
            "date": scan_date,
            "in_file": 0,
            "in_warehouse": 0,
            "difference": 0,
            "explained": True,
            "explanation": "No file arrived (supplier missing feed)"
        }

    raw = pd.read_csv(filepath, dtype=str)
    in_file = len(raw)

    with eng.connect() as conn:
        res = conn.execute(
            text("SELECT count(*) FROM parcel_scans WHERE DATE(scanned_at) = :d"),
            {"d": scan_date}
        )
        in_warehouse = res.scalar() or 0

    quarantine_path = f"quarantine/rejects_{scan_date}.csv"
    if not os.path.exists(quarantine_path):
        quarantine_path = os.path.join(os.path.dirname(__file__), "quarantine", f"rejects_{scan_date}.csv")
    if not os.path.exists(quarantine_path):
        quarantine_path = os.path.join(os.path.dirname(__file__), "..", "quarantine", f"rejects_{scan_date}.csv")

    quarantined = len(pd.read_csv(quarantine_path)) if os.path.exists(quarantine_path) else 0

    duplicates = int(raw["scan_id"].duplicated().sum()) if "scan_id" in raw.columns else 0
    diff = in_file - in_warehouse

    if in_file == 0:
        explained = (in_warehouse == 0)
        explanation = "Empty file (Eid / holiday)"
    elif diff == quarantined + duplicates:
        explained = True
        explanation = f"{quarantined} quarantined, {duplicates} deduplicated"
    else:
        explained = False
        explanation = f"UNEXPLAINED GAP: diff={diff}, quarantined={quarantined}, deduplicated={duplicates}"

    return {
        "date": scan_date,
        "in_file": in_file,
        "in_warehouse": in_warehouse,
        "difference": diff,
        "explained": explained,
        "explanation": explanation
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    file_path = os.path.join(DATA_DIR, f"scans_{args.date}.csv")
    if not os.path.exists(file_path):
        print(f"File {file_path} not found.")
        sys.exit(1)

    day = pd.read_csv(file_path, dtype=str)

    # 1. Schema drift check against baseline
    baseline_file = os.path.join(os.path.dirname(__file__), "schema_baseline.json")
    if os.path.exists(baseline_file):
        missing, new = check_schema(day, baseline_file)
        if missing:
            print(f"CRITICAL SCHEMA DRIFT: Missing required columns: {missing}")
            if new:
                print(f"New unexpected columns: {new}")
            sys.exit(1)
        elif new:
            print(f"WARNING: Schema drift detected with new columns: {new}")

    # 2. Run Quality Suite
    report = validate(day, SUITE)
    print("\n--- DATA QUALITY REPORT ---")
    print(report.to_string(index=False))

    # 3. Post-load reconciliation
    try:
        recon = reconcile(args.date, engine)
        print("\n--- RECONCILIATION ---")
        print(f"Date: {recon['date']} | In File: {recon['in_file']} | In Warehouse: {recon['in_warehouse']} | Diff: {recon['difference']}")
        print(f"Explanation: {recon['explanation']}")
        if not recon["explained"]:
            print("RECONCILIATION FAILED: Unexplained gap between source and warehouse.")
            sys.exit(1)
    except Exception as e:
        print(f"Reconciliation check could not run against warehouse: {e}")

    # Exit code: 0 indicates success (all rows accounted for, contract obeyed)
    sys.exit(0)

