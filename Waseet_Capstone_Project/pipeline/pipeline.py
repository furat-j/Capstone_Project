"""Waseet Logistics daily scan pipeline.

    python pipeline.py --date 2026-05-04

The shape is given so that the DAG in ../dags has something with a known
interface to call. The bodies are yours.

Nothing here is a puzzle. Every function below has a worked equivalent in
Lecture 13's `pipeline.py`; the difference is the data, and the data is the
point. Read a scan file before you write a line of this.
"""

import argparse
import logging
import os
import sys

import pandas as pd
from sqlalchemy import create_engine, text

DATA_DIR = os.environ.get("WASEET_DATA_DIR", "../data")
DB_URL = os.environ.get("WASEET_DB_URL",
                        "postgresql+psycopg2://de:de@localhost:5442/waseet")

# TODO. The contract: the columns a scan file must have for this run to be worth
# starting. One of the twenty-one files renames one of them.
REQUIRED = [
    "scan_id", "parcel_id", "customer_id", "scanned_at",
    "hub_id", "courier_id", "scan_type", "weight_kg", "service_code"
]

os.makedirs("logs", exist_ok=True)
os.makedirs("quarantine", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("logs/pipeline.log"), logging.StreamHandler()])
log = logging.getLogger("waseet")

engine = create_engine(DB_URL)


def record_load_log(scan_date, started_at, status, rows_read, rows_loaded, rows_rejected, rows_deduplicated):
    """Write one row to load_log recording this run's metrics."""
    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO load_log (scan_date, started_at, finished_at, status,
                                          rows_read, rows_loaded, rows_rejected, rows_deduplicated)
                    VALUES (:d, :start, now(), :s, :r, :l, :j, :dedup)
                """),
                {
                    "d": scan_date,
                    "start": started_at,
                    "s": status,
                    "r": rows_read,
                    "l": rows_loaded,
                    "j": rows_rejected,
                    "dedup": rows_deduplicated,
                }
            )
    except Exception as e:
        log.error("Failed to write to load_log: %s", e)


def extract(scan_date):
    """Read the file for one day, as strings.

    Read it with dtype=str and mean it. If you let pandas guess, it will decide
    weight_kg is an object column the moment it meets "12,5", and you will be
    checking pandas' opinion of the file instead of the file.
    """
    path = f"{DATA_DIR}/scans_{scan_date}.csv"
    if not os.path.exists(path):
        raise FileNotFoundError(f"Scan file not found: {path}")
    df = pd.read_csv(path, dtype=str)
    log.info("extract: %d rows from %s", len(df), path)
    return df


def check_contract(df):
    """Fail the run if the file is not the shape we agreed.

    Cheap, first, and before anything else. A missing column means nothing
    downstream can do anything sensible, so there is no reason to find out
    slowly.
    """
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise ValueError(f"Contract violation: missing columns {missing}")
    if len(df) == 0:
        raise ValueError("Contract violation: scan file is empty")
    log.info("contract: ok, %d columns, %d rows", len(df.columns), len(df))


def parse_timestamps(series):
    """Two formats arrive. Return one datetime column.

    Roughly 3% of rows use DD/MM/YYYY HH:MM instead of the ISO form. The
    two-format parse in Lecture 13 is the pattern; note that the wrong format
    with errors="coerce" gives NaT rather than an exception, which is what makes
    fillna work.
    """
    iso = pd.to_datetime(series, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    other = pd.to_datetime(series, format="%d/%m/%Y %H:%M", errors="coerce")
    return iso.fillna(other)


def transform(raw, scan_date, hubs, couriers, customers, services):
    """Return (good, rejects, rows_deduplicated).

    The decisions this function has to make, all of them visible in the data:
      * duplicate scan_id - the same event sent twice
      * timestamps in two formats
      * weight_kg with a comma decimal mark, blank, or impossible (600 kg)
      * hub_id 99, which is not a hub
      * courier_id blank, which is a real unassigned scan and not an error
      * scan_type in mixed case
    """
    df = raw.copy()
    initial_count = len(df)

    # 1. Deduplicate on scan_id
    df = df.drop_duplicates(subset=["scan_id"], keep="first")
    rows_deduplicated = initial_count - len(df)

    # 2. Repair timestamps
    df["parsed_scanned_at"] = parse_timestamps(df["scanned_at"])

    # 3. Repair mixed-case scan_type
    df["scan_type"] = df["scan_type"].astype(str).str.strip().str.upper()

    # 4. Repair weight_kg comma decimal marks
    w_cleaned = df["weight_kg"].astype(str).str.replace(",", ".", regex=False).str.strip()
    w_numeric = pd.to_numeric(w_cleaned, errors="coerce")

    # 5. Classify rejects with explicit reasons
    df["reject_reason"] = None

    # Date parse error
    unparseable_ts = df["parsed_scanned_at"].isna()
    df.loc[unparseable_ts, "reject_reason"] = "unparseable timestamp"

    # Blank/missing weight
    missing_weight = df["weight_kg"].isna() | (df["weight_kg"].str.strip() == "") | (df["weight_kg"].str.lower() == "nan")
    df.loc[df["reject_reason"].isna() & missing_weight, "reject_reason"] = "missing weight"

    # Impossible weight (> 100 kg or <= 0 kg)
    impossible_weight = w_numeric.isna() | (w_numeric <= 0) | (w_numeric > 100)
    df.loc[df["reject_reason"].isna() & impossible_weight, "reject_reason"] = "impossible weight"

    # Unknown hub_id
    valid_hubs = set(hubs["hub_id"].astype(str))
    invalid_hub = ~df["hub_id"].astype(str).isin(valid_hubs)
    df.loc[df["reject_reason"].isna() & invalid_hub, "reject_reason"] = "unknown hub"

    # Unknown customer_id
    valid_customers = set(customers["customer_id"].astype(str))
    invalid_customer = ~df["customer_id"].astype(str).isin(valid_customers)
    df.loc[df["reject_reason"].isna() & invalid_customer, "reject_reason"] = "unknown customer"

    # Unknown service_code
    valid_services = set(services["service_code"].astype(str))
    invalid_service = ~df["service_code"].astype(str).isin(valid_services)
    df.loc[df["reject_reason"].isna() & invalid_service, "reject_reason"] = "unknown service code"

    # Courier check: courier_id blank is legitimate (unassigned scan), but if present must be known
    valid_couriers = set(couriers["courier_id"].astype(str))
    courier_present = df["courier_id"].notna() & (df["courier_id"].str.strip() != "") & (df["courier_id"].str.lower() != "nan")
    invalid_courier = courier_present & (~df["courier_id"].astype(str).isin(valid_couriers))
    df.loc[df["reject_reason"].isna() & invalid_courier, "reject_reason"] = "unknown courier"

    # Unknown scan_type check
    valid_types = {'PICKUP', 'ARRIVE', 'DEPART', 'OUT_FOR_DELIVERY', 'DELIVERED', 'FAILED'}
    invalid_type = ~df["scan_type"].isin(valid_types)
    df.loc[df["reject_reason"].isna() & invalid_type, "reject_reason"] = "unknown scan type"

    # Split into good and rejected
    good = df[df["reject_reason"].isna()].copy()
    rejects = df[df["reject_reason"].notna()].copy()

    # Quarantine rejected rows
    if len(rejects) > 0:
        quarantine_file = f"quarantine/rejects_{scan_date}.csv"
        rejects.to_csv(quarantine_file, index=False)
        log.warning("quarantined %d rows to %s", len(rejects), quarantine_file)

    # Cast good data types for warehouse loading
    good["scan_id"] = good["scan_id"].astype("int64")
    good["parcel_id"] = good["parcel_id"].astype("int64")
    good["customer_id"] = good["customer_id"].astype("int64")
    good["hub_id"] = good["hub_id"].astype("int64")
    good["scanned_at"] = good["parsed_scanned_at"]
    good["weight_kg"] = w_numeric.loc[good.index].round(2)

    # Nullable courier_id: keep nulls as None
    good["courier_id"] = pd.to_numeric(good["courier_id"], errors="coerce").astype("Int64")

    # Strict row accounting
    rows_read = len(raw)
    rows_loaded = len(good)
    rows_rejected = len(rejects)
    if rows_read != rows_loaded + rows_rejected + rows_deduplicated:
        raise ValueError(
            f"Row accounting mismatch: read({rows_read}) != loaded({rows_loaded}) + "
            f"rejected({rows_rejected}) + deduplicated({rows_deduplicated})"
        )

    log.info(
        "transform: %d read = %d good + %d rejected + %d duplicates",
        rows_read, rows_loaded, rows_rejected, rows_deduplicated
    )
    return good[REQUIRED], rejects, rows_deduplicated


def load(good, scan_date):
    """Write one day, idempotently.

    Running this twice for the same date must leave the warehouse exactly as
    running it once did. The 13th of May is a file that was sent twice; that is
    a different problem from the pipeline being run twice, and you need both to
    be handled.
    """
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM parcel_scans WHERE DATE(scanned_at) = :d"),
            {"d": scan_date}
        )
        good.to_sql("parcel_scans", conn, if_exists="append", index=False)
    log.info("load: %d rows committed for %s", len(good), scan_date)
    return len(good)


def main(scan_date):
    log.info("run starting for %s", scan_date)
    started_at = pd.Timestamp.now(tz="UTC")
    rows_read = 0
    rows_loaded = 0
    rows_rejected = 0
    rows_deduplicated = 0

    try:
        raw = extract(scan_date)
        rows_read = len(raw)
        check_contract(raw)

        hubs = pd.read_csv(f"{DATA_DIR}/hubs.csv")
        couriers = pd.read_csv(f"{DATA_DIR}/couriers.csv")
        services = pd.read_csv(f"{DATA_DIR}/service_levels.csv")
        customers = pd.read_sql_query("SELECT customer_id FROM customers", engine)

        good, rejects, rows_deduplicated = transform(raw, scan_date, hubs, couriers, customers, services)
        rows_loaded = len(good)
        rows_rejected = len(rejects)

        load(good, scan_date)
        record_load_log(scan_date, started_at, "success", rows_read, rows_loaded, rows_rejected, rows_deduplicated)
        log.info("run finished for %s", scan_date)
        return 0
    except Exception as problem:
        log.error("run FAILED for %s: %s", scan_date, problem)
        record_load_log(scan_date, started_at, f"failed: {problem}", rows_read, rows_loaded, rows_rejected, rows_deduplicated)
        return 1



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    sys.exit(main(parser.parse_args().date))
