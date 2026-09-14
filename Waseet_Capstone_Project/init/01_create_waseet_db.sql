-- Waseet Logistics warehouse.
--
-- This file runs once, the first time the postgres container starts on an empty
-- volume. If you change it afterwards, nothing happens until you run
-- `docker compose down -v` and bring the stack back up.
--
-- Four dimensions, one fact table, one run log. The dimensions are given. The
-- fact table and the log are yours to finish - see the TODOs.

DROP TABLE IF EXISTS load_log;
DROP TABLE IF EXISTS parcel_scans;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS couriers;
DROP TABLE IF EXISTS service_levels;
DROP TABLE IF EXISTS hubs;

CREATE TABLE hubs (
    hub_id   INTEGER PRIMARY KEY,
    hub_name TEXT NOT NULL,
    city     TEXT NOT NULL,
    country  TEXT NOT NULL,
    region   TEXT NOT NULL
);

CREATE TABLE service_levels (
    service_code    TEXT PRIMARY KEY,
    service_name    TEXT NOT NULL,
    promised_hours  INTEGER NOT NULL
);

CREATE TABLE couriers (
    courier_id   INTEGER PRIMARY KEY,
    courier_name TEXT NOT NULL,
    hub_id       INTEGER NOT NULL REFERENCES hubs(hub_id),
    vehicle_type TEXT NOT NULL
);

-- Loaded from the API, not from a file. It is a dimension like any other once
-- it lands; where it came from stops mattering at the warehouse boundary.
CREATE TABLE customers (
    customer_id   INTEGER PRIMARY KEY,
    customer_name TEXT NOT NULL,
    segment       TEXT NOT NULL,
    city          TEXT,
    signup_date   DATE,
    credit_limit  NUMERIC(12, 2)
);

-- The scan events fact table.
--
-- Decision 1 - Grain:
-- One row represents a single physical scan event of a parcel at a specific
-- moment in time, recording its status, hub location, courier (if assigned),
-- parcel weight, and service level.
--
-- Decision 2 - Nullability:
-- courier_id is explicitly NULLABLE because unassigned scans (such as hub
-- sorting, arrival, or departure) are legitimate business occurrences. All other
-- columns (scanned_at, parcel_id, customer_id, hub_id, scan_type, weight_kg,
-- service_code) are NOT NULL because a scan event missing a timestamp, parcel,
-- customer, or physical hub is invalid and cannot be interpreted.
--
-- Decision 3 - Foreign Keys:
-- Explicit foreign keys are declared for hubs, couriers, customers, and
-- service_levels. While the ETL pipeline checks referential integrity at the
-- boundary, warehouse-level foreign keys guarantee that orphan records can never
-- corrupt analytical reporting under any ingestion path.
--
-- Decision 4 - Is scan_id a primary key:
-- No primary key is declared on scan_id, following the Najm sales table
-- design in Lecture 13. Upstream feeds frequently deliver duplicate scan events
-- (e.g., May 13 contains 1,282 duplicated rows), and a database PK turns a data
-- quality defect into a hard crash that aborts the pipeline. Instead,
-- deduplication is handled in the transform layer, and idempotency is enforced
-- at the daily partition level via transactional delete-then-insert.
--
-- Decision 5 - The index:
-- A single index on the timestamp column (scanned_at) is created. The daily load
-- relies on fast filtering/deletion of the daily partition, and daily operational
-- reports filter and sort by timestamp. Avoiding additional indexes keeps write
-- throughput high and storage overhead minimal.
CREATE TABLE parcel_scans (
    scan_id         BIGINT NOT NULL,
    parcel_id       BIGINT NOT NULL,
    customer_id     INTEGER NOT NULL REFERENCES customers(customer_id),
    scanned_at      TIMESTAMPTZ NOT NULL,
    hub_id          INTEGER NOT NULL REFERENCES hubs(hub_id),
    courier_id      INTEGER REFERENCES couriers(courier_id),
    scan_type       TEXT NOT NULL,
    weight_kg       NUMERIC(6, 2) NOT NULL,
    service_code    TEXT NOT NULL REFERENCES service_levels(service_code)
);

CREATE INDEX idx_parcel_scans_date ON parcel_scans (scanned_at);


-- The run log.
-- Records execution metadata for every pipeline run: what date was targeted,
-- timestamps of execution, final status, and exact row accounting satisfying:
-- rows_read = rows_loaded + rows_rejected + rows_deduplicated.
CREATE TABLE load_log (
    run_id              SERIAL PRIMARY KEY,
    scan_date           DATE NOT NULL,
    started_at          TIMESTAMPTZ NOT NULL,
    finished_at         TIMESTAMPTZ NOT NULL,
    status              TEXT NOT NULL,
    rows_read           INTEGER NOT NULL,
    rows_loaded         INTEGER NOT NULL,
    rows_rejected       INTEGER NOT NULL,
    rows_deduplicated   INTEGER NOT NULL
);

