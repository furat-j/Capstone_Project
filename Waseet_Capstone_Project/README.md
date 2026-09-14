# Waseet Logistics Data Engineering Platform

A production-grade warehouse, idempotent ETL pipeline, Airflow orchestration suite, quality & schema drift monitor, and historical PySpark batch analytics built from daily parcel logistics feeds.

---

## 1. How to Start the Stack

Make sure **Docker Desktop** is running. From the `Waseet_Capstone_Project/` directory, run:

```bash
# 1. Spin up the containers (Postgres 16 warehouse & Airflow 2.10.5)
docker compose up -d --build

# 2. Monitor Airflow startup until you see "Airflow is ready" (press Ctrl+C to exit logs)
docker compose logs -f airflow

# 3. Verify that both services are healthy and running
docker compose exec -T airflow airflow version
docker compose exec -T postgres psql -U de -d waseet -c "\dt"
```

The database check should list all **6 tables**:
`hubs`, `service_levels`, `couriers`, `customers`, `parcel_scans`, and `load_log`.

### Service URLs and Credentials

| Service | Address (from Laptop) | Internal (Docker Network) | Credentials / Details |
|---|---|---|---|
| **Airflow Web UI** | `http://localhost:8081` | `http://airflow:8080` | `admin` / `admin` |
| **Postgres Warehouse** | `localhost:5442` | `postgres:5432` | DB: `waseet`, User: `de`, Pass: `de` |
| **Airflow Metadata DB** | `localhost:5442` | `postgres:5432` | DB: `airflow`, User: `de`, Pass: `de` |

---

## 2. Ingesting the Customer Dimension (API Pull)

Before running the daily scan pipeline, pull the customer reference dimension from the API into the warehouse:

```bash
cd pipeline
python ingest_customers.py
```

* **What it does:** Starts the local API server in `api/waseet_api.py`, authenticates via `X-API-Key: waseet-demo-key`, handles transient HTTP 429 rate limits using exponential backoff retry, dynamically pages through records using `has_more`, and idempotently upserts all **200 customers** into Postgres.
* Verify:
  ```bash
  docker compose exec -T postgres psql -U de -d waseet -c "SELECT count(*) FROM customers;"
  ```
  Returns `200`.

---

## 3. How to Run One Day and How to Run the Backfill

### Running One Day from Terminal (Local / Container)

You can run any single date directly using `pipeline.py`:

```bash
cd pipeline
python pipeline.py --date 2026-05-04
```

To run it inside the Airflow container:
```bash
docker compose exec -T airflow bash -c "cd /opt/airflow/pipeline && python pipeline.py --date 2026-05-04"
```

* **Idempotency check:** Run the command twice for `2026-05-04`. The number of rows in `parcel_scans` will not double; it remains identical (`1,261` rows).

### Running the Data Quality & Reconciliation Suite

```bash
cd pipeline
python quality.py --date 2026-05-04
```

Checks the 10 data quality rules across 4 dimensions, verifies schema against `schema_baseline.json`, and reconciles raw file rows against warehouse loaded rows.

### Testing Individual Days in Airflow (`dags test`)

To test how Airflow executes a day through the FileSensor, BranchPythonOperator, pipeline, and join:

```bash
# Standard day (loads pipeline and quality suite)
docker compose exec -T airflow airflow dags test waseet_daily 2026-05-04

# Missing day (May 10 - sensor times out and routes alert to supplier on-call)
docker compose exec -T airflow airflow dags test waseet_daily 2026-05-10

# Eid holiday (May 15 - empty file routes through skip_day branch silently)
docker compose exec -T airflow airflow dags test waseet_daily 2026-05-15

# Contract breach (May 19 - column renamed to 'weight', fails loudly before transform)
docker compose exec -T airflow airflow dags test waseet_daily 2026-05-19
```

### Running the Full 3-Week Backfill (May 1 to May 21)

You can run the backfill across all 21 days via a simple terminal loop:

```bash
# From laptop terminal in Waseet_Capstone_Project/pipeline:
for day in $(seq -w 1 21); do
    python pipeline.py --date "2026-05-$day"
done
```

Or trigger the backfill directly inside Airflow:
```bash
docker compose exec -T airflow airflow dags backfill waseet_daily \
    --start-date 2026-05-01 \
    --end-date 2026-05-21 \
    --reset-dagruns
```

---

## 4. Running the Historical Spark Batch Job

The Spark job analyzes 11 months of historical scans (~285,000 rows in `data/scans_history.csv`), executes broadcast joins, computes hub and regional delivery metrics, and writes partitioned Parquet files.

Run it on your machine from the `spark/` folder:

```bash
cd spark
python history_job.py
```
*(Or via `spark-submit history_job.py`)*

* **Prerequisites:** Python environment with `pyspark`, Java 11 or 17 (`JAVA_HOME`), and Windows winutils binary set in `HADOOP_HOME`.

---

## 5. What Was Built and Finished

All six required layers are fully implemented:
1. **Storage Layer (`init/01_create_waseet_db.sql`):** Fully documented DDL for `parcel_scans` and `load_log` defending Grain, Nullability, Foreign Keys, Primary Key trade-offs, and Date indexing.
2. **Customer Ingestion Layer (`pipeline/ingest_customers.py`):** Exponential backoff on HTTP 429/5xx, `has_more` envelope pagination, and idempotent upsert into the `customers` table (200 records).
3. **Daily Scan Pipeline (`pipeline/pipeline.py`):** Contract check, dual-format timestamp parser, comma decimal repair, mixed-case standardization, rejected rows quarantined with reasons, atomic transactional load, and complete row accounting ($\text{rows\_read} = \text{rows\_loaded} + \text{rows\_rejected} + \text{rows\_deduplicated}$).
4. **Orchestration Layer (`dags/waseet_daily.py`):** Airflow DAG with `FileSensor` in `reschedule` mode, `BranchPythonOperator` routing empty files without alerts, join task with `trigger_rule="none_failed_min_one_success"`, and routed on-call failure callbacks.
5. **Quality & Drift Layer (`pipeline/quality.py`):** 10-rule suite defined as data, schema drift detection separating missing vs new columns, and full 21-day reconciliation.
6. **Processing Layer (`spark/history_job.py`):** Explicit schema Spark job, broadcast joins on dimension lookups, daily hub counts, monthly Delivered vs Failed percentages, and partitioned Parquet write.

---

## 6. Stack & Environment Configuration

* **Ports:** Postgres runs on `localhost:5442` (internal `5432`); Airflow UI runs on `localhost:8081` (internal `8080`) to prevent port conflicts with earlier course modules.
* **Database URLs:** Pipeline scripts read `WASEET_DB_URL` from the environment (`postgresql+psycopg2://de:de@postgres:5432/waseet` in container) and fall back automatically to `postgresql+psycopg2://de:de@localhost:5442/waseet` on the host laptop.
* **Packages:** Dockerfile pins `pandas==2.1.4` and `SQLAlchemy==1.4.54` to ensure compatibility with Airflow 2.10.5 and avoid SQLAlchemy 2.0 `cursor` incompatibilities.