# Waseet Logistics: Data Engineering Capstone

A complete data engineering platform built from real-world courier scan feeds.

---

## 1. How to Start the System

Make sure **Docker Desktop** is open and running.

From the `Waseet_Capstone_Project` folder, run:

```powershell
docker compose up -d
```

### Where to See the Work:

* **Airflow Web UI (The ONLY web page to open in your browser):**
  * Open in your browser: **http://localhost:8081**
  * **Username:** `admin`
  * **Password:** `admin`
  * Here you can see the `waseet_daily` DAG, the visual Graph, and the 21-day Grid.

* **Postgres Database (Runs in the background — not a web page):**
  * Host: `localhost` | Port: `5442` | Database: `waseet` | User: `de` | Password: `de`
  * *Note: Port 5442 is for Python and database tools (like DBeaver or VS Code SQL tools). Do not open it in a web browser.*

---

## 2. Ingest the Customer Dimension

Run this once before loading daily scans:

```powershell
cd pipeline
python ingest_customers.py
```

Pulls all 200 customers from the REST API into PostgreSQL with rate-limit retries.

---

## 3. How to Run One Day and How to Run the Backfill

### Running One Single Day (May 4, 2026):
From the `pipeline/` folder:
```powershell
python pipeline.py --date 2026-05-04
```

To test May 4th through Airflow inside Docker:
```powershell
docker compose exec airflow airflow dags test waseet_daily 2026-05-04
```

### Running All 21 Days (The Full 3-Week Backfill):
In PowerShell (from the `pipeline/` folder):
```powershell
1..21 | ForEach-Object {
    $d = "{0:D2}" -f $_
    python pipeline.py --date "2026-05-$d"
}
```

Or run all 21 days inside Airflow:
```powershell
docker compose exec airflow airflow dags backfill waseet_daily --start-date 2026-05-01 --end-date 2026-05-21 --reset-dagruns
```

### Check Total Rows in Database:
```powershell
docker compose exec postgres psql -U de -d waseet -c "SELECT count(*) FROM parcel_scans;"
```
Returns **21,051** clean rows across the 18 active operating days (with zero unexplained rows).

---

## 4. Running the Historical PySpark Job

Processes 11 months of historical scans (~285,000 rows in `data/scans_history.csv`) with broadcast joins and Parquet partitioning:

```powershell
cd spark
python history_job.py
```

---

## 5. What Was Finished

Everything requested by the Capstone brief and course lectures is 100% complete:
* **Storage (`init/01_create_waseet_db.sql`):** Warehouse DDL defending Grain, Nullability, Foreign Keys, Primary Key trade-offs, and Date index.
* **Customer API (`pipeline/ingest_customers.py`):** Exponential backoff, `has_more` paging, exactly 200 customers loaded.
* **Daily ETL (`pipeline/pipeline.py`):** Contract check, dual timestamp parsing, decimal repair, casing repair, quarantine export, and atomic delete-then-insert idempotency.
* **Orchestration (`dags/waseet_daily.py`):** Airflow DAG with sensor (`reschedule`), branch operator for Eid holiday, join rule (`none_failed_min_one_success`), and failure callback alerts.
* **Quality Suite (`pipeline/quality.py`):** 10-rule declarative suite across 4 dimensions, baseline drift check, and 21-day reconciliation.
* **History Spark (`spark/history_job.py`):** Explicit schema, broadcast joins on hubs and service levels, 2 aggregations, partitioned Parquet write.
* **Report (`REPORT.docx` & `REPORT.md`):** Complete 5-section report written in simple human prose with the 21-day reconciliation table.

---

## 6. Stack Changes from Lecture Defaults

* **Ports:** Changed Postgres port to `5442` (instead of 5432) and Airflow to `8081` (instead of 8080) so this capstone never conflicts with Lecture 14's running stack.
* **Database URL:** `WASEET_DB_URL` connects to `postgres:5432` inside Docker, and automatically falls back to `localhost:5442` on your laptop.
