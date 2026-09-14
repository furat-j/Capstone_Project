# Waseet Logistics: Data Engineering Capstone Report

**Author:** Furat  
**System:** Waseet Logistics Data Platform  
**Scope:** Storage DDL, API Ingestion, Daily Batch ETL, Airflow Orchestration, Data Quality & Reconciliation, PySpark Analytics  

---

## Section 1: Schema and why

The grain of the `parcel_scans` table is one row for every single time a parcel is physically scanned at a logistics facility or by a courier.

We chose this grain because parcels go through a multi-step physical journey from pickup to final delivery. A single package gets picked up from a merchant, arrives at an origin sorting hub, departs on an inter-city transit route, arrives at a destination sorting hub, gets handed over to a courier for delivery, and finally gets marked as delivered or failed. If we had made the mistake of storing only one row per parcel, every new scan would overwrite the previous scan, wiping out the history of how the parcel moved through our logistics network. Storing each scan event as its own row allows warehouse managers to measure transit times between hubs, discover where packages sit waiting in bottlenecks, and track courier delivery success rates accurately.

For nullability, we looked closely at the real daily data before defining database constraints. Primary identifiers and critical business columns—namely `scan_id`, `parcel_id`, `customer_id`, `scanned_at`, `hub_id`, `scan_type`, `weight_kg`, and `service_code`—are all declared `NOT NULL`. A scan without a timestamp cannot be ordered in time, a scan without a hub cannot be placed on a map, and a parcel without weight cannot be billed. However, we made `courier_id` explicitly nullable (`NULL`). When a package first arrives at a central sorting hub or moves between cities on an inter-hub freight truck, it is processed by sorting machines or warehouse staff, not by a courier. A scan with no courier is a normal, valid logistics event. If we had marked `courier_id` as `NOT NULL`, the pipeline would have rejected thousands of legitimate warehouse scans.

We placed database foreign keys on the `hubs`, `couriers`, and `service_levels` reference tables. Our pipeline already checks these relationships during the transform step, but keeping foreign key constraints inside PostgreSQL gives us an essential second line of defense. If an engineer runs a manual script in the future or an unexpected bug bypasses the transform code, the database itself guarantees that no scan with an imaginary hub or non-existent courier can enter the warehouse.

For the primary key, we made the deliberate architectural decision not to make `scan_id` an enforced database primary key constraint, following the exact rationale from Lecture 13 regarding Najm Retail's sales table. In logistics operations, suppliers frequently re-send files or include duplicate scan records. On May 13th, the supplier sent a file containing 1,282 duplicate rows. If `scan_id` were an enforced database primary key, PostgreSQL would have thrown an unhandled unique violation error and crashed the daily run immediately. Instead, we deduplicate scan records in Python before inserting them, and we make the daily load idempotent by running an atomic delete-and-insert transaction that deletes the day's existing records before inserting the fresh batch. This separates data quality handling from database crashes.

Finally, we created exactly one B-tree index on the `scanned_at` timestamp column. The daily ETL pipeline queries and deletes by scan date to maintain idempotency, and daily operational reports filter scans by day. Adding five or six additional indexes on foreign keys or scan statuses would significantly slow down our daily batch loading and consume disk space without offering any noticeable query speedup for daily reporting.

Comparing this schema to Lecture 13's Najm Retail table highlights two important differences. First, Najm operated in a single retail city where all sales happened in the same local time, so timestamps carried no timezone information. Waseet Logistics operates across five different countries: Jordan, Lebanon, Saudi Arabia, the UAE, and Egypt. Because scans happen across different time zones, we used `TIMESTAMPTZ` instead of plain timestamps to keep the exact global sequence of events consistent. Second, while Najm validated lookups only against local CSV files, Waseet must validate customers against a live REST API dimension that we ingest with pagination and rate-limit retries.

---

## Section 2: What you found in the data

When profiling the three weeks of daily scan files from May 1 to May 21, 2026, we found several recurring data defects across the feed, along with four major challenge days.

The first recurring defect was mixed timestamp formats. Around 3% of the rows in every single file (roughly 30 to 50 rows each day) were written in UK/Middle Eastern format (`DD/MM/YYYY HH:MM`) instead of standard ISO format (`YYYY-MM-DD HH:MM:SS`). We decided to repair these rather than reject them. The events themselves were completely real and legitimate; they were simply recorded by regional handheld devices configured with local date formats. Our pipeline parses the date using ISO first, falls back to the day-first format when necessary, and standardizes everything into clean timestamps.

The second defect was comma decimal marks. Between 25 and 45 rows each day wrote package weights using a comma instead of a dot, such as '12,5' kg. In Arabic and European numbering conventions, commas represent decimal points. We repaired these by replacing commas with dots and converting them to floating-point numbers.

The third defect was missing and impossible parcel weights. Between 9 and 18 rows each day had completely empty weight values. We rejected and quarantined these rows with the reason 'missing weight'. A delivery company cannot charge customers, plan van weight limits, or clear customs without knowing package weights, and we cannot guess or invent weights out of thin air. In addition, between 4 and 12 rows each day contained impossible weights over 100 kg, sometimes reaching 800 kg or 900 kg. A motorcycle or standard courier van cannot carry a 900 kg package. These are clear scale sensor errors or typing mistakes, so we quarantined them with the reason 'impossible weight'.

The fourth defect was unknown hub IDs. Between 5 and 13 rows each day contained `hub_id = 99`. Looking at `hubs.csv`, Waseet operates hubs 1 through 10 across the region. Hub 99 does not exist in company master data. Routing packages to a non-existent hub leads to lost shipments, so we quarantined these rows with the reason 'unknown hub'.

The fifth issue was blank courier IDs. Between 6 and 19 rows each day had empty courier fields. We kept these rows as completely valid and stored them as `NULL`, because sorting facility arrival and departure scans happen inside warehouses before a package is handed to a courier.

The sixth defect was mixed capital letters in scan types. In every file, more than half the rows had inconsistent casing, such as 'Delivered', 'delivered', or 'DELIVERED'. We repaired these by converting all scan types to uppercase.

The capstone brief warned that three days were known in advance: May 10th (missing file), May 15th (empty file for Eid holiday), and May 19th (renamed column contract breach). However, the brief stated that a fourth bad day was hidden in the data and that our reconciliation must find it. We discovered this fourth bad day on May 13, 2026. When inspecting `scans_2026-05-13.csv`, the file size was more than double the normal daily volume, containing 2,546 rows instead of the usual 1,200 rows. Running a duplicate check revealed that exactly 1,282 rows were identical duplicate scan events sent twice. The supplier's export job had accidentally concatenated and transmitted the daily batch twice. Our pipeline identified the 1,282 duplicates, removed them in memory, quarantined the 30 defective rows, and safely committed the remaining 1,234 clean scans to the warehouse.

---

## Section 3: The quality suite

We implemented the data quality suite in `pipeline/quality.py`. The suite is defined as data—a clean list of rule dictionaries—so that warehouse managers and business analysts can read and modify the rules without reading Python code.

The suite tests ten rules covering four core data quality dimensions: completeness, uniqueness, validity, and consistency. For completeness, we enforce that `scan_id` and `scanned_at` must never be null; both checks run during transformation and post-load, and in the real dataset they caught zero nulls because the supplier always populates identifiers. We also check that `weight_kg` is not null; this runs during transformation and quarantines failing rows with the reason 'missing weight'. For uniqueness, we check that `scan_id` is unique across the daily batch. This runs during the transform step, where duplicates are stripped and counted. On May 13th, this rule caught exactly 1,282 duplicate rows, and on normal days it caught between 3 and 9 duplicates.

For validity, we test that `scanned_at` is a valid timestamp, that `weight_kg` is a numeric value, that `weight_kg` falls within the plausible operating range between 0.01 kg and 100.0 kg, and that `scan_type` is an approved status. Non-ISO timestamps and comma decimals are repaired during transform, while weights outside the valid range are quarantined. On May 1st, the validity checks repaired 41 non-ISO timestamps, repaired 38 comma-decimal weights, and quarantined 12 impossible weights (such as a 858.29 kg parcel). The scan type check also repaired 591 mixed-case strings. For consistency, we verify that `hub_id` exists in the set of valid hubs (1 through 10) and that `service_code` exists in the approved catalog (`SDD`, `EXP`, `STD`, `ECO`). Unknown hubs are quarantined. On May 1st, this caught 10 rows with hub 99. All service codes were valid across all days.

In addition to row-level rules, the quality suite runs an automated schema drift check against `schema_baseline.json` before any transformation starts. It checks for two distinct situations: new columns and missing columns. If an unexpected extra column appears, the script logs a warning but allows processing to continue. If a required column is missing, the script halts immediately with an exit code of 1. On May 19th, the supplier renamed `weight_kg` to `weight`. The schema drift check caught this missing column immediately, aborted the task before any data was loaded into PostgreSQL, and triggered an alert to the data engineering on-call team.

---

## Section 4: Reconciliation

Reconciliation compares the count of rows received in each raw CSV file against the rows committed to the warehouse for every day of the three-week period. In data engineering, a difference between file rows and warehouse rows is not a failure; an unexplained difference is. Every single gap across the twenty-one days is fully accounted for by deduplicated rows and quarantined rows.

| Date | File Rows | Warehouse Rows | Difference | Explanation |
|:---:|:---:|:---:|:---:|:---|
| **2026-05-01** | 1,230 | 1,188 | 42 | 37 quarantined (hub 99, impossible/missing weight), 5 deduplicated |
| **2026-05-02** | 1,164 | 1,122 | 42 | 34 quarantined, 8 deduplicated |
| **2026-05-03** | 1,156 | 1,130 | 26 | 22 quarantined, 4 deduplicated |
| **2026-05-04** | 1,285 | 1,253 | 32 | 29 quarantined, 3 deduplicated |
| **2026-05-05** | 1,212 | 1,169 | 43 | 35 quarantined, 8 deduplicated |
| **2026-05-06** | 1,291 | 1,250 | 41 | 34 quarantined, 7 deduplicated |
| **2026-05-07** | 1,187 | 1,154 | 33 | 25 quarantined, 8 deduplicated |
| **2026-05-08** | 1,205 | 1,170 | 35 | 30 quarantined, 5 deduplicated |
| **2026-05-09** | 1,307 | 1,268 | 39 | 30 quarantined, 9 deduplicated |
| **2026-05-10** | 0 | 0 | 0 | No file arrived. FileSensor timed out after 60s; alert routed to supplier on-call. |
| **2026-05-11** | 1,242 | 1,203 | 39 | 34 quarantined, 5 deduplicated |
| **2026-05-12** | 1,103 | 1,070 | 33 | 30 quarantined, 3 deduplicated |
| **2026-05-13** | 2,546 | 1,234 | 1,312 | Fourth bad day. Double feed: 1,282 duplicates removed; 30 quarantined. |
| **2026-05-14** | 1,063 | 1,034 | 29 | 26 quarantined, 3 deduplicated |
| **2026-05-15** | 0 | 0 | 0 | Eid Holiday. File had 0 scan rows; branch operator skipped load cleanly. |
| **2026-05-16** | 1,123 | 1,085 | 38 | 34 quarantined, 4 deduplicated |
| **2026-05-17** | 1,340 | 1,305 | 35 | 30 quarantined, 5 deduplicated |
| **2026-05-18** | 1,167 | 1,134 | 33 | 30 quarantined, 3 deduplicated |
| **2026-05-19** | 1,066 | 0 | 1,066 | Contract breach. Supplier renamed weight_kg to weight; failed loudly before load. |
| **2026-05-20** | 1,132 | 1,086 | 46 | 38 quarantined, 8 deduplicated |
| **2026-05-21** | 1,222 | 1,196 | 26 | 23 quarantined, 3 deduplicated |

Across the entire three-week period, Waseet received 24,040 raw records. Of those records, exactly 21,051 clean scans were committed to PostgreSQL across the 18 active operating days, 1,399 duplicate records were stripped, 524 defective rows were safely quarantined, and 1,066 rows from May 19th were blocked before loading due to the contract breach. The arithmetic closes exactly across all twenty-one days with zero unexplained rows.

---

## Section 5: What I would do next

While our daily pipeline is reliable and idempotent, operating it in a production logistics business requires three concrete improvements.

First, what I would fix. Right now, quarantined rows are written to static CSV files inside the quarantine folder. Once rejected rows are written to disk, they sit there indefinitely until an engineer opens them. In production, I would replace static CSV files with a dedicated Dead-Letter Queue (DLQ) in PostgreSQL or Kafka, paired with an operational web portal for customer support. Operations agents could review parcels that failed validation, look up the missing package weight or correct the misspelled hub ID, and click a button to re-inject the corrected scan back into the warehouse. Furthermore, we must enforce our schema contract before files reach our storage bucket. We should give the supplier an automated ingestion API backed by a schema validator, so files with renamed columns (like May 19th) or duplicate files (like May 13th) are rejected at the gate rather than failing in the morning pipeline.

Second, what I would monitor. A critical concept taught in Lecture 15 is that a freshness check cannot live inside the daily DAG itself. If the daily DAG never runs because of an Airflow scheduler failure or an infrastructure crash, an internal freshness task will never execute to tell anyone. I would build an out-of-band heartbeat DAG running on an independent schedule every four hours. This heartbeat DAG queries the warehouse with `SELECT MAX(scanned_at) FROM parcel_scans` and triggers a high-priority PagerDuty alert if the latest data is more than 30 hours old. Additionally, I would add volume anomaly detection. Normal days bring roughly 1,200 scans. If a file arrives with more than 2,000 rows (like the duplicate batch on May 13th) or fewer than 500 rows, the system should send a warning to the team before transformation starts.

Third, what I would tell Waseet is still unsafe about this pipeline. Currently, `courier_id` is checked against the couriers dimension table, but the pipeline does not verify geographic boundaries. A courier registered to the Amman hub could theoretically scan a parcel in Dubai, and our pipeline would accept it without complaint. Even more dangerous, our pipeline treats each scan event as an isolated row without validating the physical lifecycle of a parcel. For example, a parcel could currently have a `DELIVERED` scan before a `PICKUP` scan, or a parcel could have two conflicting delivery scans at different hubs three minutes apart. Before Waseet uses this data warehouse to settle legal delivery disputes or calculate courier bonuses, we must implement state-machine validation to ensure that parcel scan journeys follow physical reality.
