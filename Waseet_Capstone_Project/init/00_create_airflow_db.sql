-- Airflow keeps its own bookkeeping - dag runs, task instances, connections - in
-- a database of its own. Same server, different database, so that a `DROP TABLE`
-- aimed at the warehouse can never touch the scheduler's state.
CREATE DATABASE airflow;
