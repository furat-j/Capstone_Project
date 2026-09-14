"""The daily scan DAG.

A skeleton with the imports you will need and the shape of the run. Fill in the
tasks, wire the dependencies, and delete the comments as you replace them.

Inside the container the paths are:

    /opt/airflow/data       the scan files and lookups
    /opt/airflow/pipeline   pipeline.py and quality.py

Iterate with `dags test`, which runs the whole DAG in one process and prints
straight to your terminal - no unpausing, no waiting for the scheduler:

    docker exec waseet-airflow airflow dags test waseet_daily 2026-05-04
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.sensors.filesystem import FileSensor
import pendulum

DATA = "/opt/airflow/data"
CODE = "/opt/airflow/pipeline"

# TODO. Which failures here deserve a retry, and how many? A supplier that is
# late is not a transient failure, and a renamed column is not one either.
def alert(context):
    """Fires after retries are exhausted.

    Write a line an on-call engineer can route from. Which task failed matters
    more than that something did: the sensor timing out is the supplier's
    problem, the load failing is yours, and the two wake up different people.
    """
    ti = context.get("task_instance")
    task_id = ti.task_id if ti else "unknown"
    ds = context.get("ds", "unknown_date")

    if task_id == "wait_for_file":
        msg = f"[SUPPLIER-ONCALL] Supplier feed scans_{ds}.csv missing or delayed. FileSensor timed out on {ds}."
    elif task_id == "run_pipeline":
        msg = f"[DE-INTERNAL-ONCALL] Pipeline execution failed for date {ds} on task {task_id}. Inspect pipeline.log."
    elif task_id == "run_quality":
        msg = f"[DE-INTERNAL-ONCALL] Data quality suite or reconciliation failed for date {ds}."
    else:
        msg = f"[DE-INTERNAL-ONCALL] Task {task_id} failed on date {ds}."

    print("=================== ALERT ===================")
    print(msg)
    print("=============================================")
    try:
        import os
        os.makedirs("/opt/airflow/logs", exist_ok=True)
        with open("/opt/airflow/logs/alerts.txt", "a", encoding="utf-8") as f:
            f.write(f"{ds} {task_id}: {msg}\n")
    except Exception:
        pass


def choose_branch(ds):
    """Return the task_id to run next.

    The 15th of May is Eid. The network is closed, the file arrives with a
    header and nothing under it, and failing that morning would be an alert on a
    day when nothing is wrong. Everything below the branch that is not chosen
    goes to `skipped`, which is why the task that joins the two paths back
    together needs a trigger rule that is not the default.
    """
    import pandas as pd
    path = f"{DATA}/scans_{ds}.csv"
    try:
        df = pd.read_csv(path, dtype=str)
        if len(df) == 0:
            print(f"Date {ds} has 0 rows (Eid/holiday). Routing to skip_day.")
            return "skip_day"
        return "run_pipeline"
    except Exception:
        return "skip_day"


# Permanent upstream failures (missing file, renamed column) do not benefit
# from retries. Fail fast so alerts are sent immediately to on-call.
default_args = {
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert,
}


with DAG(
    dag_id="waseet_daily",
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["waseet"],
) as dag:

    # FileSensor waits for daily file using mode="reschedule" to free worker slots.
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=DATA + "/scans_{{ ds }}.csv",
        fs_conn_id="fs_default",
        poke_interval=10,
        timeout=60,
        mode="reschedule",
    )

    choose = BranchPythonOperator(
        task_id="choose",
        python_callable=choose_branch,
    )

    run_pipeline = BashOperator(
        task_id="run_pipeline",
        bash_command=f"cd {CODE} && python pipeline.py --date {{{{ ds }}}}",
    )

    skip_day = BashOperator(
        task_id="skip_day",
        bash_command='echo "No scan rows for {{ ds }} (holiday); skipping pipeline load."',
    )

    run_quality = BashOperator(
        task_id="run_quality",
        bash_command=f"cd {CODE} && python quality.py --date {{{{ ds }}}}",
    )

    # Join task: trigger_rule="none_failed_min_one_success" ensures execution
    # when one of the branch paths is skipped. Default "all_success" would stall.
    finish = BashOperator(
        task_id="finish",
        bash_command='echo "Run for {{ ds }} finished."',
        trigger_rule="none_failed_min_one_success",
    )

    # Dependency wiring
    wait_for_file >> choose
    choose >> run_pipeline >> run_quality >> finish
    choose >> skip_day >> finish

