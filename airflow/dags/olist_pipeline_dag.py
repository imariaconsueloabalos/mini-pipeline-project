from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# --- CONFIGURATION ---
DBT_BIN = "/home/airflow/.local/bin/dbt"
DBT_PROJECT_DIR = "/opt/airflow/dbt"
INGEST_SCRIPT = "/opt/airflow/scripts/ingest_olist.py"

# Bypassing the Windows volume for all dbt-generated files (logs and compiled code)
LOG_FLAGS = "--log-path /tmp/dbt/logs --target-path /tmp/dbt/target"
GLOBAL_FLAGS = f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} {LOG_FLAGS}"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="olist_final_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 2, 25),
    schedule_interval="@daily",  # DELIVERABLE: Set a schedule_interval
    catchup=False,
    tags=["olist", "final_delivery"],
) as dag:

    # 1. DELIVERABLE: Data ingestion tasks
    ingest_task = BashOperator(
        task_id="run_ingestion",
        bash_command=f"python {INGEST_SCRIPT}"
    )

    # 2. DELIVERABLE: BashOperator task to run dbt seed
    # Used for loading CSVs in /seeds/ folder to the database
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"{DBT_BIN} seed {GLOBAL_FLAGS}"
    )

    # 3. DELIVERABLE: BashOperator task to run dbt run
    # This builds staging -> dim -> fact in the order defined by your SQL refs
    # Updated to include --full-refresh for a clean update every time
    dbt_run = BashOperator(
        task_id="dbt_run_all_models",
        bash_command=f"{DBT_BIN} run --full-refresh {GLOBAL_FLAGS}"
    )

    # 4. DELIVERABLE: BashOperator task to run dbt test
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_BIN} test {GLOBAL_FLAGS}"
    )

    # 5. DELIVERABLE: Define clear dependencies between tasks
    ingest_task >> dbt_seed >> dbt_run >> dbt_test