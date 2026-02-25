from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="olist_ingestion",
    default_args=default_args,
    start_date=datetime(2026, 2, 25),
    schedule_interval="@daily",
    catchup=False,
    tags=["olist", "ecommerce"],
) as dag:

    ingest_task = BashOperator(
        task_id="run_ingest_olist",
        bash_command="python /opt/airflow/scripts/ingest_olist.py"
    )

    ingest_task