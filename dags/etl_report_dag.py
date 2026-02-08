from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "etl",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="etl_report",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["etl", "report"],
) as dag:
    load_task = BashOperator(
        task_id="load",
        bash_command=(
            "python /path/to/etl_project/loading/load_pipeline.py "
            "--source file "
        ),
    )

    report_task = BashOperator(
        task_id="report",
        bash_command=(
            "python /path/to/etl_project/reporting/report_job.py "
        ),
    )

    load_task >> report_task
