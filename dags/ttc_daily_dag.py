"""Airflow DAG: refresh the TTC delay warehouse every day.

Runs the same three CLI steps as the local pipeline, so the DAG stays a thin scheduler.
(Not required to run the project; shown to demonstrate orchestration.)
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {"retries": 2, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="ttc_delay_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["portfolio", "toronto-open-data"],
) as dag:
    ingest = BashOperator(task_id="ingest_bronze", bash_command="python -m ttc_pipeline.cli ingest")
    build = BashOperator(task_id="build_silver_gold", bash_command="python -m ttc_pipeline.cli build")
    check = BashOperator(task_id="data_quality_checks", bash_command="python -m ttc_pipeline.cli check")
    ingest >> build >> check
