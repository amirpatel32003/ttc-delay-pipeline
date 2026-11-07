"""Airflow DAG: refresh the TTC delay warehouse every day.

The DAG is a thin scheduler over the same CLI steps used locally: ingest (incremental, unchanged files are
skipped), build silver/gold, then run the data-quality checks (a failing check fails the run).
Set TTC_PROJECT_DIR if the repository is not the parent folder of this dags/ directory.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG

try:  # Airflow 3 moved the bash operator into the standard provider
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.bash import BashOperator

PROJECT_DIR = os.environ.get("TTC_PROJECT_DIR", str(Path(__file__).resolve().parents[1]))
CLI = f'cd "{PROJECT_DIR}" && PYTHONPATH=src python -m ttc_pipeline.cli'

default_args = {"owner": "amir", "retries": 2, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="ttc_delay_pipeline",
    description="Incremental TTC subway delay ingest, silver/gold build and data-quality gate",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["portfolio", "toronto-open-data"],
) as dag:
    ingest = BashOperator(task_id="ingest_bronze", bash_command=f"{CLI} ingest")
    build = BashOperator(task_id="build_silver_gold", bash_command=f"{CLI} build")
    check = BashOperator(task_id="data_quality_checks", bash_command=f"{CLI} check")
    ingest >> build >> check
