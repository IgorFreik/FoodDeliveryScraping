"""
Airflow DAG: Cross-platform entity resolution.

Reads all platform_merchants from PostgreSQL, runs the entity resolution
pipeline (rule-based + optional Splink), and writes match results back.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

logger = logging.getLogger(__name__)

default_args = {
    "owner": "scraper",
    "depends_on_past": False,
    # "retries": 1, REVERT AFTER TESTING
    # "retry_delay": timedelta(minutes=1), REVERT AFTER TESTING
}


def _resolve_entities(**kwargs):
    """Airflow task: run the entity resolution pipeline."""
    from processing.entity_resolution import run_resolve_entities

    run_resolve_entities()


with DAG(
    dag_id="resolve_entities",
    default_args=default_args,
    description="Cross-platform merchant entity resolution",
    schedule_interval="@weekly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["processing", "entity-resolution"],
) as dag:

    PythonOperator(
        task_id="resolve_entities",
        python_callable=_resolve_entities,
    )
