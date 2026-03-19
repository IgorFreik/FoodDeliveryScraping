"""
Airflow DAG: Daily snapshot of merchant counts per platform.

Runs daily at 23:00. Inserts current counts into platform_merchant_daily_counts
for week-over-week delta display in Grafana.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import text

# Ensure project root is on sys.path so imports work inside Airflow
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

logger = logging.getLogger(__name__)

default_args = {
    "owner": "scraper",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _snapshot_merchant_counts(**kwargs):
    """Task callable: insert current merchant counts into daily snapshot table."""
    from storage.db import get_session

    session = get_session()
    try:
        session.execute(
            text("""
                INSERT INTO platform_merchant_daily_counts (snapshot_date, platform, count)
                SELECT CURRENT_DATE, platform, COUNT(DISTINCT platform_id)
                FROM platform_merchants
                GROUP BY platform
                ON CONFLICT (snapshot_date, platform) DO UPDATE SET count = EXCLUDED.count
            """)
        )
        session.commit()
        logger.info("[DAG] Snapshot merchant counts completed for %s", datetime.utcnow().date())
    finally:
        session.close()


with DAG(
    dag_id="snapshot_merchant_counts",
    default_args=default_args,
    description="Daily snapshot of merchant counts per platform for Grafana deltas",
    schedule_interval="0 23 * * *",  # Daily at 23:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["analytics", "snapshot"],
) as dag:
    PythonOperator(
        task_id="snapshot_counts",
        python_callable=_snapshot_merchant_counts,
    )
