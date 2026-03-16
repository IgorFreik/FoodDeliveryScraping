"""
Airflow DAG: Cross-platform entity resolution.

Reads all platform_merchants from PostgreSQL, runs the entity resolution
pipeline (rule-based + optional Splink), and writes match results back.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

logger = logging.getLogger(__name__)

default_args = {
    "owner": "scraper",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def _resolve_entities(**kwargs):
    """
    Load all platform merchants, run entity resolution, and persist
    matched entity groups to the merchants + merchant_matches tables.
    """
    import pandas as pd
    from sqlalchemy import text

    from storage.db import get_session, engine, Merchant, MerchantMatch
    from processing.entity_resolution import (
        rule_based_match,
        merge_matches_to_entities,
    )

    session = get_session()

    # Load platform merchants into a DataFrame
    query = text("""
        SELECT
            id,
            platform,
            name,
            address,
            ST_Y(geom::geometry) AS lat,
            ST_X(geom::geometry) AS lng,
            market
        FROM platform_merchants
        WHERE name IS NOT NULL
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    if df.empty:
        logger.info("[DAG] No merchants to resolve.")
        return

    logger.info("[DAG] Running entity resolution on %d merchants...", len(df))

    # Step 1: Rule-based matching
    matches = rule_based_match(df)
    logger.info("[DAG] Rule-based matching produced %d candidate pairs.", len(matches))

    # Step 2: Cluster into entities
    clusters = merge_matches_to_entities(matches)
    logger.info("[DAG] Merged into %d entity clusters.", len(clusters))

    # Step 3: Create/update merchant entities and match records
    match_lookup = {
        (m.platform_merchant_id_a, m.platform_merchant_id_b): m
        for m in matches
    }

    for entity_root, member_ids in clusters.items():
        # Pick canonical name from the member with most reviews
        members = df[df["id"].isin(member_ids)]
        canonical_row = members.iloc[0]  # simplistic: take first

        merchant = Merchant(
            canonical_name=canonical_row["name"],
            canonical_addr=canonical_row.get("address", ""),
        )
        if canonical_row.get("lat") and canonical_row.get("lng"):
            merchant.canonical_geom = (
                f"SRID=4326;POINT({canonical_row['lng']} {canonical_row['lat']})"
            )

        session.add(merchant)
        session.flush()  # Get the merchant.id

        for pm_id in member_ids:
            # Find confidence from the match pairs
            confidence = 1.0
            method = "cluster_member"
            for pair_key in [(pm_id, entity_root), (entity_root, pm_id)]:
                if pair_key in match_lookup:
                    confidence = match_lookup[pair_key].confidence
                    method = match_lookup[pair_key].match_method
                    break

            mm = MerchantMatch(
                platform_merchant_id=pm_id,
                merchant_id=merchant.id,
                confidence=confidence,
                match_method=method,
            )
            session.merge(mm)

    session.commit()
    session.close()

    logger.info(
        "[DAG] Entity resolution complete: %d entities, %d matches.",
        len(clusters),
        sum(len(v) for v in clusters.values()),
    )


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
