"""
Airflow DAG: Scrape DoorDash listings.

Runs weekly (and can be triggered manually). For each configured market,
launches the DoorDash listing scraper, normalizes results, and loads
them into PostgreSQL.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure project root is on sys.path so imports work inside Airflow
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

logger = logging.getLogger(__name__)

# ── DAG Config ──────────────────────────────────────────────────────

default_args = {
    "owner": "scraper",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _scrape_doordash(market: str, **kwargs):
    """Task callable: run the DoorDash listing scraper for one market."""
    from processing.normalizer import normalize_listing
    from scrapers.doordash.listing import DoorDashListingScraper
    from storage.db import CrawlRun, MenuItemRow, PlatformMerchant, get_session

    session = get_session()
    crawl = CrawlRun(platform="doordash", market=market)
    session.add(crawl)
    session.commit()

    async def _run():
        async with DoorDashListingScraper(market=market, headless=True) as scraper:
            return await scraper.run()

    try:
        listings = asyncio.run(_run())

        for listing in listings:
            listing = normalize_listing(listing)

            pm = PlatformMerchant(
                platform=listing.platform,
                platform_id=listing.platform_merchant_id,
                name=listing.name,
                address=listing.address,
                cuisine_tags=listing.cuisine_tags,
                rating=listing.rating,
                review_count=listing.review_count,
                price_bucket=listing.price_bucket,
                delivery_fee=listing.delivery_fee,
                estimated_delivery_min=listing.estimated_delivery_min,
                is_promoted=listing.is_promoted,
                market=listing.market,
                raw_url=listing.raw_url,
            )

            # Set geography if coords available
            if listing.lat and listing.lng:
                pm.geom = f"SRID=4326;POINT({listing.lng} {listing.lat})"

            # Perform PostgreSQL Upsert
            from sqlalchemy.dialects.postgresql import insert

            stmt = insert(PlatformMerchant).values(
                platform=pm.platform,
                platform_id=pm.platform_id,
                name=pm.name,
                address=pm.address,
                geom=pm.geom,
                cuisine_tags=pm.cuisine_tags,
                rating=pm.rating,
                review_count=pm.review_count,
                price_bucket=pm.price_bucket,
                delivery_fee=pm.delivery_fee,
                estimated_delivery_min=pm.estimated_delivery_min,
                is_promoted=pm.is_promoted,
                market=pm.market,
                raw_url=pm.raw_url,
                scraped_at=datetime.utcnow(),
            )

            update_dict = {
                c.name: c for c in stmt.excluded if c.name not in ("id", "platform", "platform_id")
            }

            do_update_stmt = stmt.on_conflict_do_update(
                constraint="platform_merchants_platform_platform_id_key", set_=update_dict
            ).returning(PlatformMerchant.id)

            result = session.execute(do_update_stmt)
            result.scalar_one()
            session.flush()

            # Insert menu items
            for item in listing.menu_items:
                mi = MenuItemRow(
                    platform_merchant_id=pm.id,
                    name=item.name,
                    price=item.price,
                    description=item.description,
                    category=item.category,
                )
                session.add(mi)

        session.commit()

        crawl.status = "success"
        crawl.merchants_found = len(listings)
        crawl.finished_at = datetime.utcnow()

    except Exception as exc:
        crawl.status = "failed"
        crawl.error_detail = str(exc)[:500]
        crawl.finished_at = datetime.utcnow()
        session.commit()
        raise

    finally:
        session.commit()
        session.close()

    logger.info("[DAG] DoorDash/%s: %d merchants scraped.", market, len(listings))


# ── DAG Definition ──────────────────────────────────────────────────

with DAG(
    dag_id="scrape_doordash",
    default_args=default_args,
    description="Scrape DoorDash restaurant listings per market",
    schedule_interval="@weekly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["scraping", "doordash"],
) as dag:
    import yaml

    config_path = Path(__file__).resolve().parents[1] / "config" / "markets.yaml"
    with open(config_path) as f:
        markets = yaml.safe_load(f)["markets"]

    for market_config in markets:
        slug = market_config["slug"]

        PythonOperator(
            task_id=f"scrape_doordash_{slug}",
            python_callable=_scrape_doordash,
            op_kwargs={"market": slug},
        )
