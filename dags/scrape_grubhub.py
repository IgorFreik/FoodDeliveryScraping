"""
Airflow DAG: Scrape Grubhub listings.

Mirrors the DoorDash DAG structure — one task per market.
"""

from __future__ import annotations

import asyncio
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
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _scrape_grubhub(market: str, **kwargs):
    """Task callable: run the Grubhub listing scraper for one market."""
    from processing.normalizer import normalize_listing
    from scrapers.grubhub.listing import GrubhubListingScraper
    from storage.db import CrawlRun, MenuItemRow, PlatformMerchant, get_session

    session = get_session()
    crawl = CrawlRun(platform="grubhub", market=market)
    session.add(crawl)
    session.commit()

    async def _run():
        async with GrubhubListingScraper(market=market, headless=True) as scraper:
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

            if listing.lat and listing.lng:
                pm.geom = f"SRID=4326;POINT({listing.lng} {listing.lat})"

            session.merge(pm)

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

    logger.info("[DAG] Grubhub/%s: %d merchants scraped.", market, len(listings))


with DAG(
    dag_id="scrape_grubhub",
    default_args=default_args,
    description="Scrape Grubhub restaurant listings per market",
    schedule_interval="@weekly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["scraping", "grubhub"],
) as dag:
    import yaml

    config_path = Path(__file__).resolve().parents[1] / "config" / "markets.yaml"
    with open(config_path) as f:
        markets = yaml.safe_load(f)["markets"]

    for market_config in markets:
        slug = market_config["slug"]

        PythonOperator(
            task_id=f"scrape_grubhub_{slug}",
            python_callable=_scrape_grubhub,
            op_kwargs={"market": slug},
        )
