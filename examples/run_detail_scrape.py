#!/usr/bin/env python3
"""
Integration example: Test Uber Eats detail page scraping (live).

Requires network access and Playwright. Run from project root:
    python examples/run_detail_scrape.py
"""
import asyncio
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from processing.models import MerchantListing
from scrapers.ubereats.listing import UberEatsListingScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)


async def main():
    print("Testing UberEats detail scraping...")
    async with UberEatsListingScraper(market="amsterdam", headless=True) as scraper:
        url = "https://www.ubereats.com/nl-en/store/mcdonalds-kinkerstraat/c2jx4wWSS0u5wQHpmayIMg"
        listing = MerchantListing(
            platform="ubereats",
            platform_merchant_id="c2jx4wWSS0u5wQHpmayIMg",
            name="McDonald's Kinkerstraat",
            market="amsterdam",
            raw_url=url,
        )
        print(f"Scraping {listing.name}...")
        res = await scraper.scrape_detail(listing)
        print(f"Result: Address='{res.address}'")


if __name__ == "__main__":
    asyncio.run(main())
