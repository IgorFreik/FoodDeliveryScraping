#!/usr/bin/env python3
"""
Integration example: Verify address extraction in listing phase.

Requires network access and Playwright. Run from project root:
    python examples/run_optimized_extraction.py
"""
import asyncio
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from scrapers.ubereats.listing import UberEatsListingScraper  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)


async def main():
    print("\n--- Testing Optimized Uber Eats Extraction ---\n")
    async with UberEatsListingScraper(market="amsterdam", headless=True) as scraper:
        print("[Step 1] Scraping listings (address should be extracted here)...")
        listings = await scraper.scrape_listings()
        if not listings:
            print("No listings found. Check connection/market.")
            return
        print(f"\n[Step 2] Found {len(listings)} listings.")
        sample = listings[0]
        print(f"\n[Sample] Name: {sample.name}")
        print(f"[Sample] Address: {sample.address}")
        print(f"[Sample] Lat/Lng: {sample.lat}, {sample.lng}")
        if sample.address:
            print("\n✅ SUCCESS: Address found in listing phase!")
        else:
            print("\n❌ FAILURE: Address missing in listing phase.")


if __name__ == "__main__":
    asyncio.run(main())
