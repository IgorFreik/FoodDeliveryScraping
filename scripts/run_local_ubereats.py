import asyncio
import json
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(PROJECT_ROOT / ".env")
sys.path.insert(0, str(PROJECT_ROOT))

from processing.normalizer import normalize_listing
from scrapers.base import BaseScraper
from scrapers.ubereats.listing import UberEatsListingScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

STORAGE_DIR = PROJECT_ROOT / "local_storage"
STORAGE_DIR.mkdir(exist_ok=True)

def patch_scraper(cls: type[BaseScraper]):
    class LocalScraper(cls):
        def _archive_html(self, merchant_id: str, html: str) -> str:
            filename = STORAGE_DIR / f"{self.PLATFORM}_{self.market}_{merchant_id}.html"
            with open(filename, "w", encoding="utf-8") as f:
                f.write(html)
            return str(filename)
    return LocalScraper

async def main():
    print("Starting Amsterdam scraping for UberEats...")

    LocalScraper = patch_scraper(UberEatsListingScraper)
    results = []

    try:
        async with LocalScraper(market="amsterdam", headless=False) as scraper:
            print("Scraping ubereats listings...")
            listings = await scraper.scrape_listings()
            print(f"Found {len(listings)} listings in amsterdam.")

            # Enrich with a few details for verification
            subset = listings[:10]
            for i, listing in enumerate(subset):
                print(f"[{i+1}/{len(subset)}] Scraping detail for {listing.name}...")
                await scraper.scrape_detail(listing)
                normalized = normalize_listing(listing)
                results.append(normalized.model_dump())
    except Exception as e:
        logger.error(f"Failed to run UberEats scraper: {e}")
    finally:
        output_file = STORAGE_DIR / "scraped_results_ubereats_amsterdam.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\n✅ Scraping complete. {len(results)} listings saved to {output_file}")

if __name__ == "__main__":
    asyncio.run(main())
