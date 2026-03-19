"""
Grubhub listing scraper.

Navigates to Grubhub's restaurant listing pages for a given market and
extracts merchant stubs using the parser module.
"""

from __future__ import annotations

import logging
from pathlib import Path

import yaml

from processing.models import MerchantListing
from processing.parser import parse_grubhub_listing
from scrapers.base import BaseScraper
from scrapers.utils.stealth import human_like_scroll

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


class GrubhubListingScraper(BaseScraper):
    PLATFORM = "grubhub"

    def __init__(self, market: str, **kwargs):
        with open(CONFIG_DIR / "platforms.yaml") as f:
            platforms = yaml.safe_load(f)["platforms"]
        self._config = platforms["grubhub"]

        super().__init__(
            market,
            requests_per_second=self._config["rate_limit"]["requests_per_second"],
            delay_range_ms=tuple(self._config["rate_limit"]["delay_range_ms"]),
            **kwargs,
        )

    def _build_listing_url(self) -> str:
        pattern = self._config["listing_url_pattern"]
        slug = self.market.replace("_", "-")
        return pattern.format(slug=slug)

    async def scrape_listings(self) -> list[MerchantListing]:
        """Scrape Grubhub listing pages with pagination / scroll."""
        url = self._build_listing_url()
        logger.info("[Grubhub] Scraping listings for market '%s': %s", self.market, url)

        all_listings: list[MerchantListing] = []

        try:
            page = await self._context.new_page()
            try:
                await self._rate_limit()
                await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                await page.wait_for_timeout(3000)

                # Scroll to trigger lazy loading
                previous_count = 0
                for scroll_round in range(5):
                    await human_like_scroll(page, scroll_count=3)
                    await page.wait_for_timeout(2000)

                    html = await page.content()
                    listings = parse_grubhub_listing(html, self.market)
                    current_count = len(listings)

                    logger.info(
                        "[Grubhub] Scroll round %d: %d merchants found",
                        scroll_round + 1,
                        current_count,
                    )

                    if current_count == previous_count:
                        break
                    previous_count = current_count

                html = await page.content()
                self._archive_html(f"listing_{self.market}", html)
                all_listings = parse_grubhub_listing(html, self.market)
            finally:
                await page.close()

        except Exception as exc:
            logger.error("[Grubhub] Listing scrape failed for %s: %s", self.market, exc)

        logger.info("[Grubhub/%s] Total listings extracted: %d", self.market, len(all_listings))
        return all_listings

    async def scrape_detail(self, listing: MerchantListing) -> MerchantListing:
        """Scrape individual Grubhub merchant detail page for menu."""
        if not listing.raw_url:
            return listing

        try:
            html = await self._fetch_with_retry(listing.raw_url)
            self._archive_html(listing.platform_merchant_id, html)

            from processing.parser import parse_menu_items_from_html

            menu_items = parse_menu_items_from_html(html)
            listing.menu_items = menu_items

        except Exception as exc:
            logger.warning(
                "[Grubhub] Detail scrape failed for %s: %s",
                listing.name,
                exc,
            )

        return listing
