"""
DoorDash listing scraper.

Navigates to DoorDash's restaurant listing pages for a given market and
extracts merchant stubs using the parser module.
"""

from __future__ import annotations

import logging
import yaml
from pathlib import Path

from scrapers.base import BaseScraper
from scrapers.utils.stealth import human_like_scroll
from processing.models import MerchantListing
from processing.parser import parse_doordash_listing

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


class DoorDashListingScraper(BaseScraper):
    PLATFORM = "doordash"

    def __init__(self, market: str, **kwargs):
        # Load platform config
        with open(CONFIG_DIR / "platforms.yaml") as f:
            platforms = yaml.safe_load(f)["platforms"]
        self._config = platforms["doordash"]

        super().__init__(
            market,
            requests_per_second=self._config["rate_limit"]["requests_per_second"],
            delay_range_ms=tuple(self._config["rate_limit"]["delay_range_ms"]),
            **kwargs,
        )

        # Load market zips
        with open(CONFIG_DIR / "markets.yaml") as f:
            markets = yaml.safe_load(f)["markets"]
        self._market_info = next((m for m in markets if m["slug"] == market), None)

    def _build_listing_url(self) -> str:
        """Build the DoorDash listing URL for the current market."""
        pattern = self._config["listing_url_pattern"]
        slug = self.market.replace("_", "-")
        return pattern.format(slug=slug)

    async def scrape_listings(self) -> list[MerchantListing]:
        """
        Scrape real DoorDash listing pages. Scrolls to load more merchants
        (infinite scroll pattern).
        """
        url = self._build_listing_url()
        logger.info("[DoorDash] Scraping listings for market '%s': %s", self.market, url)

        all_listings: list[MerchantListing] = []

        try:
            page = await self._context.new_page()
            try:
                await self._rate_limit()
                await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                await page.wait_for_timeout(3000)

                # Scroll down a few times to load more restaurants
                previous_count = 0
                for scroll_round in range(5):
                    await human_like_scroll(page, scroll_count=3)
                    await page.wait_for_timeout(2000)

                    html = await page.content()
                    listings = parse_doordash_listing(html, self.market)
                    current_count = len(listings)

                    logger.info(
                        "[DoorDash] Scroll round %d: %d merchants found",
                        scroll_round + 1, current_count,
                    )

                    if current_count == previous_count:
                        break  # No more merchants loading
                    previous_count = current_count

                # Final parse
                html = await page.content()

                # Archive raw HTML
                self._archive_html(f"listing_{self.market}", html)

                all_listings = parse_doordash_listing(html, self.market)
            finally:
                await page.close()

        except Exception as exc:
            logger.error("[DoorDash] Listing scrape failed for %s: %s", self.market, exc)

        logger.info("[DoorDash/%s] Total listings extracted: %d", self.market, len(all_listings))
        return all_listings

    async def scrape_detail(self, listing: MerchantListing) -> MerchantListing:
        """
        Scrape individual merchant detail page for menu and extra info.
        """
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
                "[DoorDash] Detail scrape failed for %s: %s",
                listing.name, exc,
            )

        return listing
