"""
Grubhub detail scraper.
"""

from __future__ import annotations

import logging

from processing.models import MerchantListing
from processing.parser import parse_menu_items_from_html
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


class GrubhubDetailScraper(BaseScraper):
    PLATFORM = "grubhub"

    async def scrape_listings(self) -> list[MerchantListing]:
        return []

    async def scrape_detail(self, listing: MerchantListing) -> MerchantListing:
        """Navigate to a Grubhub store page and extract menu data."""
        url = listing.raw_url
        if not url:
            url = f"https://www.grubhub.com/restaurant/{listing.platform_merchant_id}/"

        logger.info("[Grubhub] Scraping detail for '%s': %s", listing.name, url)

        try:
            html = await self._fetch_with_retry(url)
            self._archive_html(listing.platform_merchant_id, html)

            menu_items = parse_menu_items_from_html(html)
            if menu_items:
                listing.menu_items = menu_items
                logger.info(
                    "[Grubhub] Extracted %d menu items for '%s'",
                    len(menu_items), listing.name,
                )

        except Exception as exc:
            logger.error(
                "[Grubhub] Detail scrape failed for '%s' (%s): %s",
                listing.name, listing.platform_merchant_id, exc,
            )

        return listing
