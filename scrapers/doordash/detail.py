"""
DoorDash detail scraper.

Given a merchant stub (from the listing scraper), navigates to the
merchant's detail page and extracts the full menu, delivery info, and
additional metadata.
"""

from __future__ import annotations

import logging

from scrapers.base import BaseScraper
from processing.models import MerchantListing, MenuItem
from processing.parser import parse_menu_items_from_html

logger = logging.getLogger(__name__)


class DoorDashDetailScraper(BaseScraper):
    PLATFORM = "doordash"

    async def scrape_listings(self) -> list[MerchantListing]:
        """Not used for the detail scraper — operates on pre-existing stubs."""
        return []

    async def scrape_detail(self, listing: MerchantListing) -> MerchantListing:
        """
        Navigate to a single DoorDash store page and extract:
        - Full menu (items + prices + categories)
        - Delivery fee, ETA
        - Rating, review count

        Updates the listing in-place and returns it.
        """
        url = listing.raw_url
        if not url:
            url = f"https://www.doordash.com/store/{listing.platform_merchant_id}/"

        logger.info("[DoorDash] Scraping detail for '%s': %s", listing.name, url)

        try:
            html = await self._fetch_with_retry(url)
            self._archive_html(listing.platform_merchant_id, html)

            # Parse menu
            menu_items = parse_menu_items_from_html(html)
            if menu_items:
                listing.menu_items = menu_items
                logger.info(
                    "[DoorDash] Extracted %d menu items for '%s'",
                    len(menu_items), listing.name,
                )

        except Exception as exc:
            logger.error(
                "[DoorDash] Detail scrape failed for '%s' (%s): %s",
                listing.name, listing.platform_merchant_id, exc,
            )

        return listing
