"""
JustEatTakeaway listing scraper.

Navigates to Thuisbezorgd/JET's restaurant listing pages for a given market and
extracts merchant stubs using the parser module.
"""

from __future__ import annotations

import logging
import yaml
from pathlib import Path

from scrapers.base import BaseScraper
from scrapers.utils.stealth import human_like_scroll
from processing.models import MerchantListing
from processing.parser import parse_listing, parse_detail

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


class JustEatTakeawayListingScraper(BaseScraper):
    PLATFORM = "justeattakeaway"

    def __init__(self, market: str, **kwargs):
        # Load platform config
        with open(CONFIG_DIR / "platforms.yaml") as f:
            platforms = yaml.safe_load(f)["platforms"]
        self._config = platforms["justeattakeaway"]

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
        """Build the JET listing URL for the current market."""
        # Amsterdam: use the postcode-specific URL which surfaces far more results
        if self.market == "amsterdam":
            return "https://www.thuisbezorgd.nl/en/delivery/food/amsterdam-1072"
        pattern = self._config["listing_url_pattern"]
        slug = self.market.replace("_", "-")
        return pattern.format(slug=slug)

    async def scrape_listings(self) -> list[MerchantListing]:
        """
        Scrape real JET listing pages.
        """
        url = self._build_listing_url()
        logger.info("[JustEat] Scraping listings for market '%s': %s", self.market, url)

        all_listings: list[MerchantListing] = []

        try:
            page = await self._context.new_page()
            try:
                await self._rate_limit()

                # Accept cookies if the dialog pops up
                async def accept_cookies(response):
                    if "consent" in response.url.lower():
                        pass

                page.on("response", accept_cookies)

                await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                await page.wait_for_timeout(3000)

                # Click OK on cookie consent if it appears
                try:
                    cookie_btn = await page.query_selector(
                        "button[data-qa='cookie-consent-accept-all']"
                    )
                    if cookie_btn:
                        await cookie_btn.click()
                        await page.wait_for_timeout(1000)
                except Exception:
                    pass

                # Scroll down a few times to load more restaurants
                previous_count = 0
                for scroll_round in range(5):
                    await human_like_scroll(
                        page, scroll_count=3
                    )  # FIXME: CHANGE TO 40 AFTER TESTING
                    await page.wait_for_timeout(2000)

                    html = await page.content()
                    listings = parse_listing(self.PLATFORM, html, self.market)
                    current_count = len(listings)

                    logger.info(
                        "[JustEat] Scroll round %d: %d merchants found",
                        scroll_round + 1,
                        current_count,
                    )

                    if current_count == previous_count:
                        break  # No more merchants loading
                    previous_count = current_count

                # Final parse
                html = await page.content()

                # Archive raw HTML
                self._archive_html(f"listing_{self.market}", html)

                all_listings = parse_listing(self.PLATFORM, html, self.market)
            finally:
                await page.close()

        except Exception as exc:
            logger.error("[JustEat] Listing scrape failed for %s: %s", self.market, exc)

        logger.info(
            "[JustEat/%s] Total listings extracted: %d", self.market, len(all_listings)
        )
        return all_listings

    async def scrape_detail(self, listing: MerchantListing) -> MerchantListing:
        """Scrape individual merchant detail page for extra info like address."""
        if not listing.raw_url:
            return listing

        try:
            page = await self._context.new_page()
            try:
                await self._rate_limit()
                await page.goto(listing.raw_url, wait_until="networkidle", timeout=30_000)
                await page.wait_for_timeout(2000)

                # Wait for potential lazy content
                await page.evaluate("window.scrollTo(0, 500)")
                await page.wait_for_timeout(1000)

                html = await page.content()
                detail_data = parse_detail(self.PLATFORM, html, self.market)

                if detail_data.get("address"):
                    listing.address = detail_data["address"]
                if detail_data.get("lat"):
                    listing.lat = detail_data["lat"]
                if detail_data.get("lng"):
                    listing.lng = detail_data["lng"]

                logger.info(
                    "[JustEat] Detail scraped for %s: %s", listing.name, listing.address
                )
            finally:
                await page.close()
        except Exception as e:
            logger.warning(
                "[JustEat] Failed to scrape detail for %s: %s", listing.name, e
            )

        return listing
