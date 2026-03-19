"""
Uber Eats listing scraper.

Navigates to Uber Eats city pages and extracts merchant stubs.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import yaml

from processing.models import MerchantListing
from processing.parser import parse_detail, parse_listing
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


class UberEatsListingScraper(BaseScraper):
    PLATFORM = "ubereats"

    def __init__(self, market: str, **kwargs):
        # Load platform config
        with open(CONFIG_DIR / "platforms.yaml") as f:
            platforms = yaml.safe_load(f)["platforms"]
        self._config = platforms["ubereats"]

        # Load market config for ZIP codes/coordinates
        with open(CONFIG_DIR / "markets.yaml") as f:
            markets_data = yaml.safe_load(f)["markets"]
            self._market_config = next(
                (m for m in markets_data if m["slug"] == market), {}
            )

        super().__init__(
            market,
            requests_per_second=self._config["rate_limit"]["requests_per_second"],
            delay_range_ms=tuple(self._config["rate_limit"]["delay_range_ms"]),
            **kwargs,
        )

    def _build_listing_url(self) -> str:
        pattern = self._config["listing_url_pattern"]
        if self.market == "amsterdam":
            # Include the pl (place) param — required for pagination to work
            return (
                "https://www.ubereats.com/nl-en/city/amsterdam-noord-holland"
                "?pl=JTdCJTIyYWRkcmVzcyUyMiUzQSUyMkFtc3RlcmRhbSUyMiUyQyUyMnJlZmVyZ"
                "W5jZSUyMiUzQSUyMkNoSUpWWGVhbExVX3hrY1JqYV9BdDB6OUFHWSUyMiUyQyUyMn"
                "JlZmVyZW5jZVR5cGUlMjIlM0ElMjJnb29nbGVfcGxhY2VzJTIyJTJDJTIybGF0aXR"
                "1ZGUlMjIlM0E1Mi4zNjc1NzM0JTJDJTIybG9uZ2l0dWRlJTIyJTNBNC45MDQxMzg4"
                "OTk5OTk5OTk1JTdE"
                "&page=1"
            )
        slug_map = {"nyc": "new-york-city"}
        slug = slug_map.get(self.market, self.market)
        return pattern.format(slug=slug)

    async def _handle_prompts(self, page) -> None:
        """Handle cookie banners and address prompts."""
        # 1. Handle Cookie Banner
        try:
            for btn in [
                "button:has-text('Accept')",
                "button[aria-label='Accept']",
                "button#cookie-banner-accept",
                "#privacy-cookie-banners-root button:last-child",
            ]:
                if await page.is_visible(btn):
                    logger.info("[UberEats] Dismissing cookie banner...")
                    await page.click(btn, timeout=2000)
                    await page.wait_for_timeout(1000)
                    break
        except Exception:
            pass

        # 2. Handle Address Prompt
        html = await page.content()
        if "Nothing to eat here" not in html and "address" not in html.lower():
            return

        logger.info("[UberEats] Address prompt detected, setting location...")
        zip_code = self._market_config.get("zips", [""])[0]
        if not zip_code:
            return

        try:
            for sel in [
                "a[aria-label^='Deliver to']",
                "a[aria-label='Enter delivery address']",
                "button[data-testid='delivery-address-label']",
                "button:has-text('address')",
            ]:
                try:
                    if await page.is_visible(sel):
                        await page.click(sel, timeout=2000)
                        await page.wait_for_timeout(2000)
                        break
                except Exception:
                    pass

            try:
                if await page.is_visible("button:has-text('Change')"):
                    await page.click("button:has-text('Change')", timeout=2000)
                    await page.wait_for_timeout(1000)
            except Exception:
                pass

            for inp in [
                "input[placeholder='Enter delivery address']",
                "input#location-typeahead-home-input",
                "input#location-typeahead-input",
            ]:
                try:
                    await page.wait_for_selector(inp, timeout=3000)
                    await page.fill(inp, zip_code)
                    await page.wait_for_timeout(1000)
                    await page.press(inp, "Enter")
                    await page.wait_for_timeout(3000)
                    break
                except Exception:
                    pass

            for dd in [
                'ul[role="listbox"] li:first-child',
                'div[data-testid="location-typeahead-home-menu"] div:first-child',
                'div[data-testid="location-typeahead-input-menu"] div:first-child',
            ]:
                try:
                    if await page.is_visible(dd):
                        await page.click(dd, timeout=3000)
                        await page.wait_for_timeout(5000)
                        break
                except Exception:
                    pass

            # Sometimes a "Delivery details" modal pops up covering the screen.
            # Click "Done" to dismiss it.
            try:
                done_btn = await page.query_selector(
                    'button[data-testid="done-button"]'
                )
                if done_btn and await done_btn.is_visible():
                    await done_btn.click()
                    await page.wait_for_timeout(2000)
            except Exception:
                pass
        except Exception as e:
            logger.warning("[UberEats] Failed to set location: %s", e)

    async def scrape_listings(self) -> list[MerchantListing]:
        base_url = self._build_listing_url()
        logger.info(
            "[UberEats] Scraping listings for market '%s': %s", self.market, base_url
        )

        all_listings: list[MerchantListing] = []
        seen_ids: set[str] = set()
        max_pages = 200

        try:
            page = await self._context.new_page()
            try:
                await self._rate_limit()
                await page.goto(base_url, wait_until="domcontentloaded", timeout=60_000)
                await page.wait_for_timeout(5000)

                # Handle cookie banner and address prompt
                await self._handle_prompts(page)

                for page_num in range(1, max_pages + 1):
                    # Parse current page
                    html = await page.content()
                    page_listings = parse_listing(html, self.PLATFORM, self.market)

                    new_merchants = [
                        m
                        for m in page_listings
                        if m.platform_merchant_id not in seen_ids
                    ]

                    logger.info(
                        "[UberEats] Page %d: %d on page, %d new merchants (total: %d)",
                        page_num,
                        len(page_listings),
                        len(new_merchants),
                        len(seen_ids) + len(new_merchants),
                    )

                    if new_merchants:
                        all_listings.extend(new_merchants)
                        seen_ids.update(m.platform_merchant_id for m in new_merchants)
                        self._archive_html(
                            f"listing_{self.market}_page{page_num}", html
                        )

                    # No new merchants = we've exhausted pagination
                    if not new_merchants and page_num > 1:
                        logger.info(
                            "[UberEats] No new merchants on page %d, stopping.",
                            page_num,
                        )
                        break

                    # Navigate to next page by clicking the numbered page link
                    next_page = page_num + 1
                    navigated = False

                    # Scroll to bottom to reveal pagination
                    await page.evaluate(
                        "window.scrollTo(0, document.body.scrollHeight)"
                    )
                    await page.wait_for_timeout(1000)

                    # Click the numbered page link (most reliable method)
                    try:
                        page_links = await page.locator(
                            f'a[href*="page={next_page}"]'
                        ).all()
                        if page_links:
                            await page_links[0].click()
                            await page.wait_for_timeout(4000)
                            navigated = True
                    except Exception:
                        pass

                    if not navigated:
                        logger.info(
                            "[UberEats] No page %d link found, stopping pagination.",
                            next_page,
                        )
                        break

            finally:
                await page.close()

        except Exception as exc:
            logger.error(
                "[UberEats] Listing scrape failed for %s: %s", self.market, exc
            )

        logger.info(
            "[UberEats/%s] Total listings extracted: %d", self.market, len(all_listings)
        )
        return all_listings

    async def scrape_detail(self, listing: MerchantListing) -> MerchantListing:
        """Scrape individual merchant detail page for address and coordinates."""
        if not listing.raw_url:
            return listing

        # Optimization: If address was found on listing page, skip detail scrape
        if listing.address:
            logger.info(
                "[UberEats] Skipping detail for %s (Address already found: %s)",
                listing.name,
                listing.address,
            )
            return listing

        last_exc = None
        for attempt in range(1, 4):  # 3 attempts
            try:
                page = await self._get_reusable_page()
                await self._rate_limit()

                # Extended timeout and more permissive wait
                response = await page.goto(
                    listing.raw_url, wait_until="domcontentloaded", timeout=60_000
                )

                if response and response.status == 404:
                    logger.warning("[UberEats] Merchant page 404: %s", listing.name)
                    return listing

                await page.wait_for_timeout(3000)

                # Handle cookie banner if it appears
                await self._handle_prompts(page)

                html = await page.content()
                detail_data = parse_detail(self.PLATFORM, html, self.market)

                if detail_data.get("address"):
                    listing.address = detail_data["address"]
                if detail_data.get("lat"):
                    listing.lat = detail_data["lat"]
                if detail_data.get("lng"):
                    listing.lng = detail_data["lng"]

                logger.info(
                    "[UberEats] Detail scraped for %s: %s",
                    listing.name,
                    listing.address,
                )
                return listing  # Success

            except Exception as e:
                last_exc = e
                logger.warning(
                    "[UberEats] Attempt %d failed for %s: %s", attempt, listing.name, e
                )
                await asyncio.sleep(2 * attempt)

        logger.error(
            "[UberEats] All attempts failed for %s: %s", listing.name, last_exc
        )
        return listing
