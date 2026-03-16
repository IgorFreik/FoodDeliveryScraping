"""
Abstract base scraper with built-in:
- Retry logic with exponential backoff
- Proxy rotation via Bright Data
- Rate limiting (token bucket)
- Raw HTML archival to MinIO
- Playwright browser lifecycle management
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from abc import ABC, abstractmethod
from typing import Any

import yaml
from playwright.async_api import Browser, BrowserContext, Page, async_playwright

from processing.models import MerchantListing
from scrapers.utils.proxy import get_proxy_config, get_browser_url
from scrapers.utils.stealth import apply_stealth
from storage.minio_client import upload_raw_html

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """
    Abstract base for all platform scrapers.

    Subclasses implement ``scrape_listings()`` and ``scrape_detail()``
    while inheriting retry, proxy, rate-limit, and archival logic.
    """

    PLATFORM: str = ""  # Override in subclass

    def __init__(
        self,
        market: str,
        *,
        headless: bool = True,
        max_retries: int = 3,
        requests_per_second: float = 2.0,
        delay_range_ms: tuple[int, int] = (1500, 4000),
    ):
        self.market = market
        self.headless = headless
        self.max_retries = max_retries
        self.requests_per_second = requests_per_second
        self.delay_range_ms = delay_range_ms

        # Rate-limit state
        self._last_request_time: float = 0
        self._min_interval = 1.0 / requests_per_second

        # Browser (set up in __aenter__)
        self._playwright = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None

    # ── Lifecycle ───────────────────────────────────────────────────

    async def __aenter__(self):
        self._playwright = await async_playwright().start()
        
        browser_url = get_browser_url()
        if browser_url:
            logger.info("[%s] Connecting to Bright Data Scraping Browser via CDP...", self.PLATFORM)
            self._browser = await self._playwright.chromium.connect_over_cdp(browser_url)
            # When connecting via CDP to Bright Data, use the default context provided by the remote browser
            # Overriding headers/locale is often forbidden or causes protocol errors.
            self._context = self._browser.contexts[0] if self._browser.contexts else await self._browser.new_context()
        else:
            proxy_config = get_proxy_config()
            launch_kwargs: dict[str, Any] = {
                "headless": self.headless,
                "args": ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
            }
            if proxy_config:
                launch_kwargs["proxy"] = proxy_config
            
            self._browser = await self._playwright.chromium.launch(**launch_kwargs)
            self._context = await self._browser.new_context(
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
                timezone_id="America/New_York",
            )
            await apply_stealth(self._context)

        return self

    async def __aexit__(self, *args):
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    # ── Rate Limiting ───────────────────────────────────────────────

    async def _rate_limit(self):
        """Sleep to respect the configured rate limit + jitter."""
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < self._min_interval:
            await asyncio.sleep(self._min_interval - elapsed)

        # Add random jitter
        jitter_ms = random.randint(*self.delay_range_ms)
        await asyncio.sleep(jitter_ms / 1000.0)
        self._last_request_time = time.monotonic()

    # ── Retry Wrapper ───────────────────────────────────────────────

    async def _fetch_with_retry(self, url: str) -> str:
        """
        Navigate to ``url``, wait for content, and return the page HTML.
        Retries up to ``max_retries`` times with exponential backoff.
        """
        last_exc: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                await self._rate_limit()
                page: Page = await self._context.new_page()
                try:
                    response = await page.goto(url, wait_until="domcontentloaded", timeout=30_000)

                    if response and response.status >= 400:
                        raise RuntimeError(f"HTTP {response.status} for {url}")

                    # Wait for meaningful content to render
                    await page.wait_for_timeout(2000)
                    html = await page.content()
                    return html
                finally:
                    await page.close()

            except Exception as exc:
                last_exc = exc
                wait = 2 ** attempt + random.random()
                logger.warning(
                    "[%s] Attempt %d/%d failed for %s: %s — retrying in %.1fs",
                    self.PLATFORM, attempt, self.max_retries, url, exc, wait,
                )
                await asyncio.sleep(wait)

        raise RuntimeError(
            f"All {self.max_retries} attempts failed for {url}"
        ) from last_exc

    # ── Archival ────────────────────────────────────────────────────

    def _archive_html(self, merchant_id: str, html: str) -> str:
        """Upload raw HTML to MinIO and publish Kafka event."""
        s3_key = upload_raw_html(
            platform=self.PLATFORM,
            market=self.market,
            merchant_id=merchant_id,
            html=html,
        )

        from streaming.producer import publish_event
        publish_event(
            topic="raw-html-scraped",
            event_data={
                "platform": self.PLATFORM,
                "market": self.market,
                "merchant_id": merchant_id,
                "minio_key": s3_key,
                "scraped_at": time.time(),
            }
        )

        return s3_key

    # ── Abstract Methods ────────────────────────────────────────────

    @abstractmethod
    async def scrape_listings(self) -> list[MerchantListing]:
        """Scrape the listing/search page and return merchant stubs."""
        ...

    @abstractmethod
    async def scrape_detail(self, listing: MerchantListing) -> MerchantListing:
        """Enrich a merchant stub with menu, hours, and detail data."""
        ...

    # ── Convenience ─────────────────────────────────────────────────

    async def run(self) -> list[MerchantListing]:
        """Full scrape: listings → details for each merchant."""
        listings = await self.scrape_listings()
        logger.info("[%s/%s] Found %d listings.", self.PLATFORM, self.market, len(listings))

        enriched: list[MerchantListing] = []
        for listing in listings:
            try:
                detail = await self.scrape_detail(listing)
                enriched.append(detail)
            except Exception as exc:
                logger.error(
                    "[%s] Failed to scrape detail for %s: %s",
                    self.PLATFORM, listing.name, exc,
                )
                enriched.append(listing)  # Keep stub even if detail fails

        # Flush Kafka events before exiting
        from streaming.producer import flush_producer
        flush_producer()

        return enriched
