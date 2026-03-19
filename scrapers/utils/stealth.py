"""
Stealth utilities for evading bot detection.

Applies anti-fingerprinting patches to Playwright browser contexts:
- Overrides navigator.webdriver
- Randomizes User-Agent
- Patches WebGL and Canvas fingerprinting
- Adds realistic browser plugins/languages
"""

from __future__ import annotations

import random

from playwright.async_api import BrowserContext

# ── User-Agent Pool ────────────────────────────────────────────────

_USER_AGENTS = [
    # Chrome on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Chrome on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Firefox on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:126.0) Gecko/20100101 Firefox/126.0",
    # Firefox on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    # Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
]


# ── Stealth JavaScript Patches ──────────────────────────────────────

_STEALTH_JS = """
// Override navigator.webdriver
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

// Override chrome runtime
window.chrome = { runtime: {} };

// Override permissions query
const _query = window.navigator.permissions.query;
window.navigator.permissions.query = (parameters) =>
    parameters.name === 'notifications'
        ? Promise.resolve({ state: Notification.permission })
        : _query(parameters);

// Override plugins to appear non-empty
Object.defineProperty(navigator, 'plugins', {
    get: () => [
        { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer' },
        { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
        { name: 'Native Client', filename: 'internal-nacl-plugin' },
    ],
});

// Override languages
Object.defineProperty(navigator, 'languages', {
    get: () => ['en-US', 'en'],
});

// Prevent canvas fingerprinting (add noise)
const _toDataURL = HTMLCanvasElement.prototype.toDataURL;
HTMLCanvasElement.prototype.toDataURL = function(type) {
    if (type === 'image/png') {
        const ctx = this.getContext('2d');
        if (ctx) {
            const imageData = ctx.getImageData(0, 0, this.width, this.height);
            for (let i = 0; i < imageData.data.length; i += 4) {
                imageData.data[i] ^= 1;  // Slight noise on red channel
            }
            ctx.putImageData(imageData, 0, 0);
        }
    }
    return _toDataURL.apply(this, arguments);
};
"""


# ── Public API ──────────────────────────────────────────────────────


async def apply_stealth(context: BrowserContext) -> None:
    """
    Apply stealth patches to a Playwright BrowserContext.

    Should be called right after creating the context, before any
    navigation.
    """
    # Randomize User-Agent
    ua = random.choice(_USER_AGENTS)
    await context.set_extra_http_headers(
        {
            "User-Agent": ua,
            "Accept-Language": "en-US,en;q=0.9",
        }
    )

    # Inject stealth JS into every new page
    await context.add_init_script(_STEALTH_JS)


async def human_like_scroll(page, scroll_count: int = 3) -> None:
    """
    Simulate human-like scrolling behavior on a page.

    Scrolls down in random increments with random pauses.
    """
    for _ in range(scroll_count):
        scroll_amount = random.randint(300, 800)
        await page.evaluate(f"window.scrollBy(0, {scroll_amount})")
        await page.wait_for_timeout(random.randint(500, 1500))

    # Scroll back up a bit (humans do this)
    await page.evaluate(f"window.scrollBy(0, -{random.randint(100, 300)})")
    await page.wait_for_timeout(random.randint(300, 800))
