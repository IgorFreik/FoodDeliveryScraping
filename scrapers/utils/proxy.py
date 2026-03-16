"""
Bright Data (or any HTTP proxy) integration.

Reads proxy credentials from environment variables and returns a dict
compatible with Playwright's ``proxy`` launch option.
"""

from __future__ import annotations

import os
from typing import Any


def get_proxy_config() -> dict[str, Any] | None:
    """
    Return a Playwright-compatible proxy dict, or None if not configured.

    Expected env vars:
        BRIGHTDATA_PROXY_HOST
        BRIGHTDATA_PROXY_PORT
        BRIGHTDATA_PROXY_USER
        BRIGHTDATA_PROXY_PASS
    """
    host = os.getenv("BRIGHTDATA_PROXY_HOST", "").strip()
    port = os.getenv("BRIGHTDATA_PROXY_PORT", "").strip()
    user = os.getenv("BRIGHTDATA_PROXY_USER", "").strip()
    password = os.getenv("BRIGHTDATA_PROXY_PASS", "").strip()

    if not host or not port:
        return None

    proxy: dict[str, Any] = {
        "server": f"http://{host}:{port}",
    }

    if user:
        proxy["username"] = user
    if password:
        proxy["password"] = password

    return proxy


def get_httpx_proxy_url() -> str | None:
    """
    Return a proxy URL string for httpx / requests, or None.

    Format: ``http://user:pass@host:port``
    """
    host = os.getenv("BRIGHTDATA_PROXY_HOST", "").strip()
    port = os.getenv("BRIGHTDATA_PROXY_PORT", "").strip()
    user = os.getenv("BRIGHTDATA_PROXY_USER", "").strip()
    password = os.getenv("BRIGHTDATA_PROXY_PASS", "").strip()

    if not host or not port:
        return None

    auth = f"{user}:{password}@" if user else ""
    return f"http://{auth}{host}:{port}"


def get_browser_url() -> str | None:
    """
    Return the Bright Data Scraping Browser WebSocket URL (Remote CDP).
    Expected env var: BRIGHTDATA_BROWSER_URL
    """
    url = os.getenv("BRIGHTDATA_BROWSER_URL", "").strip()
    return url if url else None
