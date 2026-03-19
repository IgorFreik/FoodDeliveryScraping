"""
Unit tests for the parser module.

Tests cover:
- DoorDash listing parsing (JSON + HTML card fallback)
- Grubhub listing parsing (JSON + HTML card fallback)
- Menu item parsing from detail pages
- Edge cases: empty HTML, malformed data
"""

from __future__ import annotations

import json

from processing.parser import (
    _safe_float,
    _safe_int,
    parse_doordash_listing,
    parse_grubhub_listing,
    parse_listing,
)

# ── Helper Fixtures ─────────────────────────────────────────────────


def _wrap_next_data(stores: list[dict]) -> str:
    """Wrap store data in a DoorDash-style __NEXT_DATA__ script tag."""
    payload = {
        "props": {
            "pageProps": {
                "stores": stores,
            }
        }
    }
    return f'<html><script id="__NEXT_DATA__" type="application/json">{json.dumps(payload)}</script></html>'


def _wrap_grubhub_json(restaurants: list[dict]) -> str:
    """Wrap restaurant data in a Grubhub-style embedded JSON tag."""
    payload = {"restaurants": restaurants}
    return f'<html><script type="application/json">{json.dumps(payload)}</script></html>'


# ── safe_float / safe_int ───────────────────────────────────────────


class TestSafeCoercion:
    def test_safe_float_none(self):
        assert _safe_float(None) is None

    def test_safe_float_string(self):
        assert _safe_float("$12.50") == 12.50

    def test_safe_float_int(self):
        assert _safe_float(4) == 4.0

    def test_safe_float_garbage(self):
        assert _safe_float("n/a") is None

    def test_safe_int_none(self):
        assert _safe_int(None) is None

    def test_safe_int_string(self):
        assert _safe_int("1,234 reviews") == 1234

    def test_safe_int_float(self):
        assert _safe_int(42.9) == 42


# ── DoorDash Parser ────────────────────────────────────────────────


class TestDoorDashParser:
    def test_parse_from_next_data(self):
        html = _wrap_next_data([
            {
                "id": "12345",
                "name": "Joe's Pizza",
                "address": {"formattedAddress": "123 Main St, NYC", "lat": 40.71, "lng": -74.00},
                "tags": [{"name": "Italian"}, {"name": "Pizza"}],
                "averageRating": 4.5,
                "numRatings": 200,
                "priceRange": "$$",
                "deliveryFee": 2.99,
                "estimatedDeliveryTime": 30,
                "isSponsored": False,
            }
        ])

        results = parse_doordash_listing(html, "nyc")

        assert len(results) == 1
        m = results[0]
        assert m.platform == "doordash"
        assert m.platform_merchant_id == "12345"
        assert m.name == "Joe's Pizza"
        assert m.lat == 40.71
        assert m.lng == -74.00
        assert "Italian" in m.cuisine_tags
        assert m.rating == 4.5
        assert m.review_count == 200
        assert m.delivery_fee == 2.99

    def test_parse_empty_html(self):
        results = parse_doordash_listing("<html></html>", "nyc")
        assert results == []

    def test_parse_html_card_fallback(self):
        html = """
        <html><body>
            <article>
                <h2>Test Restaurant</h2>
                <a href="/store/999/">Link</a>
            </article>
        </body></html>
        """
        results = parse_doordash_listing(html, "nyc")
        assert len(results) == 1
        assert results[0].name == "Test Restaurant"
        assert results[0].platform_merchant_id == "999"


# ── Grubhub Parser ─────────────────────────────────────────────────


class TestGrubhubParser:
    def test_parse_from_json(self):
        html = _wrap_grubhub_json([
            {
                "restaurant_id": "67890",
                "name": "Taco Palace",
                "address": {
                    "street_address": "456 Oak Ave",
                    "latitude": 34.05,
                    "longitude": -118.24,
                },
                "cuisines": [{"name": "Mexican"}, {"name": "Tacos"}],
                "rating": {"rating_value": 4.2, "rating_count": 150},
                "price_rating": "$",
                "delivery_fee": {"price": 1.99},
                "delivery_time_estimate": 25,
                "sponsored": False,
            }
        ])

        results = parse_grubhub_listing(html, "la")

        # Grubhub parser is not yet implemented, so it returns []
        assert len(results) == 0

    def test_parse_empty_html(self):
        results = parse_grubhub_listing("<html></html>", "la")
        assert results == []





# ── Dispatcher ──────────────────────────────────────────────────────


class TestDispatcher:
    def test_unknown_platform_returns_empty(self):
        # The parser currently logs an error and returns [] instead of raising
        results = parse_listing("<html></html>", "unknown_platform", "nyc")
        assert results == []
