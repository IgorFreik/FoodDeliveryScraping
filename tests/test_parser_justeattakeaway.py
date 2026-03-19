"""
Unit tests for the JustEatTakeaway listing parser.
"""

from __future__ import annotations

import json

from processing.parser import parse_justeattakeaway_listing, parse_listing

# ── Helper Fixtures ───────────────────────────────────────────────────


def _wrap_next_data_restaurant_data(restaurant_data: dict[str, dict]) -> str:
    """Wrap restaurantData in __NEXT_DATA__ or __REACT_QUERY_STATE__ style."""
    payload = {"restaurantData": restaurant_data}
    return f'<html><script id="__NEXT_DATA__" type="application/json">{json.dumps(payload)}</script></html>'


def _wrap_react_query_restaurant_data(restaurant_data: dict[str, dict]) -> str:
    """Wrap restaurantData in nested __REACT_QUERY_STATE__ structure."""
    payload = {"queries": [{"state": {"data": {"restaurantData": restaurant_data}}}]}
    return f'<html><script id="__REACT_QUERY_STATE__" type="application/json">{json.dumps(payload)}</script></html>'


# ── JustEatTakeaway Parser ───────────────────────────────────────────


class TestJustEatTakeawayParser:
    def test_parse_from_restaurant_data_next_data(self):
        html = _wrap_next_data_restaurant_data(
            {
                "abc123": {
                    "name": "Pizza Palace",
                    "uniqueName": "pizza-palace",
                    "primarySlug": "pizza-palace",
                    "address": {
                        "firstLine": "Overtoom 123",
                        "postalCode": "1054 HG",
                        "city": "Amsterdam",
                        "location": {
                            "coordinates": [4.87, 52.36],
                        },
                    },
                    "rating": {"score": 4.5, "count": 120},
                },
            }
        )
        results = parse_justeattakeaway_listing(html, "amsterdam")
        assert len(results) == 1
        m = results[0]
        assert m.platform == "justeattakeaway"
        assert m.platform_merchant_id == "abc123"
        assert m.name == "Pizza Palace"
        assert "Overtoom" in (m.address or "")
        assert "1054" in (m.address or "")
        assert m.rating == 4.5
        assert m.review_count == 120
        assert m.lat == 52.36
        assert m.lng == 4.87
        assert "thuisbezorgd.nl" in (m.raw_url or "")

    def test_parse_from_react_query_state(self):
        html = _wrap_react_query_restaurant_data(
            {
                "xyz789": {
                    "name": "Sushi Bar",
                    "uniqueName": "sushi-bar",
                    "address": {
                        "firstLine": "Leidseplein 5",
                        "postalCode": "1017",
                        "city": "Amsterdam",
                        "location": {"coordinates": [4.88, 52.37]},
                    },
                    "rating": {"score": 4.2, "count": 85},
                },
            }
        )
        results = parse_justeattakeaway_listing(html, "amsterdam")
        assert len(results) == 1
        m = results[0]
        assert m.platform_merchant_id == "xyz789"
        assert m.name == "Sushi Bar"
        assert "Leidseplein" in (m.address or "")

    def test_parse_empty_html(self):
        results = parse_justeattakeaway_listing("<html></html>", "amsterdam")
        assert results == []

    def test_parse_multiple_restaurants(self):
        html = _wrap_next_data_restaurant_data(
            {
                "r1": {
                    "name": "Restaurant A",
                    "uniqueName": "a",
                    "address": {"firstLine": "Street 1", "location": {"coordinates": [4.9, 52.35]}},
                },
                "r2": {
                    "name": "Restaurant B",
                    "uniqueName": "b",
                    "address": {
                        "firstLine": "Street 2",
                        "location": {"coordinates": [4.91, 52.36]},
                    },
                },
            }
        )
        results = parse_justeattakeaway_listing(html, "amsterdam")
        assert len(results) == 2
        names = {m.name for m in results}
        assert "Restaurant A" in names
        assert "Restaurant B" in names

    def test_parse_via_dispatcher(self):
        html = _wrap_next_data_restaurant_data(
            {
                "disp123": {
                    "name": "Dispatcher Test",
                    "uniqueName": "disp",
                    "address": {"firstLine": "Test 1", "location": {"coordinates": [4.9, 52.35]}},
                },
            }
        )
        results = parse_listing(html, "justeattakeaway", "amsterdam")
        assert len(results) == 1
        assert results[0].platform == "justeattakeaway"
        assert results[0].name == "Dispatcher Test"
