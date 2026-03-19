"""
Parsers for converting raw HTML / JSON responses into MerchantListing models.

Each platform has its own parse function. The parsers are intentionally
structured as standalone functions so they can be unit-tested without a browser.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from bs4 import BeautifulSoup

from processing.models import MerchantListing

logger = logging.getLogger(__name__)


# ── Helpers ─────────────────────────────────────────────────────────


def _safe_float(val: Any) -> float | None:
    """Try to coerce *val* to float; return None on failure."""
    if val is None:
        return None
    try:
        if isinstance(val, str):
            val = re.sub(r"[^\d.]", "", val)
        return float(val)
    except (ValueError, TypeError):
        return None


def _parse_ld_json(soup: BeautifulSoup) -> dict[str, Any]:
    """Extract address and coords from application/ld+json."""
    res = {}
    for stag in soup.find_all("script", type="application/ld+json"):
        if not (stag and stag.string):
            continue
        try:
            data = json.loads(stag.string)
            if isinstance(data, list):
                data = data[0] if data else {}

            # Look for Restaurant or FoodEstablishment
            if isinstance(data, dict) and data.get("@type") in ["Restaurant", "FoodEstablishment", "Store"]:
                # Address
                addr = data.get("address", {})
                if isinstance(addr, dict):
                    parts = []
                    # Try streetAddress first, then just 'address'
                    street = addr.get("streetAddress")
                    city = addr.get("addressLocality")
                    zip_code = addr.get("postalCode")

                    if street:
                        parts.append(str(street))
                    if zip_code:
                        parts.append(str(zip_code))
                    if city:
                        parts.append(str(city))

                    if parts:
                        res["address"] = ", ".join(parts)

                # Coordinates
                geo = data.get("geo", {})
                if isinstance(geo, dict):
                    res["lat"] = _safe_float(geo.get("latitude"))
                    res["lng"] = _safe_float(geo.get("longitude"))

                if res.get("address"):
                    return res
        except Exception:
            continue
    return res


def _safe_int(val: Any) -> int | None:
    if val is None:
        return None
    try:
        if isinstance(val, str):
            val = re.sub(r"[^\d]", "", val)
        return int(val)
    except (ValueError, TypeError):
        return None


def _load_encoded_json(text: str) -> Any:
    """Load JSON that might be unicode-escaped or malformed."""
    if not text:
        return None
    try:
        # First try normal load
        return json.loads(text)
    except json.JSONDecodeError:
        try:
            # Try to unescape unicode characters manually if needed
            # but usually json.loads handles \u0022 fine if it's a real string.
            # If it's a "string of a string", we might need multiple passes.
            cleaned = text.encode().decode('unicode_escape')
            return json.loads(cleaned)
        except Exception:
            return None

def find_in_json(obj, target_val, keys=["uuid", "storeUUID", "id", "slug", "storeSlug", "platform_id"]):
    """Recursively search for an object that has any of the target keys matching target_val."""
    if isinstance(obj, str) and (obj.startswith("{") or obj.startswith("[")):
        try:
            nested = json.loads(obj)
            return find_in_json(nested, target_val, keys)
        except Exception:
            pass

    if isinstance(obj, dict):
        # Flexible matching
        for k in keys:
            if str(obj.get(k)) == str(target_val):
                return obj

        # Search values
        for v in obj.values():
            res = find_in_json(v, target_val, keys)
            if res:
                return res
    elif isinstance(obj, list):
        for item in obj:
            res = find_in_json(item, target_val, keys)
            if res:
                return res
    return None


# ── DoorDash Parser ─────────────────────────────────────────────────


def parse_doordash_listing(html: str, market: str) -> list[MerchantListing]:
    merchants: list[MerchantListing] = []
    soup = BeautifulSoup(html, "html.parser")
    script_tag = soup.find("script", id="__NEXT_DATA__")
    stores = []
    if script_tag and script_tag.string:
        try:
            data = json.loads(script_tag.string)
            props = data.get("props", {}).get("pageProps", {})
            for key in ("stores", "storeList", "searchResults", "initialData"):
                if key in props:
                    val = props[key]
                    if isinstance(val, list):
                        stores = val
                    elif isinstance(val, dict):
                        for sk in ("stores", "items", "results"):
                            if sk in val and isinstance(val[sk], list):
                                stores = val[sk]
                                break
                    if stores:
                        break
        except Exception:
            pass

    for s in stores:
        merchants.append(MerchantListing(
            platform="doordash",
            platform_merchant_id=str(s.get("id", s.get("storeId", ""))),
            name=s.get("name", ""),
            address=s.get("address", {}).get("formattedAddress", "") if isinstance(s.get("address"), dict) else str(s.get("address", "")),
            lat=_safe_float(s.get("address", {}).get("lat") if isinstance(s.get("address"), dict) else None),
            lng=_safe_float(s.get("address", {}).get("lng") if isinstance(s.get("address"), dict) else None),
            cuisine_tags=[t.get("name") for t in s.get("tags", []) if isinstance(t, dict) and t.get("name")],
            rating=_safe_float(s.get("averageRating")),
            review_count=_safe_int(s.get("numRatings")),
            delivery_fee=_safe_float(s.get("deliveryFee")),
            market=market,
            raw_url=f"https://www.doordash.com/store/{s.get('id', '')}/",
        ))

    if not merchants:
        cards = soup.select("[data-testid='StoreCard'], .store-card, article")
        for card in cards:
            try:
                name_el = card.select_one("h2, h3, [data-testid='StoreName'], .store-name")
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                link = card.select_one("a[href]")
                href = link["href"] if link else ""
                merchants.append(MerchantListing(
                    platform="doordash", platform_merchant_id=href.rstrip("/").split("/")[-1] if href else name,
                    name=name, market=market,
                    raw_url=f"https://www.doordash.com{href}" if href.startswith("/") else href,
                ))
            except Exception:
                pass
    return merchants


# ── Grubhub Parser ─────────────────────────────────────────────────


def parse_grubhub_listing(html: str, market: str) -> list[MerchantListing]:
    # Placeholder
    return []


# ── JustEatTakeaway Parser ─────────────────────────────────────────

def parse_justeattakeaway_listing(html: str, market: str) -> list[MerchantListing]:
    merchants: list[MerchantListing] = []
    soup = BeautifulSoup(html, "html.parser")

    def find_key(obj: Any, key: str) -> Any:
        if isinstance(obj, dict):
            if key in obj:
                return obj[key]
            for v in obj.values():
                found = find_key(v, key)
                if found is not None:
                    return found
        elif isinstance(obj, list):
            for i in obj:
                found = find_key(i, key)
                if found is not None:
                    return found
        return None

    for sid in ["__NEXT_DATA__", "__REACT_QUERY_STATE__"]:
        stag = soup.find("script", id=sid)
        if stag and stag.string:
            try:
                data = json.loads(stag.string)
                # Thuisbezorgd/JET embeds restaurantData: {id: {name, address, ...}}
                rd = find_key(data, "restaurantData")
                if rd and isinstance(rd, dict):
                    for rid, s in rd.items():
                        if not isinstance(s, dict):
                            continue
                        unique_name = s.get("uniqueName", s.get("primarySlug", rid))
                        addr = s.get("address", {}) or {}
                        loc = addr.get("location", {}) if isinstance(addr, dict) else {}
                        coords = loc.get("coordinates", [0, 0]) if isinstance(loc, dict) else [0, 0]
                        # Build full address: firstLine, postalCode city
                        first_line = addr.get("firstLine", "") if isinstance(addr, dict) else ""
                        postal = addr.get("postalCode", "") if isinstance(addr, dict) else ""
                        city = addr.get("city", "") if isinstance(addr, dict) else ""
                        parts = [p for p in [first_line, postal, city] if p]
                        address_str = ", ".join(parts) if parts else first_line
                        merchants.append(
                            MerchantListing(
                                platform="justeattakeaway",
                                platform_merchant_id=str(rid),
                                name=s.get("name", ""),
                                rating=_safe_float(s.get("rating", {}).get("score")),
                                review_count=_safe_int(s.get("rating", {}).get("count")),
                                market=market,
                                raw_url=f"https://www.thuisbezorgd.nl/en/menu/{unique_name}",
                                lat=_safe_float(coords[1]) if len(coords) > 1 else None,
                                lng=_safe_float(coords[0]) if len(coords) > 0 else None,
                                address=address_str,
                            )
                        )
                    if merchants:
                        return merchants

                # Fallback: generic find objects with name + location
                def find_restaurants(obj: Any) -> list:
                    if isinstance(obj, dict):
                        if "name" in obj and "location" in obj and isinstance(obj["location"], dict):
                            loc = obj["location"]
                            if "lat" in loc or "latitude" in loc:
                                return [obj]
                        res = []
                        for v in obj.values():
                            res.extend(find_restaurants(v))
                        return res
                    if isinstance(obj, list):
                        res = []
                        for i in obj:
                            res.extend(find_restaurants(i))
                        return res
                    return []

                rests = find_restaurants(data)
                for s in rests:
                    merchants.append(
                        MerchantListing(
                            platform="justeattakeaway",
                            platform_merchant_id=str(s.get("id", s.get("primarySlug", s.get("name")))),
                            name=s.get("name", ""),
                            rating=_safe_float(s.get("rating", {}).get("score")),
                            review_count=_safe_int(s.get("rating", {}).get("count")),
                            market=market,
                            raw_url=f"https://www.thuisbezorgd.nl/en/menu/{s.get('primarySlug', s.get('id'))}",
                            lat=_safe_float(s.get("location", {}).get("lat") or s.get("location", {}).get("latitude")),
                            lng=_safe_float(s.get("location", {}).get("lng") or s.get("location", {}).get("longitude")),
                            address=s.get("address", {}).get("streetName", ""),
                        )
                    )
                if merchants:
                    return merchants
            except Exception:
                pass

    return merchants


# ── Uber Eats Parser ───────────────────────────────────────────────

def parse_ubereats_listing(html: str, market: str) -> list[MerchantListing]:
    merchants: list[MerchantListing] = []
    soup = BeautifulSoup(html, "html.parser")

    def _norm_text(s: str) -> str:
        return re.sub(r"\s+", " ", (s or "").strip())

    def _looks_like_address(t: str) -> bool:
        t = _norm_text(t)
        if len(t) < 8:
            return False
        # Strong signals: NL postal code, or "street..., <number...>"
        if re.search(r"\b\d{4}\s?[A-Z]{2}\b", t):
            return True
        if "," in t and re.search(r"\d", t):
            return True
        return False

    # Prefer stable store-card links; they exist in the provided fixture.
    store_links = soup.select("a[data-testid='store-card'][href*='/store/']")
    if not store_links:
        store_links = soup.select("a[href*='/store/']")

    seen_ids: set[str] = set()
    for link_el in store_links:
        try:
            href = link_el.get("href", "")
            if not href or "/store/" not in href:
                continue

            store_id = href.split("?", 1)[0].rstrip("/").split("/")[-1]
            if not store_id or store_id == "store" or store_id in seen_ids:
                continue
            seen_ids.add(store_id)

            # Walk up to a reasonable card root (Uber uses a <div class="_hs ..."> wrapper).
            card = None
            for parent in link_el.parents:
                if parent is None or not hasattr(parent, "name"):
                    continue
                if parent.name not in ("div", "li", "article"):
                    continue
                cls = parent.get("class") or []
                if isinstance(cls, list) and "_hs" in cls:
                    card = parent
                    break
            if card is None:
                card = link_el.parent or link_el

            name = ""
            name_el = card.select_one("[data-test='store-title'], h3, h4, .store-name")
            if name_el:
                name = _norm_text(name_el.get_text(" ", strip=True))
            if not name:
                name = _norm_text(link_el.get_text(" ", strip=True).split("•")[0])
            if not name:
                name = _norm_text(link_el.get("aria-label", ""))
            if not name:
                continue

            rating, review_count = None, None
            label_el = card.select_one('[aria-label*="Rating"], [aria-label*="Rated"], [aria-label*="Gewaardeerd"]')
            if label_el:
                label = label_el["aria-label"]
                rmatch = re.search(r"(\d[.,]\d)", label)
                if rmatch:
                    rating = _safe_float(rmatch.group(1).replace(",", "."))
                cmatch = re.search(r"(\d+[\d\s,.]*)\s*(?:reviews|recensies|ratings|waarderingen)", label, re.I)
                if cmatch:
                    review_count = _safe_int(re.sub(r"\D", "", cmatch.group(1)))

            # DOM address extraction (fixture has a clean address span on each card).
            address = ""
            candidates: list[str] = []
            for el in card.find_all(["span", "div", "p"]):
                t = _norm_text(el.get_text(" ", strip=True))
                if _looks_like_address(t):
                    candidates.append(t)
            if candidates:
                # Prefer the shortest candidate among the strong signals to avoid
                # selecting whole-card flattened text.
                candidates.sort(key=len)
                address = candidates[0]

            # High-accuracy JSON extraction as supplement (coords; address as fallback).
            lat, lng = None, None
            found_in_json = False
            for sid in ["__REACT_QUERY_STATE__", "__REDUX_STATE__"]:
                stag = soup.find("script", id=sid)
                if not (stag and stag.string):
                    continue

                # FALLBACK: Regex search for coordinates near the store_id in the raw string
                # This bypasses the need to parse the potentially malformed/escaped JSON
                raw_state = stag.string
                # Look for the store_id first to narrow down the search
                start_idx = raw_state.find(store_id)
                if start_idx != -1:
                    # Find coordinates in a 2000-char window around the store_id
                    window = raw_state[max(0, start_idx-1000):start_idx+1000]
                    # Log snippet for debugging (internal only)
                    # logger.debug(f"[ubereats] Window for {store_id}: {window[:100]}...")

                    # Search for coordinates with various names and possible escaping
                    # Handles \u0022latitude\u0022: 52.3, "latitude": 52.3, etc.
                    lat_match = re.search(r'(?:latitude|lat)["\u0022\\]*:\s*(-?\d+\.\d+)', window, re.I)
                    lng_match = re.search(r'(?:longitude|lng)["\u0022\\]*:\s*(-?\d+\.\d+)', window, re.I)

                    if lat_match and lng_match:
                        lat = _safe_float(lat_match.group(1))
                        lng = _safe_float(lng_match.group(1))
                        if lat and lng:
                            logger.info(f"[ubereats] Found coordinates via REGEX for {store_id}: {lat}, {lng}")
                            found_in_json = True
                            break

                # If regex fails, try the standard JSON path (cleaned)
                try:
                    raw_text = stag.string.replace('\\u0022', '"').replace('\\"', '"')
                    if raw_text.startswith('"') and raw_text.endswith('"'):
                        raw_text = raw_text[1:-1]
                    data = json.loads(raw_text)
                    store_data = find_in_json(data, store_id)
                    if store_data:
                        loc = store_data.get("location") or store_data
                        lat = _safe_float(loc.get("latitude") or loc.get("lat"))
                        lng = _safe_float(loc.get("longitude") or loc.get("lng"))
                        if not address:
                            addr_obj = store_data.get("address")
                            if isinstance(addr_obj, dict):
                                address = addr_obj.get("streetAddress") or addr_obj.get("formattedAddress", "")
                            elif isinstance(addr_obj, str):
                                address = addr_obj
                        if lat and lng:
                            found_in_json = True
                            break
                except Exception:
                    pass
                if found_in_json:
                    break

            merchants.append(MerchantListing(
                platform="ubereats",
                platform_merchant_id=store_id,
                name=name, market=market,
                raw_url=f"https://www.ubereats.com{href}" if href.startswith("/") else href,
                rating=rating, review_count=review_count,
                lat=lat, lng=lng, address=address
            ))
            if lat and lng:
                logger.info(f"[ubereats] Extracted coordinates for {store_id}: {lat}, {lng}")

        except Exception as exc:
            logger.debug("Failed to parse Uber Eats card: %s", exc)

    return merchants


def parse_listing(html: str, platform: str, market: str) -> list[MerchantListing]:
    """Dispatch to the appropriate platform parser."""
    parser = LISTING_PARSERS.get(platform)
    if not parser:
        logger.error("No parser found for platform: %s", platform)
        return []
    return parser(html, market)


LISTING_PARSERS = {
    "doordash": parse_doordash_listing,
    "grubhub": parse_grubhub_listing,
    "justeattakeaway": parse_justeattakeaway_listing,
    "ubereats": parse_ubereats_listing,
}

# ── Detail Parsers ─────────────────────────────────────────────────

def parse_ubereats_detail(html: str, market: str) -> dict[str, Any]:
    """Parse Uber Eats detail page for extra info like address."""
    soup = BeautifulSoup(html, "html.parser")
    res = _parse_ld_json(soup)
    if res.get("address") and res.get("lat"):
        return res

    # Try script tags hydration state
    for sid in ["__REACT_QUERY_STATE__", "__REDUX_STATE__"]:
        stag = soup.find("script", id=sid)
        if not (stag and stag.string):
            continue
        try:
            # Aggressive unescaping as learned from listing parser
            raw_text = stag.string.replace('\\u0022', '"').replace('\\"', '"')
            if raw_text.startswith('"') and raw_text.endswith('"'):
                raw_text = raw_text[1:-1]
            data = json.loads(raw_text)

            # Find any object that looks like it has a 'location' or 'address'
            # Find any object that looks like it has a 'location' or 'address'
            # Since we are on a detail page, we just look for the first one that fits
            def find_address_in_obj(obj):
                if isinstance(obj, dict):
                    if "streetAddress" in obj or ("address" in obj and isinstance(obj["address"], dict)):
                        return obj
                    for v in obj.values():
                        found = find_address_in_obj(v)
                        if found:
                            return found
                elif isinstance(obj, list):
                    for i in obj:
                        found = find_address_in_obj(i)
                        if found:
                            return found
                return None

            store_info = find_address_in_obj(data)
            if store_info:
                addr = store_info.get("address")
                if isinstance(addr, dict):
                    res["address"] = addr.get("streetAddress") or addr.get("formattedAddress", "")
                elif isinstance(addr, str):
                    res["address"] = addr

                loc = store_info.get("location") or store_info
                if isinstance(loc, dict):
                    res["lat"] = _safe_float(loc.get("latitude") or loc.get("lat"))
                    res["lng"] = _safe_float(loc.get("longitude") or loc.get("lng"))

                if res.get("address"):
                    break
        except Exception:
            pass

    # Fallback to DOM
    if not res.get("address"):
        # Look for typical address containers
        addr_el = soup.select_one("button[aria-label*='Address'], button[aria-label*='Adres'], [data-testid='store-info-address']")
        if addr_el:
            res["address"] = addr_el.get_text(separator=", ", strip=True)
        else:
            # Search for text nodes that look like addresses near 'info'
            info_sections = soup.find_all(string=re.compile(r"info", re.I))
            for section in info_sections:
                parent = section.parent
                if parent:
                    # Look at siblings or parent's parent
                    text = parent.get_text(strip=True)
                    if len(text) > 10 and any(c.isdigit() for c in text):
                        res["address"] = text
                        break

    return res

def parse_justeattakeaway_detail(html: str, market: str) -> dict[str, Any]:
    """Parse JET detail page for extra info like address."""
    soup = BeautifulSoup(html, "html.parser")
    res = _parse_ld_json(soup)
    if res.get("address") and res.get("lat"):
        return res

    # Try __NEXT_DATA__
    stag = soup.find("script", id="__NEXT_DATA__")
    if stag and stag.string:
        try:
            data = json.loads(stag.string)
            # Drill down into common Next.js state paths for JET
            props = data.get("props", {}).get("pageProps", {})
            restaurant = props.get("restaurant") or props.get("initialState", {}).get("restaurant", {}).get("restaurant", {})

            if restaurant:
                loc = restaurant.get("location", {})
                street = loc.get("streetName", "")
                number = loc.get("streetNumber", "")
                zip_code = loc.get("postalCode", "")
                city = loc.get("city", "")

                parts = []
                if street and number:
                    parts.append(f"{street} {number}")
                elif street:
                    parts.append(street)

                if zip_code:
                    parts.append(zip_code)
                if city:
                    parts.append(city)

                if parts:
                    res["address"] = ", ".join(parts)
                else:
                    res["address"] = street # Fallback

                res["lat"] = _safe_float(loc.get("lat") or loc.get("latitude"))
                res["lng"] = _safe_float(loc.get("lng") or loc.get("longitude"))
        except Exception:
            pass

    # Fallback to DOM
    if not res.get("address"):
        details = soup.select_one('[data-testid="business-details-content"], section:has(h2:contains("details"))')
        if details:
            # Usually address is in the first few paragraphs
            ps = details.find_all("p")
            if ps:
                res["address"] = ", ".join([p.get_text(strip=True) for p in ps[:3]])

    return res

def parse_detail(platform: str, html: str, market: str) -> dict[str, Any]:
    """Dispatch to the appropriate platform detail parser."""
    parser = DETAIL_PARSERS.get(platform)
    if not parser:
        logger.error("No detail parser found for platform: %s", platform)
        return {}
    return parser(html, market)

DETAIL_PARSERS = {
    "ubereats": parse_ubereats_detail,
    "justeattakeaway": parse_justeattakeaway_detail,
}
