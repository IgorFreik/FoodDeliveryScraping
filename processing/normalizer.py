"""
Normalization utilities for cleaning scraped merchant data before storage.

Covers:
- Address standardization
- Cuisine tag taxonomy mapping
- Price bucket unification
- Name cleaning
"""

from __future__ import annotations

import re
import unicodedata

from processing.models import MerchantListing

# ── Cuisine Taxonomy ────────────────────────────────────────────────

# Map free-text cuisine labels to canonical categories.
CUISINE_TAXONOMY: dict[str, str] = {
    # American
    "american": "American",
    "burgers": "American",
    "hot dogs": "American",
    "bbq": "BBQ",
    "barbecue": "BBQ",
    # Asian
    "chinese": "Chinese",
    "szechuan": "Chinese",
    "cantonese": "Chinese",
    "japanese": "Japanese",
    "sushi": "Japanese",
    "ramen": "Japanese",
    "korean": "Korean",
    "thai": "Thai",
    "vietnamese": "Vietnamese",
    "pho": "Vietnamese",
    "indian": "Indian",
    "curry": "Indian",
    # European
    "italian": "Italian",
    "pizza": "Pizza",
    "pasta": "Italian",
    "french": "French",
    "greek": "Greek",
    "mediterranean": "Mediterranean",
    "spanish": "Spanish",
    # Latin
    "mexican": "Mexican",
    "tacos": "Mexican",
    "burritos": "Mexican",
    "latin": "Latin American",
    "caribbean": "Caribbean",
    # Other
    "seafood": "Seafood",
    "vegetarian": "Vegetarian",
    "vegan": "Vegan",
    "halal": "Halal",
    "kosher": "Kosher",
    "breakfast": "Breakfast",
    "brunch": "Breakfast",
    "desserts": "Desserts",
    "bakery": "Bakery",
    "cafe": "Cafe",
    "coffee": "Cafe",
    "healthy": "Healthy",
    "salads": "Healthy",
    "sandwiches": "Sandwiches",
    "subs": "Sandwiches",
    "wings": "Wings",
    "chicken": "Chicken",
    "fast food": "Fast Food",
}


# ── Price Bucket Normalization ──────────────────────────────────────

_PRICE_MAP = {
    "$": "$",
    "1": "$",
    "low": "$",
    "$$": "$$",
    "2": "$$",
    "mid": "$$",
    "medium": "$$",
    "$$$": "$$$",
    "3": "$$$",
    "high": "$$$",
    "$$$$": "$$$$",
    "4": "$$$$",
    "expensive": "$$$$",
}


def normalize_listing(listing: MerchantListing) -> MerchantListing:
    """
    Apply all normalization steps to a ``MerchantListing`` in-place
    (returns the same object for chaining convenience).
    """
    listing.name = normalize_name(listing.name)
    listing.address = normalize_address(listing.address)
    listing.cuisine_tags = normalize_cuisine_tags(listing.cuisine_tags)
    listing.price_bucket = normalize_price_bucket(listing.price_bucket)
    return listing


# Common Amsterdam location suffixes used by Uber Eats
_LOCATION_SUFFIXES = [
    "amsterdam", "centrum", "oost", "west", "noord", "zuid",
    "de pijp", "jordaan", "westerpark", "zuidas", "ndsm",
    "diemen", "amstelveen", "badhoevedorp", "duivendrecht",
    "ijburg", "zuidoost", "bijlmerplein", "osdorp",
]


def normalize_name(name: str) -> str:
    """
    Clean merchant name for cross-platform matching:
    - Unicode normalization (NFC)
    - Strip platform-injected suffixes
    - Normalize separators (I, l, | → |)
    - Remove location/branch suffixes (e.g. "- Kinkerstraat", "Amsterdam")
    - Remove slogans (e.g. "De beste pizza van NL!")
    - Collapse whitespace and deduplicate words
    """
    name = unicodedata.normalize("NFC", name).strip()
    name = re.sub(r"\s+", " ", name)

    # Remove common platform-injected suffixes (incl. Thuisbezorgd/Just Eat)
    name = re.sub(
        r"\s*[-–—]\s*(order (online|now|delivery)|delivery|menu|doordash|grubhub|uber\s?eats|thuisbezorgd|just\s?eat).*$",
        "",
        name,
        flags=re.IGNORECASE,
    )
    # Remove parenthetical platform/location names
    name = re.sub(
        r"\s*\((doordash|grubhub|uber\s?eats|thuisbezorgd|just\s?eat|[^)]{1,30})\)\s*$",
        "",
        name,
        flags=re.IGNORECASE,
    )

    # Normalize pipe-like separators: " I " and " l " (standalone) → " | "
    # but only when surrounded by spaces (to avoid matching words like "Indian")
    name = re.sub(r" [Il] ", " | ", name)

    # Remove slogans after pipe (e.g. "Loulou | De beste pizza van NL! | Weesperzijde")
    # Keep the part before the first pipe as the canonical name
    if " | " in name:
        parts = [p.strip() for p in name.split(" | ")]
        # Filter out parts that look like slogans (>4 words) or location suffixes
        meaningful = []
        for p in parts:
            p_lower = p.lower()
            # Skip if it's a location suffix
            if p_lower in _LOCATION_SUFFIXES:
                continue
            # Skip if it looks like a slogan (contains common slogan words)
            if any(w in p_lower for w in ["beste", "best", "new!", "nieuw", "order"]):
                continue
            meaningful.append(p)
        name = meaningful[0] if meaningful else parts[0]

    # Remove trailing " - LocationName" branch suffix
    # e.g. "McDonald's - Kinkerstraat" → "McDonald's"
    name = re.sub(r"\s*[-–—]\s+[A-Z][\w\s'./-]*$", "", name)

    # Remove trailing city/neighborhood names (case-insensitive)
    for loc in _LOCATION_SUFFIXES:
        name = re.sub(rf"\s+{re.escape(loc)}\s*$", "", name, flags=re.IGNORECASE)

    # Remove duplicate consecutive words (e.g. "Sushi Su Amsterdam Amsterdam" → "Sushi Su")
    words = name.split()
    deduped = [words[0]] if words else []
    for w in words[1:]:
        if w.lower() != deduped[-1].lower():
            deduped.append(w)
    name = " ".join(deduped)

    # Final cleanup
    name = re.sub(r"\s+", " ", name).strip()
    return name


def normalize_address(address: str) -> str:
    """
    Standardize an address string:
    - Unicode NFC
    - Common abbreviations (St → Street, Ave → Avenue, etc.)
    - Collapse whitespace
    """
    if not address:
        return ""
    address = unicodedata.normalize("NFC", address).strip()
    address = re.sub(r"\s+", " ", address)

    # Expand common abbreviations (EN + NL for cross-platform matching)
    replacements = [
        (r"\bSt\b\.?", "Street"),
        (r"\bStraat\b", "Street"),
        (r"\bAve\b\.?", "Avenue"),
        (r"\bBlvd\b\.?", "Boulevard"),
        (r"\bDr\b\.?", "Drive"),
        (r"\bLn\b\.?", "Lane"),
        (r"\bLaan\b", "Lane"),
        (r"\bCt\b\.?", "Court"),
        (r"\bPl\b\.?", "Place"),
        (r"\bPlein\b", "Square"),
        (r"\bRd\b\.?", "Road"),
        (r"\bWeg\b", "Road"),
        (r"\bSte\b\.?", "Suite"),
        (r"\bApt\b\.?", "Apartment"),
        (r"\bFl\b\.?", "Floor"),
        # NL postal: normalize "1011 AA" vs "1011AA"
        (r"\b(\d{4})\s*([A-Z]{2})\b", r"\1 \2"),
    ]
    for pattern, repl in replacements:
        address = re.sub(pattern, repl, address, flags=re.IGNORECASE)

    return address


def normalize_cuisine_tags(tags: list[str]) -> list[str]:
    """Map free-text cuisine tags to the canonical taxonomy."""
    normalized: list[str] = []
    seen: set[str] = set()
    for tag in tags:
        canonical = CUISINE_TAXONOMY.get(tag.lower().strip())
        if canonical and canonical not in seen:
            normalized.append(canonical)
            seen.add(canonical)
        elif not canonical and tag.strip() and tag.strip() not in seen:
            # Keep unmapped tags as-is (title-cased)
            clean = tag.strip().title()
            normalized.append(clean)
            seen.add(clean)
    return normalized


def normalize_price_bucket(bucket: str | None) -> str | None:
    """Normalize price bucket string to $/$$/$$$/$$$$."""
    if not bucket:
        return None
    return _PRICE_MAP.get(bucket.strip().lower(), bucket.strip())
