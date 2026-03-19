"""
Cross-platform entity resolution for merchant matching.

Simple approach: extract restaurant name, street number, street name, and city
from each record, then apply fuzzy matching on those fields.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

import pandas as pd
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)


# ── Configuration ───────────────────────────────────────────────────

FUZZ_THRESHOLD = 80  # Min rapidfuzz ratio (0–100) for each field to match


@dataclass
class MatchCandidate:
    """A potential match between two platform merchant records."""
    platform_merchant_id_a: int
    platform_merchant_id_b: int
    name_a: str
    name_b: str
    name_similarity: float
    geo_distance_m: None
    address_match: bool
    confidence: float
    match_method: str


@dataclass
class ExtractedFields:
    """Parsed fields from a merchant record."""
    name: str
    street_number: str
    street_name: str
    city: str


# ── Extraction ───────────────────────────────────────────────────────


def _normalize(s: str) -> str:
    """Lowercase, collapse whitespace."""
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def extract_restaurant_name(name: str) -> str:
    """Clean restaurant name: strip location suffixes, platform tags, etc."""
    from processing.normalizer import normalize_name

    return _normalize(normalize_name(name or ""))


def extract_address_fields(address: str) -> ExtractedFields:
    """
    Parse address into street_number, street_name, city.
    Handles NL format: "Overtoom 123, 1054 Amsterdam" or "123 Overtoom, 1011 AA Amsterdam"
    and generic: "123 Main St, Amsterdam".
    """
    from processing.normalizer import normalize_address

    addr = _normalize(normalize_address(address or ""))
    street_number = ""
    street_name = ""
    city = ""

    if not addr:
        return ExtractedFields(name="", street_number="", street_name="", city="")

    # NL postal: 4 digits + optional 2 letters (1011, 1011 AA)
    postal_match = re.search(r"\b(\d{4})\s*([A-Z]{2})?\b", addr, re.I)
    if postal_match:
        # City is typically after postal code
        after_postal = addr[postal_match.end() :].strip(" ,")
        city_parts = after_postal.split(",")
        if city_parts:
            city = city_parts[0].strip()
        before_postal = addr[: postal_match.start()].strip(" ,")
    else:
        # No postal: split on comma, last part often city
        parts = [p.strip() for p in addr.split(",")]
        if len(parts) >= 2:
            city = parts[-1]
            before_postal = ",".join(parts[:-1])
        else:
            before_postal = addr
            city = ""

    # Extract street number (first standalone number)
    num_match = re.search(r"\b(\d+[a-zA-Z]?)\b", before_postal)
    if num_match:
        street_number = num_match.group(1).lower()
        street_name = (
            before_postal[: num_match.start()].strip() + " " + before_postal[num_match.end() :].strip()
        ).strip()
    else:
        street_name = before_postal

    street_name = re.sub(r"\s+", " ", street_name).strip(" ,-")
    return ExtractedFields(
        name="",
        street_number=street_number,
        street_name=street_name,
        city=city,
    )


def _fuzz_score(a: str, b: str) -> int:
    """Return 0–100 similarity; 100 if both empty."""
    if not a and not b:
        return 100
    if not a or not b:
        return 0
    return max(fuzz.ratio(a, b), fuzz.token_set_ratio(a, b))


# ── Matching ────────────────────────────────────────────────────────


def rule_based_match(merchants_df: pd.DataFrame) -> list[MatchCandidate]:
    """
    Simple matching: extract name, street_number, street_name, city;
    compare cross-platform pairs with fuzzy logic on each field.
    Match when all non-empty fields score above FUZZ_THRESHOLD.
    """
    df = merchants_df.copy()
    if df["market"].isna().any():
        mode_val = df["market"].mode()
        fill_val = mode_val.iloc[0] if len(mode_val) > 0 else "unknown"
        df["market"] = df["market"].fillna(fill_val)

    # Pre-extract fields for each row
    def extract_row(row) -> ExtractedFields:
        name = extract_restaurant_name(getattr(row, "name", "") or "")
        addr = extract_address_fields(str(getattr(row, "address", "") or ""))
        return ExtractedFields(
            name=name,
            street_number=addr.street_number,
            street_name=addr.street_name,
            city=addr.city,
        )

    matches: list[MatchCandidate] = []

    for market, group in df.groupby("market"):
        platforms = group["platform"].unique()
        if len(platforms) < 2:
            continue

        records = list(group.itertuples(index=False))
        for i, row_a in enumerate(records):
            fields_a = extract_row(row_a)
            for j, row_b in enumerate(records):
                if i >= j or row_a.platform == row_b.platform:
                    continue

                fields_b = extract_row(row_b)

                # Fuzzy score each field (0–100)
                name_score = _fuzz_score(fields_a.name, fields_b.name)
                num_score = _fuzz_score(fields_a.street_number, fields_b.street_number)
                street_score = _fuzz_score(fields_a.street_name, fields_b.street_name)
                city_score = _fuzz_score(fields_a.city, fields_b.city)

                # Require name to match (restaurant identity)
                if name_score < FUZZ_THRESHOLD:
                    continue

                # For address fields: if both have data, they must match; if one empty, skip that field
                addr_ok = True
                if fields_a.street_number and fields_b.street_number:
                    addr_ok = addr_ok and (num_score >= FUZZ_THRESHOLD)
                if fields_a.street_name and fields_b.street_name:
                    addr_ok = addr_ok and (street_score >= FUZZ_THRESHOLD)
                if fields_a.city and fields_b.city:
                    addr_ok = addr_ok and (city_score >= FUZZ_THRESHOLD)

                if not addr_ok:
                    continue

                # Compute overall confidence (average of non-empty field scores)
                scores = [name_score]
                if fields_a.street_number and fields_b.street_number:
                    scores.append(num_score)
                if fields_a.street_name and fields_b.street_name:
                    scores.append(street_score)
                if fields_a.city and fields_b.city:
                    scores.append(city_score)
                confidence = sum(scores) / len(scores) / 100.0

                matches.append(
                    MatchCandidate(
                        platform_merchant_id_a=int(getattr(row_a, "id")),
                        platform_merchant_id_b=int(getattr(row_b, "id")),
                        name_a=getattr(row_a, "name", ""),
                        name_b=getattr(row_b, "name", ""),
                        name_similarity=name_score / 100.0,
                        geo_distance_m=None,
                        address_match=addr_ok,
                        confidence=confidence,
                        match_method="field_fuzzy",
                    )
                )

    logger.info("Rule-based matching found %d candidate pairs.", len(matches))
    return matches


def _compute_pair_similarity(fields_a: ExtractedFields, fields_b: ExtractedFields) -> float:
    """
    Compute similarity (0–1) between two extracted records.
    Uses same logic as rule_based_match but always returns the score.
    """
    name_score = _fuzz_score(fields_a.name, fields_b.name)
    num_score = _fuzz_score(fields_a.street_number, fields_b.street_number)
    street_score = _fuzz_score(fields_a.street_name, fields_b.street_name)
    city_score = _fuzz_score(fields_a.city, fields_b.city)

    scores = [name_score]
    if fields_a.street_number and fields_b.street_number:
        scores.append(num_score)
    if fields_a.street_name and fields_b.street_name:
        scores.append(street_score)
    if fields_a.city and fields_b.city:
        scores.append(city_score)
    return sum(scores) / len(scores) / 100.0


def _row_val(row, key: str, default=None):
    """Get value from row (Series or named tuple)."""
    try:
        return row[key]
    except (TypeError, KeyError):
        return getattr(row, key, default)


def add_row_confidence_to_target(
    merchants_df: pd.DataFrame,
    target_row,
) -> pd.DataFrame:
    """
    Add a ``confidence`` column: for each row, the similarity (0–1) between
    that row and the target row. Target row gets 1.0. Rows from the same
    platform as target get 0.0 (not compared).
    """
    def extract_row(row) -> ExtractedFields:
        name = extract_restaurant_name(str(_row_val(row, "name") or ""))
        addr = extract_address_fields(str(_row_val(row, "address") or ""))
        return ExtractedFields(
            name=name,
            street_number=addr.street_number,
            street_name=addr.street_name,
            city=addr.city,
        )

    target_id = int(_row_val(target_row, "id", 0))
    target_platform = _row_val(target_row, "platform", "")
    target_fields = extract_row(target_row)

    df = merchants_df.copy()
    confs = []
    for row in df.itertuples(index=False):
        if _row_val(row, "id") == target_id:
            confs.append(1.0)
        elif _row_val(row, "platform") == target_platform:
            confs.append(0.0)
        else:
            row_fields = extract_row(row)
            confs.append(_compute_pair_similarity(target_fields, row_fields))
    df["confidence"] = confs
    return df


# ── Merge Logic ─────────────────────────────────────────────────────


def merge_matches_to_entities(
    matches: list[MatchCandidate],
) -> dict[int, list[int]]:
    """
    Given a list of pairwise matches, cluster them into entity groups
    using Union-Find.

    Returns a dict mapping entity_id → [platform_merchant_id, ...].
    """
    parent: dict[int, int] = {}

    def find(x: int) -> int:
        while parent.get(x, x) != x:
            parent[x] = parent.get(parent[x], parent[x])
            x = parent[x]
        return x

    def union(a: int, b: int):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for m in matches:
        parent.setdefault(m.platform_merchant_id_a, m.platform_merchant_id_a)
        parent.setdefault(m.platform_merchant_id_b, m.platform_merchant_id_b)
        union(m.platform_merchant_id_a, m.platform_merchant_id_b)

    # Group by root
    clusters: dict[int, list[int]] = {}
    for node in parent:
        root = find(node)
        clusters.setdefault(root, []).append(node)

    return clusters


# ── Full Pipeline (DB load → match → persist) ──────────────────────────


def run_resolve_entities() -> None:
    """
    Full entity resolution pipeline: load platform_merchants from DB,
    run rule-based matching, cluster, and persist to merchants + merchant_matches.

    Used by both the Airflow DAG and local test scripts.
    """
    from storage.db import Merchant, MerchantMatch, engine, get_session

    session = get_session()

    query = """
        SELECT
            id,
            platform,
            name,
            address,
            ST_Y(geom::geometry) AS lat,
            ST_X(geom::geometry) AS lng,
            market
        FROM platform_merchants
        WHERE name IS NOT NULL
    """

    try:
        conn = engine.raw_connection()
        try:
            df = pd.read_sql(query, conn)
        finally:
            conn.close()
    except Exception as e:
        logger.error("Failed to read from database: %s", e)
        raise

    if df.empty:
        logger.info("No merchants to resolve.")
        return

    total_input = len(df)
    platform_counts = df.groupby("platform").size().to_dict()
    logger.info(
        "Running entity resolution on %d merchants (platforms: %s)",
        total_input,
        platform_counts,
    )

    matches = rule_based_match(df)
    num_pairs = len(matches)
    matched_ids = set()
    for m in matches:
        matched_ids.add(m.platform_merchant_id_a)
        matched_ids.add(m.platform_merchant_id_b)
    num_matched_listings = len(matched_ids)

    logger.info(
        "Rule-based matching: %d pairs, %d unique listings matched",
        num_pairs,
        num_matched_listings,
    )

    clusters = merge_matches_to_entities(matches)
    total_in_clusters = sum(len(v) for v in clusters.values())
    logger.info(
        "Merged into %d entity clusters (%d total matched listings)",
        len(clusters),
        total_in_clusters,
    )

    match_lookup = {
        (m.platform_merchant_id_a, m.platform_merchant_id_b): m
        for m in matches
    }

    for entity_root, member_ids in clusters.items():
        members = df[df["id"].isin(member_ids)]
        canonical_row = members.iloc[0]
        is_on_our_platform = (members["platform"] == "ubereats").any()

        merchant = Merchant(
            canonical_name=canonical_row["name"],
            canonical_addr=canonical_row.get("address", ""),
            is_on_our_platform=is_on_our_platform,
        )
        if canonical_row.get("lat") is not None and canonical_row.get("lng") is not None:
            merchant.canonical_geom = (
                f"SRID=4326;POINT({canonical_row['lng']} {canonical_row['lat']})"
            )

        session.add(merchant)
        session.flush()

        for pm_id in member_ids:
            confidence = 1.0
            method = "cluster_member"
            for pair_key in [(pm_id, entity_root), (entity_root, pm_id)]:
                if pair_key in match_lookup:
                    confidence = match_lookup[pair_key].confidence
                    method = match_lookup[pair_key].match_method
                    break

            mm = MerchantMatch(
                platform_merchant_id=pm_id,
                merchant_id=merchant.id,
                confidence=confidence,
                match_method=method,
            )
            session.merge(mm)

    try:
        session.commit()
    except Exception as e:
        logger.error("Failed to commit resolution results: %s", e)
        session.rollback()
        raise
    finally:
        session.close()

    logger.info(
        "Entity resolution complete: %d entities, %d matched listings persisted",
        len(clusters),
        total_in_clusters,
    )
