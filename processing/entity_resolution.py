"""
Cross-platform entity resolution for merchant matching.

Uses Splink (with DuckDB backend) for probabilistic record linkage and
rapidfuzz for fuzzy string matching. Designed to match merchants across
platforms (e.g. "Joe's Pizza" on DoorDash with "Joe's Famous Pizza" on
Grubhub) into a single canonical entity.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import duckdb
import pandas as pd
from haversine import haversine, Unit
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)


# ── Configuration ───────────────────────────────────────────────────

# Thresholds for the rule-based fast path
NAME_SIMILARITY_THRESHOLD = 0.80     # Jaro-Winkler ratio (0–1)
GEO_DISTANCE_THRESHOLD_M = 150      # meters
HIGH_CONFIDENCE_THRESHOLD = 0.90


@dataclass
class MatchCandidate:
    """A potential match between two platform merchant records."""
    platform_merchant_id_a: int
    platform_merchant_id_b: int
    name_a: str
    name_b: str
    name_similarity: float
    geo_distance_m: Optional[float]
    address_match: bool
    confidence: float
    match_method: str


# ── Rule-Based Fast Path ───────────────────────────────────────────


def compute_name_similarity(name_a: str, name_b: str) -> float:
    """Jaro-Winkler similarity between two merchant names."""
    return fuzz.WRatio(name_a.lower(), name_b.lower()) / 100.0


def compute_geo_distance(
    lat_a: float | None, lng_a: float | None,
    lat_b: float | None, lng_b: float | None,
) -> float | None:
    """
    Haversine distance in meters between two points.
    Returns None if either coordinate pair is missing.
    """
    if any(v is None for v in (lat_a, lng_a, lat_b, lng_b)):
        return None
    return haversine((lat_a, lng_a), (lat_b, lng_b), unit=Unit.METERS)


def rule_based_match(
    merchants_df: pd.DataFrame,
) -> list[MatchCandidate]:
    """
    Fast rule-based matching:
    1. Block by market (same zip/market only)
    2. For each cross-platform pair, compute name similarity + geo distance
    3. Accept high-confidence matches; queue ambiguous ones

    ``merchants_df`` must have columns:
        id, platform, name, address, lat, lng, market
    """
    matches: list[MatchCandidate] = []

    for market, group in merchants_df.groupby("market"):
        platforms = group["platform"].unique()
        if len(platforms) < 2:
            continue

        # Only compare across different platforms
        for i, row_a in group.iterrows():
            for j, row_b in group.iterrows():
                if row_a["platform"] >= row_b["platform"]:
                    continue  # Avoid duplicate pairs and same-platform

                name_sim = compute_name_similarity(row_a["name"], row_b["name"])
                if name_sim < 0.6:
                    continue  # Skip obviously different names

                geo_dist = compute_geo_distance(
                    row_a.get("lat"), row_a.get("lng"),
                    row_b.get("lat"), row_b.get("lng"),
                )

                address_match = (
                    bool(row_a.get("address")) and
                    bool(row_b.get("address")) and
                    fuzz.ratio(
                        str(row_a["address"]).lower(),
                        str(row_b["address"]).lower(),
                    ) > 85
                )

                # Score the match
                confidence = _score_match(name_sim, geo_dist, address_match)

                # Accept matches with reasonable confidence.
                # Threshold is lower than NAME_SIMILARITY_THRESHOLD because
                # the composite score includes geo + address signals.
                if confidence >= 0.70:
                    matches.append(
                        MatchCandidate(
                            platform_merchant_id_a=int(row_a["id"]),
                            platform_merchant_id_b=int(row_b["id"]),
                            name_a=row_a["name"],
                            name_b=row_b["name"],
                            name_similarity=name_sim,
                            geo_distance_m=geo_dist,
                            address_match=address_match,
                            confidence=confidence,
                            match_method="rule_based",
                        )
                    )

    logger.info("Rule-based matching found %d candidate pairs.", len(matches))
    return matches


def _score_match(
    name_sim: float,
    geo_dist: float | None,
    address_match: bool,
) -> float:
    """
    Weighted scoring for a candidate match.

    Base weights:
    - Name similarity: 50%
    - Geo proximity:   30%
    - Address match:   20%

    When geo or address signals are unavailable, their weight is
    redistributed proportionally to the available signals.
    """
    signals: list[tuple[float, float]] = []  # (weight, score)
    signals.append((0.50, name_sim))

    if geo_dist is not None:
        if geo_dist < 50:
            geo_score = 1.0
        elif geo_dist < GEO_DISTANCE_THRESHOLD_M:
            geo_score = 1.0 - (geo_dist / GEO_DISTANCE_THRESHOLD_M)
        else:
            geo_score = 0.0
        signals.append((0.30, geo_score))

    if address_match is not None and (geo_dist is not None or address_match):
        # Only count address signal if we actually have address data
        signals.append((0.20, 1.0 if address_match else 0.0))

    # Redistribute: normalize weights to sum to 1.0
    total_weight = sum(w for w, _ in signals)
    if total_weight == 0:
        return 0.0
    return sum((w / total_weight) * s for w, s in signals)


# ── Splink Probabilistic Matching ──────────────────────────────────


def splink_match(merchants_df: pd.DataFrame) -> pd.DataFrame:
    """
    Run Splink probabilistic record linkage using DuckDB backend.

    Returns a DataFrame of matched pairs with columns:
        id_l, id_r, match_probability
    """
    try:
        import splink.duckdb.linker as duckdb_linker
        from splink.duckdb.linker import DuckDBLinker
    except ImportError:
        # Splink 4.x uses a different import structure
        from splink import Linker, DuckDBAPI, SettingsCreator, block_on
        import splink.comparison_library as cl

        db_api = DuckDBAPI()

        settings = SettingsCreator(
            link_type="link_only",
            comparisons=[
                cl.JaroWinklerAtThresholds("name", score_threshold_or_thresholds=[0.9, 0.7]),
                cl.LevenshteinAtThresholds("address", distance_threshold_or_thresholds=[3, 5]),
            ],
            blocking_rules_to_generate_predictions=[
                block_on("market"),
            ],
        )

        # Prepare: split by platform for link_only mode
        platforms = merchants_df["platform"].unique()
        if len(platforms) < 2:
            logger.warning("Need ≥2 platforms for Splink matching, got %d", len(platforms))
            return pd.DataFrame(columns=["id_l", "id_r", "match_probability"])

        frames = {
            p: merchants_df[merchants_df["platform"] == p].reset_index(drop=True)
            for p in platforms
        }

        linker = Linker(
            list(frames.values()),
            settings,
            db_api=db_api,
        )

        linker.training.estimate_u_using_random_sampling(max_pairs=5_000_000)

        for col in ["name", "address"]:
            try:
                linker.training.estimate_parameters_using_expectation_maximisation(
                    block_on(col), estimate_without_term_frequencies=True
                )
            except Exception as exc:
                logger.warning("EM training on '%s' failed: %s", col, exc)

        predictions = linker.inference.predict(threshold_match_probability=0.6)
        results_df = predictions.as_pandas_dataframe()

        return results_df[["unique_id_l", "unique_id_r", "match_probability"]].rename(
            columns={"unique_id_l": "id_l", "unique_id_r": "id_r"}
        )

    except Exception as exc:
        logger.error("Splink matching failed: %s", exc, exc_info=True)
        return pd.DataFrame(columns=["id_l", "id_r", "match_probability"])


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
