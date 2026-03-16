"""
Unit tests for the entity resolution module.

Tests:
- Name similarity (rapidfuzz)
- Geo distance computation
- Rule-based matching logic
- Match scoring
- Union-Find entity clustering
"""

from __future__ import annotations

import pandas as pd
import pytest

from processing.entity_resolution import (
    compute_name_similarity,
    compute_geo_distance,
    rule_based_match,
    merge_matches_to_entities,
    _score_match,
    MatchCandidate,
)


# ── Name Similarity ─────────────────────────────────────────────────


class TestNameSimilarity:
    def test_identical_names(self):
        score = compute_name_similarity("Joe's Pizza", "Joe's Pizza")
        assert score == 1.0

    def test_similar_names(self):
        score = compute_name_similarity("Joe's Pizza", "Joe's Famous Pizza")
        assert score >= 0.8

    def test_different_names(self):
        score = compute_name_similarity("Joe's Pizza", "Thai Garden")
        assert score < 0.5

    def test_case_insensitive(self):
        score = compute_name_similarity("JOE'S PIZZA", "joe's pizza")
        assert score == 1.0


# ── Geo Distance ────────────────────────────────────────────────────


class TestGeoDistance:
    def test_same_point(self):
        dist = compute_geo_distance(40.7128, -74.0060, 40.7128, -74.0060)
        assert dist == 0.0

    def test_nearby_points(self):
        # ~100m apart in NYC
        dist = compute_geo_distance(40.7128, -74.0060, 40.7137, -74.0060)
        assert dist is not None
        assert dist < 200  # Should be ~100m

    def test_none_coords(self):
        dist = compute_geo_distance(40.7128, -74.0060, None, None)
        assert dist is None


# ── Match Scoring ───────────────────────────────────────────────────


class TestScoreMatch:
    def test_perfect_match(self):
        score = _score_match(name_sim=1.0, geo_dist=0.0, address_match=True)
        assert score == 1.0

    def test_name_only(self):
        score = _score_match(name_sim=0.9, geo_dist=None, address_match=False)
        assert score == 0.9

    def test_geo_penalty(self):
        close = _score_match(name_sim=0.9, geo_dist=10.0, address_match=False)
        far = _score_match(name_sim=0.9, geo_dist=140.0, address_match=False)
        assert close > far


# ── Rule-Based Match ────────────────────────────────────────────────


class TestRuleBasedMatch:
    def test_matches_across_platforms(self):
        df = pd.DataFrame([
            {"id": 1, "platform": "doordash", "name": "Joe's Pizza", "address": "123 Main St", "lat": 40.71, "lng": -74.00, "market": "nyc"},
            {"id": 2, "platform": "grubhub", "name": "Joe's Famous Pizza", "address": "123 Main Street", "lat": 40.71, "lng": -74.00, "market": "nyc"},
        ])

        matches = rule_based_match(df)
        assert len(matches) >= 1
        assert matches[0].name_similarity >= 0.7

    def test_no_match_different_markets(self):
        df = pd.DataFrame([
            {"id": 1, "platform": "doordash", "name": "Joe's Pizza", "address": "", "lat": None, "lng": None, "market": "nyc"},
            {"id": 2, "platform": "grubhub", "name": "Joe's Pizza", "address": "", "lat": None, "lng": None, "market": "la"},
        ])

        matches = rule_based_match(df)
        assert len(matches) == 0  # Different markets should not match

    def test_no_match_same_platform(self):
        df = pd.DataFrame([
            {"id": 1, "platform": "doordash", "name": "Joe's Pizza", "address": "", "lat": None, "lng": None, "market": "nyc"},
            {"id": 2, "platform": "doordash", "name": "Joe's Pizza Copy", "address": "", "lat": None, "lng": None, "market": "nyc"},
        ])

        matches = rule_based_match(df)
        assert len(matches) == 0  # Same platform should not match


# ── Entity Clustering ───────────────────────────────────────────────


class TestMergeEntities:
    def test_simple_cluster(self):
        matches = [
            MatchCandidate(1, 2, "A", "B", 0.9, None, False, 0.9, "test"),
            MatchCandidate(2, 3, "B", "C", 0.85, None, False, 0.85, "test"),
        ]

        clusters = merge_matches_to_entities(matches)
        # All three should be in the same cluster
        assert len(clusters) == 1
        cluster_members = list(clusters.values())[0]
        assert set(cluster_members) == {1, 2, 3}

    def test_separate_clusters(self):
        matches = [
            MatchCandidate(1, 2, "A", "B", 0.9, None, False, 0.9, "test"),
            MatchCandidate(3, 4, "C", "D", 0.85, None, False, 0.85, "test"),
        ]

        clusters = merge_matches_to_entities(matches)
        assert len(clusters) == 2

    def test_empty_matches(self):
        clusters = merge_matches_to_entities([])
        assert clusters == {}
