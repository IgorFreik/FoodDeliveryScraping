#!/usr/bin/env python3
"""
Integration example: 1 Uber Eats vs all JET, and 1 JET vs all Uber Eats.

Uses raw HTML from local MinIO. Requires MinIO running with scraped data.
Run from project root:
    python examples/run_matching_simple.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from processing.entity_resolution import (
    add_row_confidence_to_target,
    extract_address_fields,
    extract_restaurant_name,
    merge_matches_to_entities,
    rule_based_match,
)
from processing.normalizer import normalize_listing
from processing.parser import parse_listing
from storage.minio_client import RAW_BUCKET, download_raw_html, list_keys


def load_listings_from_minio(platform: str, market: str = "amsterdam") -> list:
    """Load and parse all listing HTML from MinIO for a platform/market."""
    prefix = f"{platform}/"
    keys = list_keys(RAW_BUCKET, prefix=prefix)
    listing_keys = [
        k for k in keys
        if k.endswith(".html") and f"/{market}/" in k and "listing" in k
    ]
    if not listing_keys:
        return []

    seen_ids: set[str] = set()
    all_listings = []
    for key in sorted(listing_keys):
        html = download_raw_html(key)
        listings = parse_listing(html, platform, market)
        for listing in listings:
            if listing.platform_merchant_id in seen_ids:
                continue
            seen_ids.add(listing.platform_merchant_id)
            listing = normalize_listing(listing)
            all_listings.append(listing)
    return all_listings


def listings_to_df(listings: list, start_id: int = 0) -> pd.DataFrame:
    """Convert MerchantListing list to DataFrame for rule_based_match."""
    rows = []
    for i, L in enumerate(listings):
        rows.append({
            "id": start_id + i,
            "platform": L.platform,
            "name": L.name or "",
            "address": L.address or "",
            "market": L.market,
        })
    return pd.DataFrame(rows)


def run_test(name: str, df: pd.DataFrame, target_row_index: int = 0):
    """Run matching and print results."""
    target_row = df.iloc[target_row_index]
    df_with_conf = add_row_confidence_to_target(df, target_row)
    df_with_conf = df_with_conf.sort_values(by="confidence", ascending=False)

    matches = rule_based_match(df)
    matches = sorted(matches, key=lambda m: m.confidence, reverse=True)

    print(f"\n{'='*60}")
    print(f"TEST: {name}")
    print("=" * 60)
    MAX_PRINTED_ROWS = 10
    print("Input (rows sorted by similarity to target desc):")
    for i, (_, row) in enumerate(df_with_conf.iterrows()):
        if i >= MAX_PRINTED_ROWS:
            print(f"... (only showing first {MAX_PRINTED_ROWS} rows)")
            break
        print(f"  [{row['platform']}] id={row['id']} conf={row['confidence']:.2f}: {row['name']!r} @ {row['address']!r}")
    print()

    print(f"Matches found: {len(matches)}")
    for m in matches:
        print(f"  -> {m.name_a!r} (id={m.platform_merchant_id_a}) <-> {m.name_b!r} (id={m.platform_merchant_id_b})")
        print(f"     confidence={m.confidence:.2f}, name_sim={m.name_similarity:.2f}")

    return matches


def main():
    market = "amsterdam"

    print("Loading Uber Eats listings from MinIO...")
    uber_listings = load_listings_from_minio("ubereats", market)
    print(f"  Found {len(uber_listings)} Uber Eats listings")

    print("Loading Just Eat Takeaway listings from MinIO...")
    jet_listings = load_listings_from_minio("justeattakeaway", market)
    print(f"  Found {len(jet_listings)} JET listings")

    if not uber_listings:
        print("No Uber Eats data in MinIO. Run scrape_ubereats DAG or scripts/run_local_ubereats.py first.")
        return
    if not jet_listings:
        print("No JET data in MinIO. Run scrape_justeattakeaway DAG or scripts/run_local_justeattakeaway.py first.")
        return

    uber_df = listings_to_df(uber_listings, start_id=1000)
    jet_df = listings_to_df(jet_listings, start_id=2000)

    one_uber = uber_df.sample(1)
    df_1_uber_vs_jet = pd.concat([one_uber, jet_df], ignore_index=True)
    run_test("1 Uber Eats vs all JET", df_1_uber_vs_jet)

    one_jet = jet_df.sample(1)
    df_1_jet_vs_uber = pd.concat([one_jet, uber_df], ignore_index=True)
    run_test("2 JET vs all Uber Eats", df_1_jet_vs_uber)

    print(f"\n{'='*60}")
    print("FULL MATCH SUMMARY (all Uber vs all JET)")
    print("=" * 60)
    full_df = pd.concat([uber_df, jet_df], ignore_index=True)
    full_matches = rule_based_match(full_df)
    full_clusters = merge_matches_to_entities(full_matches)
    matched_ids = set()
    for m in full_matches:
        matched_ids.add(m.platform_merchant_id_a)
        matched_ids.add(m.platform_merchant_id_b)
    print(f"  Input: {len(uber_df)} Uber + {len(jet_df)} JET = {len(full_df)} listings")
    print(f"  Match pairs: {len(full_matches)}")
    print(f"  Unique listings in matches: {len(matched_ids)}")
    print(f"  Entity clusters: {len(full_clusters)}")
    print(f"  Total matched listings (in clusters): {sum(len(v) for v in full_clusters.values())}")

    print(f"\n{'='*60}")
    print("EXTRACTED FIELDS (sample)")
    print("=" * 60)
    for listing in [uber_listings[0], jet_listings[0]]:
        n = extract_restaurant_name(listing.name or "")
        a = extract_address_fields(listing.address or "")
        print(f"  [{listing.platform}] {listing.name!r}")
        print(f"    name -> {n!r}")
        print(f"    addr -> num={a.street_number!r} street={a.street_name!r} city={a.city!r}")

    print("\nDone.")


if __name__ == "__main__":
    main()
