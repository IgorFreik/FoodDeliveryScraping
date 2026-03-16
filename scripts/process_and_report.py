import json
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from sqlalchemy import text

from storage.db import get_session, engine, PlatformMerchant, Merchant, MerchantMatch
from processing.entity_resolution import (
    rule_based_match,
    merge_matches_to_entities,
)
from analytics.coverage_report import generate_coverage_report, print_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

INPUT_FILES = [
    PROJECT_ROOT / "local_storage/scraped_results_justeattakeaway_amsterdam.json",
    PROJECT_ROOT / "local_storage/scraped_results_ubereats_amsterdam.json",
]
# Fallback to old combined file
INPUT_FILE_LEGACY = PROJECT_ROOT / "local_storage/scraped_results_amsterdam.json"
REPORT_OUTPUT = PROJECT_ROOT / "local_storage/coverage_report_amsterdam.json"

def ingest_data(data):
    """Upsert scraped data into the platform_merchants table."""
    session = get_session()
    logger.info("Ingesting %d listings into the database...", len(data))
    
    try:
        for item in data:
            # Check if exists first to avoid UniqueViolation on (platform, platform_id)
            existing = session.query(PlatformMerchant).filter_by(
                platform=item["platform"],
                platform_id=item["platform_merchant_id"]
            ).first()
            
            if existing:
                pm = existing
                pm.name = item["name"]
                pm.address = item["address"]
                pm.cuisine_tags = item.get("cuisine_tags")
                pm.rating = item.get("rating")
                pm.review_count = item.get("review_count")
                pm.price_bucket = item.get("price_bucket")
                pm.delivery_fee = item.get("delivery_fee")
                pm.estimated_delivery_min = item.get("estimated_delivery_min")
                pm.market = item["market"]
                pm.raw_url = item.get("raw_url")
            else:
                pm = PlatformMerchant(
                    platform=item["platform"],
                    platform_id=item["platform_merchant_id"],
                    name=item["name"],
                    address=item["address"],
                    cuisine_tags=item.get("cuisine_tags"),
                    rating=item.get("rating"),
                    review_count=item.get("review_count"),
                    price_bucket=item.get("price_bucket"),
                    delivery_fee=item.get("delivery_fee"),
                    estimated_delivery_min=item.get("estimated_delivery_min"),
                    is_promoted=item.get("is_promoted", False),
                    market=item["market"],
                    raw_url=item.get("raw_url"),
                )
            
            # Handle geometry if coordinates exist
            lat, lng = item.get("lat"), item.get("lng")
            if lat and lng:
                pm.geom = f"SRID=4326;POINT({lng} {lat})"
            
            if not existing:
                session.add(pm)
        
        session.commit()
        logger.info("Data ingestion complete.")
    except Exception as e:
        session.rollback()
        logger.error(f"Ingestion failed: {e}")
        raise
    finally:
        session.close()

def resolve_entities():
    """Run cross-platform entity resolution."""
    logger.info("Running entity resolution...")
    session = get_session()
    
    # Load platform merchants into a DataFrame
    query = text("""
        SELECT
            id,
            platform,
            name,
            address,
            ST_Y(geom::geometry) AS lat,
            ST_X(geom::geometry) AS lng,
            market
        FROM platform_merchants
        WHERE market = 'amsterdam'
    """)

    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()
        df = pd.DataFrame(rows, columns=result.keys())

    if df.empty:
        logger.warning("No merchants found in Amsterdam to resolve.")
        return

    # Step 1: Rule-based matching
    matches = rule_based_match(df)
    
    # Step 2: Cluster into entities
    clusters = merge_matches_to_entities(matches)
    
    # Step 3: Persistence (simplistic version of DAG logic)
    # Clear old results for this market for a clean report
    session.execute(text("DELETE FROM merchant_matches WHERE platform_merchant_id IN (SELECT id FROM platform_merchants WHERE market='amsterdam')"))
    session.execute(text("DELETE FROM merchants WHERE id NOT IN (SELECT merchant_id FROM merchant_matches)"))
    for _, member_ids in clusters.items():
        # Just create a new canonical merchant for each cluster
        # In production, we'd be more careful about updating existing ones
        members = df[df["id"].isin(member_ids)]
        canonical_row = members.iloc[0]

        # Flag as being on "our platform" if any member is from Uber Eats
        is_on_uber = any(m["platform"] == "ubereats" for _, m in members.iterrows())

        merchant = Merchant(
            canonical_name=canonical_row["name"],
            canonical_addr=canonical_row.get("address", ""),
            is_on_our_platform=is_on_uber,
        )
        if canonical_row.get("lat") and canonical_row.get("lng"):
            merchant.canonical_geom = f"SRID=4326;POINT({canonical_row['lng']} {canonical_row['lat']})"

        session.add(merchant)
        session.flush()

        for pm_id in member_ids:
            mm = MerchantMatch(
                platform_merchant_id=pm_id,
                merchant_id=merchant.id,
                confidence=0.9,
                match_method="local_batch"
            )
            session.merge(mm)

    session.commit()
    session.close()
    logger.info("Entity resolution complete.")

def main():
    # Load data from the split scraper files
    data = []
    found_files = [f for f in INPUT_FILES if f.exists()]

    if found_files:
        for input_file in found_files:
            with open(input_file, "r") as f:
                file_data = json.load(f)
                logger.info("Loaded %d listings from %s", len(file_data), input_file.name)
                data.extend(file_data)
    elif INPUT_FILE_LEGACY.exists():
        with open(INPUT_FILE_LEGACY, "r") as f:
            data = json.load(f)
            logger.info("Loaded %d listings from legacy file %s", len(data), INPUT_FILE_LEGACY.name)
    else:
        logger.error("No input files found. Run run_local_ubereats.py and run_local_justeattakeaway.py first.")
        return

    logger.info("Total listings to ingest: %d", len(data))

    # 1. Ingest
    ingest_data(data)

    # 2. Resolve
    resolve_entities()

    # 3. Report
    logger.info("Generating coverage report...")
    report = generate_coverage_report()
    
    # Filter report to just Amsterdam for the printout if possible, 
    # but generate_coverage_report does full DB.
    
    print_report(report)

    # 4. Save report
    with open(REPORT_OUTPUT, "w") as f:
        json.dump(report, f, indent=2, default=str)
    
    logger.info(f"Report saved to {REPORT_OUTPUT}")

if __name__ == "__main__":
    main()
