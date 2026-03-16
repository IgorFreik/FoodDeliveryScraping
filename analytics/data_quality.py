"""
SQL-based Data Quality rules for checking at-rest PostGIS data.
Calculates completeness, freshness, and geo-accuracy metrics.
"""

from typing import Dict, Any
from storage.db import get_session
from sqlalchemy import text


def get_dq_metrics() -> Dict[str, Any]:
    """Execute SQL rules to calculate overall DB health scores."""
    session = get_session()
    metrics = {
        "completeness_score": 0.0,
        "geo_accuracy_score": 0.0,
        "freshness_score": 0.0,
        "overall_health": 0.0,
        "total_merchants": 0,
        "missing_address_count": 0,
        "missing_cuisine_count": 0,
        "stale_merchant_count": 0,
        "invalid_coords_count": 0
    }

    try:
        # 1. Total records
        total_res = session.execute(text("SELECT COUNT(*) FROM platform_merchants")).scalar()
        if not total_res or total_res == 0:
            return metrics
            
        metrics["total_merchants"] = total_res

        # 2. Completeness Check
        missing_address = session.execute(text("SELECT COUNT(*) FROM platform_merchants WHERE address IS NULL OR address = ''")).scalar() or 0
        missing_cuisine = session.execute(text("SELECT COUNT(*) FROM platform_merchants WHERE cuisine_tags IS NULL OR array_length(cuisine_tags, 1) = 0")).scalar() or 0
        
        metrics["missing_address_count"] = missing_address
        metrics["missing_cuisine_count"] = missing_cuisine
        
        # Completeness Score: % of records that have both address and cuisine
        complete_records = total_res - (missing_address + missing_cuisine)
        metrics["completeness_score"] = round(max(0.0, complete_records / total_res * 100), 2)

        # 3. Freshness Check (Scraped within the last 7 days)
        stale_res = session.execute(text("SELECT COUNT(*) FROM platform_merchants WHERE scraped_at < NOW() - INTERVAL '7 days'")).scalar() or 0
        metrics["stale_merchant_count"] = stale_res
        metrics["freshness_score"] = round(((total_res - stale_res) / total_res) * 100, 2)

        # 4. Geo-Accuracy Check 
        # For simplicity, invalid coords = (0,0) or missing geom
        invalid_geom = session.execute(text(
            "SELECT COUNT(*) FROM platform_merchants WHERE geom IS NULL OR ST_Equals(geom::geometry, ST_SetSRID(ST_MakePoint(0, 0), 4326))"
        )).scalar() or 0
        
        metrics["invalid_coords_count"] = invalid_geom
        metrics["geo_accuracy_score"] = round(((total_res - invalid_geom) / total_res) * 100, 2)

        # 5. Overall Health
        # We weight Geo Accuracy highly because it breaks the overlap analysis.
        metrics["overall_health"] = round((
            metrics["completeness_score"] * 0.3 + 
            metrics["freshness_score"] * 0.3 + 
            metrics["geo_accuracy_score"] * 0.4
        ), 2)

    except Exception as e:
        # Avoid crashing the API if DB is unavailable
        import logging
        logging.getLogger(__name__).error(f"Failed to calculate DQ metrics: {e}")
        
    finally:
        session.close()

    return metrics
