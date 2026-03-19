"""
Kafka consumer daemon that listens for raw HTML scrapes and parses them.
"""

import json
import logging
import os
import sys
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException
from prometheus_client import Counter, start_http_server
from sqlalchemy.dialects.postgresql import insert

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from processing.normalizer import normalize_listing
from processing.parser import parse_listing
from storage.db import MenuItemRow, PlatformMerchant, get_session
from storage.minio_client import download_raw_html, upload_parsed_json

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("parser_consumer")

KAFKA_TOPIC = "raw-html-scraped"
GROUP_ID = "parser-consumer-group-1"

# --- Data Quality Metrics ---
DQ_MISSING_ADDRESS = Counter("dq_missing_address_total", "Number of scraped merchants missing an address", ["platform", "market"])
DQ_MISSING_CUISINE = Counter("dq_missing_cuisine_total", "Number of scraped merchants missing cuisine tags", ["platform", "market"])
DQ_INVALID_PRICE = Counter("dq_invalid_price_total", "Number of items with unparseable or negative prices", ["platform", "market"])
DQ_MISSING_COORDS = Counter("dq_missing_coords_total", "Number of scraped merchants missing lat/lng", ["platform", "market"])

def process_message(msg):
    try:
        data = json.loads(msg.value().decode("utf-8"))
    except json.JSONDecodeError:
        logger.error("Failed to decode message: %s", msg.value())
        return

    platform = data.get("platform")
    market = data.get("market")
    minio_key = data.get("minio_key")

    if not all([platform, market, minio_key]):
        logger.error("Missing required fields in event: %s", data)
        return

    logger.info("Processing HTML for %s / %s (key: %s)", platform, market, minio_key)

    try:
        html = download_raw_html(minio_key)
    except Exception as exc:
        logger.error("Failed to download HTML from MinIO %s: %s", minio_key, exc)
        return

    try:
        listings = parse_listing(html, platform, market)
    except Exception as exc:
        logger.error("Failed to parse HTML for %s: %s", minio_key, exc)
        return

    session = get_session()
    try:
        for listing in listings:
            listing = normalize_listing(listing)

            pm = PlatformMerchant(
                platform=listing.platform,
                platform_id=listing.platform_merchant_id,
                name=listing.name,
                address=listing.address,
                cuisine_tags=listing.cuisine_tags,
                rating=listing.rating,
                review_count=listing.review_count,
                price_bucket=listing.price_bucket,
                delivery_fee=listing.delivery_fee,
                estimated_delivery_min=listing.estimated_delivery_min,
                is_promoted=listing.is_promoted,
                market=listing.market,
                raw_url=listing.raw_url,
                raw_s3_key=minio_key,
            )

            # --- Inline Data Quality Tracking ---
            if not listing.address:
                DQ_MISSING_ADDRESS.labels(platform=platform, market=market).inc()
            if not listing.cuisine_tags:
                DQ_MISSING_CUISINE.labels(platform=platform, market=market).inc()
            if not listing.lat or not listing.lng:
                DQ_MISSING_COORDS.labels(platform=platform, market=market).inc()

            # Perform PostgreSQL Upsert
            stmt = insert(PlatformMerchant).values(
                platform=pm.platform,
                platform_id=pm.platform_id,
                name=pm.name,
                address=pm.address,
                geom=pm.geom,
                cuisine_tags=pm.cuisine_tags,
                rating=pm.rating,
                review_count=pm.review_count,
                price_bucket=pm.price_bucket,
                delivery_fee=pm.delivery_fee,
                estimated_delivery_min=pm.estimated_delivery_min,
                is_promoted=pm.is_promoted,
                market=pm.market,
                raw_url=pm.raw_url,
                raw_s3_key=pm.raw_s3_key,
                scraped_at=pm.scraped_at
            )

            update_dict = {
                c.name: c
                for c in stmt.excluded
                if c.name not in ('id', 'platform', 'platform_id')
            }

            do_update_stmt = stmt.on_conflict_do_update(
                constraint='platform_merchants_platform_platform_id_key',
                set_=update_dict
            ).returning(PlatformMerchant.id)

            result = session.execute(do_update_stmt)
            merged_pm_id = result.scalar_one()
            session.flush()

            if listing.menu_items:
                # Clear existing menu items and replace
                session.query(MenuItemRow).filter(
                    MenuItemRow.platform_merchant_id == merged_pm_id
                ).delete()

                for mi in listing.menu_items:
                    mi_row = MenuItemRow(
                        platform_merchant_id=merged_pm_id,
                        name=mi.name,
                        price=mi.price,
                        description=mi.description,
                        category=mi.category,
                    )

                    if mi.price is None or mi.price < 0:
                        DQ_INVALID_PRICE.labels(platform=platform, market=market).inc()

                    session.add(mi_row)

            # Archive structured json
            upload_parsed_json(
                platform=listing.platform,
                market=listing.market,
                merchant_id=listing.platform_merchant_id,
                data=listing.model_dump(mode="json"),
            )
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error("Database error while saving %s: %s", minio_key, e)
    finally:
        session.close()

def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }

    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])

    logger.info("Starting Prometheus metrics server on port 8001")
    start_http_server(8001)

    logger.info("Kafka consumer started. Listening on %s for topic %s", bootstrap_servers, KAFKA_TOPIC)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                    continue
                else:
                    logger.error("Consumer error: %s", msg.error())
                    raise KafkaException(msg.error())

            process_message(msg)
            consumer.commit(asynchronous=True)

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
    finally:
        consumer.close()
        logger.info("Consumer closed.")

if __name__ == "__main__":
    main()
