#!/usr/bin/env python3
"""
Integration example: Run the resolve_entities pipeline (same logic as Airflow DAG).

Requires PostgreSQL with platform_merchants data. Run from project root:
    python examples/run_resolve_entities.py
"""
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def main():
    from processing.entity_resolution import run_resolve_entities

    logger.info("Running resolve_entities (same logic as Airflow DAG)...")
    try:
        run_resolve_entities()
        logger.info("resolve_entities completed successfully.")
    except Exception as e:
        logger.exception("resolve_entities failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
