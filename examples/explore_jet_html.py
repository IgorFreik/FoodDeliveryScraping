#!/usr/bin/env python3
"""
Exploration script: Inspect JustEatTakeaway page structure.

Prints script IDs and data keys present in JET HTML. Requires MinIO with
scraped JET data. Run from project root:
    python examples/explore_jet_html.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bs4 import BeautifulSoup

from storage.minio_client import RAW_BUCKET, download_raw_html, list_keys

keys = list_keys(RAW_BUCKET, prefix="justeattakeaway/")
if not keys:
    print("No JustEatTakeaway data in MinIO. Run scrape_justeattakeaway first.")
    sys.exit(1)

html = download_raw_html(keys[0])
soup = BeautifulSoup(html, "html.parser")

print("Script IDs:")
for s in soup.find_all("script", id=True):
    print("  ", s["id"], "| length:", len(s.string or ""))

print("\nData keys in HTML:")
for key in ["restaurantData", "__NEXT_DATA__", "__REACT_QUERY_STATE__"]:
    if key in html:
        print(f"  '{key}' found")
