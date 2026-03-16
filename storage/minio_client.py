"""
MinIO (S3-compatible) client for storing and retrieving raw scrape artifacts.

Uses boto3 with an endpoint override so the same code works against both
local MinIO and production AWS S3.
"""

from __future__ import annotations

import io
import json
import os
from datetime import datetime

import boto3
from botocore.config import Config as BotoConfig


# ── Configuration ───────────────────────────────────────────────────

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

RAW_BUCKET = "raw-html"
PARSED_BUCKET = "parsed-json"


def _get_client():
    """Create a boto3 S3 client pointed at MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )


# ── Public API ──────────────────────────────────────────────────────


def upload_raw_html(
    platform: str,
    market: str,
    merchant_id: str,
    html: str,
) -> str:
    """
    Upload raw HTML to MinIO and return the S3 key.

    Key pattern: ``{platform}/{date}/{market}/{merchant_id}.html``
    """
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    key = f"{platform}/{date_str}/{market}/{merchant_id}.html"

    client = _get_client()
    client.put_object(
        Bucket=RAW_BUCKET,
        Key=key,
        Body=html.encode("utf-8"),
        ContentType="text/html",
    )
    return key


def upload_parsed_json(
    platform: str,
    market: str,
    merchant_id: str,
    data: dict,
) -> str:
    """
    Upload parsed JSON to MinIO and return the S3 key.

    Key pattern: ``{platform}/{date}/{market}/{merchant_id}.json``
    """
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    key = f"{platform}/{date_str}/{market}/{merchant_id}.json"

    body = json.dumps(data, ensure_ascii=False, default=str)

    client = _get_client()
    client.put_object(
        Bucket=PARSED_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    return key


def download_raw_html(key: str) -> str:
    """Download raw HTML from MinIO by S3 key."""
    client = _get_client()
    response = client.get_object(Bucket=RAW_BUCKET, Key=key)
    return response["Body"].read().decode("utf-8")


def download_parsed_json(key: str) -> dict:
    """Download parsed JSON from MinIO by S3 key."""
    client = _get_client()
    response = client.get_object(Bucket=PARSED_BUCKET, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def list_keys(bucket: str, prefix: str = "") -> list[str]:
    """List all object keys in a bucket under the given prefix."""
    client = _get_client()
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys
