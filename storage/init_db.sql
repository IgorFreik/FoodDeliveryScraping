-- Initialization script for PostgreSQL.
-- Runs on first container start via /docker-entrypoint-initdb.d/.
-- The `scraping` database is already created by POSTGRES_DB env var.
-- We also need a separate `airflow` database for Airflow metadata.

-- Create airflow database
CREATE DATABASE airflow;

-- Enable PostGIS on the scraping database (current DB)
CREATE EXTENSION IF NOT EXISTS postgis;

-- ────────────────────────────────────────────────────────────────────
-- Core tables
-- ────────────────────────────────────────────────────────────────────

-- Scraped merchants: one row per platform listing per crawl
CREATE TABLE IF NOT EXISTS platform_merchants (
    id              BIGSERIAL PRIMARY KEY,
    platform        TEXT NOT NULL,
    platform_id     TEXT NOT NULL,
    name            TEXT,
    address         TEXT,
    geom            GEOGRAPHY(Point, 4326),
    cuisine_tags    TEXT[],
    rating          NUMERIC(3,2),
    review_count    INT,
    price_bucket    TEXT,
    delivery_fee    NUMERIC(6,2),
    estimated_delivery_min INT,
    is_promoted     BOOLEAN DEFAULT FALSE,
    market          TEXT,
    scraped_at      TIMESTAMPTZ DEFAULT NOW(),
    raw_s3_key      TEXT,
    raw_url         TEXT,
    UNIQUE (platform, platform_id)
);

-- Resolved real-world entities
CREATE TABLE IF NOT EXISTS merchants (
    id              BIGSERIAL PRIMARY KEY,
    canonical_name  TEXT,
    canonical_geom  GEOGRAPHY(Point, 4326),
    canonical_addr  TEXT,
    is_on_our_platform BOOLEAN DEFAULT FALSE,
    our_merchant_id TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Many-to-one: platform listings → resolved merchant
CREATE TABLE IF NOT EXISTS merchant_matches (
    platform_merchant_id BIGINT REFERENCES platform_merchants(id) ON DELETE CASCADE,
    merchant_id          BIGINT REFERENCES merchants(id) ON DELETE CASCADE,
    confidence           NUMERIC(4,3),
    match_method         TEXT,
    matched_at           TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (platform_merchant_id)
);

-- Menu items scraped per merchant
CREATE TABLE IF NOT EXISTS menu_items (
    id                   BIGSERIAL PRIMARY KEY,
    platform_merchant_id BIGINT REFERENCES platform_merchants(id) ON DELETE CASCADE,
    name                 TEXT NOT NULL,
    price                NUMERIC(8,2),
    description          TEXT,
    category             TEXT,
    scraped_at           TIMESTAMPTZ DEFAULT NOW()
);

-- Crawl metadata / audit log
CREATE TABLE IF NOT EXISTS crawl_runs (
    id              BIGSERIAL PRIMARY KEY,
    platform        TEXT NOT NULL,
    market          TEXT NOT NULL,
    started_at      TIMESTAMPTZ DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    status          TEXT DEFAULT 'running',  -- running | success | failed
    merchants_found INT DEFAULT 0,
    errors          INT DEFAULT 0,
    error_detail    TEXT
);

-- ────────────────────────────────────────────────────────────────────
-- Indexes
-- ────────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_pm_platform_market
    ON platform_merchants (platform, market);

CREATE INDEX IF NOT EXISTS idx_pm_geom
    ON platform_merchants USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_merchants_geom
    ON merchants USING GIST (canonical_geom);

CREATE INDEX IF NOT EXISTS idx_menu_items_merchant
    ON menu_items (platform_merchant_id);
