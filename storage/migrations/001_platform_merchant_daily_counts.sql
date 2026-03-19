-- Migration: Add platform_merchant_daily_counts table for Grafana week-over-week deltas.
-- Run this if your DB was created before this table was added to init_db.sql:
--   psql -U scraper -d scraping -f storage/migrations/001_platform_merchant_daily_counts.sql

CREATE TABLE IF NOT EXISTS platform_merchant_daily_counts (
    snapshot_date DATE NOT NULL,
    platform      TEXT NOT NULL,
    count         BIGINT NOT NULL,
    PRIMARY KEY (snapshot_date, platform)
);
