-- ──────────────────────────────────────────────────────────────────
-- DuckDB analytical views.
--
-- These queries are designed to run against a DuckDB instance that
-- connects to the PostgreSQL database via the postgres_scanner
-- extension, or against Parquet exports.
--
-- Usage (DuckDB CLI):
--   INSTALL postgres_scanner;
--   LOAD postgres_scanner;
--   ATTACH 'host=localhost dbname=scraping user=scraper password=scraper123' AS pg (TYPE postgres);
--   .read analytics/duckdb_views.sql
-- ──────────────────────────────────────────────────────────────────

-- 1. Coverage by platform × market
CREATE OR REPLACE VIEW coverage_by_platform_market AS
SELECT
    platform,
    market,
    COUNT(DISTINCT platform_id) AS merchant_count,
    ROUND(AVG(rating), 2) AS avg_rating,
    ROUND(AVG(review_count), 0) AS avg_reviews,
    COUNT(DISTINCT CASE WHEN rating >= 4.5 THEN platform_id END) AS high_rated_count
FROM pg.platform_merchants
GROUP BY platform, market
ORDER BY market, platform;


-- 2. Coverage gap: merchants on competitor but NOT matched to our platform
CREATE OR REPLACE VIEW coverage_gaps AS
SELECT
    pm.platform,
    pm.market,
    pm.name,
    pm.address,
    pm.rating,
    pm.review_count,
    pm.cuisine_tags,
    pm.price_bucket
FROM pg.platform_merchants pm
LEFT JOIN pg.merchant_matches mm ON mm.platform_merchant_id = pm.id
LEFT JOIN pg.merchants m ON m.id = mm.merchant_id
WHERE m.is_on_our_platform = FALSE
   OR m.id IS NULL
ORDER BY pm.rating DESC NULLS LAST, pm.review_count DESC NULLS LAST;


-- 3. Overlap: merchants present on multiple platforms
CREATE OR REPLACE VIEW cross_platform_overlap AS
SELECT
    m.id AS entity_id,
    m.canonical_name,
    m.canonical_addr,
    COUNT(DISTINCT pm.platform) AS platform_count,
    ARRAY_AGG(DISTINCT pm.platform) AS platforms,
    ROUND(AVG(pm.rating), 2) AS avg_rating,
    SUM(pm.review_count) AS total_reviews
FROM pg.merchants m
JOIN pg.merchant_matches mm ON mm.merchant_id = m.id
JOIN pg.platform_merchants pm ON pm.id = mm.platform_merchant_id
GROUP BY m.id, m.canonical_name, m.canonical_addr
HAVING COUNT(DISTINCT pm.platform) >= 2
ORDER BY total_reviews DESC NULLS LAST;


-- 4. Price comparison for overlapping merchants
CREATE OR REPLACE VIEW price_comparison AS
SELECT
    m.canonical_name,
    pm.platform,
    pm.market,
    pm.price_bucket,
    pm.delivery_fee,
    pm.estimated_delivery_min,
    pm.rating
FROM pg.merchants m
JOIN pg.merchant_matches mm ON mm.merchant_id = m.id
JOIN pg.platform_merchants pm ON pm.id = mm.platform_merchant_id
WHERE m.id IN (
    SELECT merchant_id
    FROM pg.merchant_matches
    GROUP BY merchant_id
    HAVING COUNT(*) >= 2
)
ORDER BY m.canonical_name, pm.platform;


-- 5. Cuisine distribution by platform
CREATE OR REPLACE VIEW cuisine_distribution AS
SELECT
    platform,
    UNNEST(cuisine_tags) AS cuisine,
    COUNT(*) AS merchant_count
FROM pg.platform_merchants
WHERE cuisine_tags IS NOT NULL
GROUP BY platform, cuisine
ORDER BY platform, merchant_count DESC;


-- 6. Weekly crawl health
CREATE OR REPLACE VIEW crawl_health AS
SELECT
    platform,
    market,
    DATE_TRUNC('week', started_at) AS week,
    COUNT(*) AS crawl_count,
    COUNT(CASE WHEN status = 'success' THEN 1 END) AS successes,
    COUNT(CASE WHEN status = 'failed' THEN 1 END) AS failures,
    ROUND(AVG(merchants_found), 0) AS avg_merchants_per_crawl
FROM pg.crawl_runs
GROUP BY platform, market, DATE_TRUNC('week', started_at)
ORDER BY week DESC, platform, market;


-- 7. Top acquisition targets (high-value merchants not on our platform)
CREATE OR REPLACE VIEW acquisition_targets AS
SELECT
    pm.platform,
    pm.market,
    pm.name,
    pm.address,
    pm.rating,
    pm.review_count,
    pm.cuisine_tags,
    pm.price_bucket,
    (COALESCE(pm.rating, 0) * LOG(COALESCE(pm.review_count, 1) + 1)) AS quality_score
FROM pg.platform_merchants pm
LEFT JOIN pg.merchant_matches mm ON mm.platform_merchant_id = pm.id
LEFT JOIN pg.merchants m ON m.id = mm.merchant_id
WHERE m.is_on_our_platform = FALSE
   OR m.id IS NULL
ORDER BY quality_score DESC
LIMIT 500;
