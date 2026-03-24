-- ─── ClickHouse Init for Staging ────────────────────────────────
-- Auto-executed on first container start.
-- Creates databases for each review source with raw_reviews table.
-- 30-day TTL auto-deletes old staging data.
-- ────────────────────────────────────────────────────────────────

-- source_33 (TripAdvisor reviews)
CREATE DATABASE IF NOT EXISTS source_33;
CREATE TABLE IF NOT EXISTS source_33.raw_reviews (
    source_product_id   String,
    review_language     String,
    review_text         String,
    original_review_text String DEFAULT '',
    source_review_id    String,
    url                 String DEFAULT '',
    owner_response      String DEFAULT '',
    original_owner_response String DEFAULT '',
    source              String,
    user_id             String DEFAULT '',
    username            String,
    review_rating       Float32,
    review_location     String DEFAULT '',
    review_date         DateTime,
    review_parse_date   DateTime DEFAULT now(),
    additional_info     String DEFAULT '',
    batch_id            UInt64,
    batch_timestamp     DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(batch_timestamp)
ORDER BY (source_product_id, source_review_id, batch_timestamp)
TTL batch_timestamp + INTERVAL 30 DAY DELETE
SETTINGS merge_with_ttl_timeout = 3600, ttl_only_drop_parts = 1;

-- source_47 (Yandex Maps reviews)
CREATE DATABASE IF NOT EXISTS source_47;
CREATE TABLE IF NOT EXISTS source_47.raw_reviews AS source_33.raw_reviews
ENGINE = MergeTree()
PARTITION BY toYYYYMM(batch_timestamp)
ORDER BY (source_product_id, source_review_id, batch_timestamp)
TTL batch_timestamp + INTERVAL 30 DAY DELETE
SETTINGS merge_with_ttl_timeout = 3600, ttl_only_drop_parts = 1;

-- source_40 (Baidu reviews)
CREATE DATABASE IF NOT EXISTS source_40;
CREATE TABLE IF NOT EXISTS source_40.raw_reviews AS source_33.raw_reviews
ENGINE = MergeTree()
PARTITION BY toYYYYMM(batch_timestamp)
ORDER BY (source_product_id, source_review_id, batch_timestamp)
TTL batch_timestamp + INTERVAL 30 DAY DELETE
SETTINGS merge_with_ttl_timeout = 3600, ttl_only_drop_parts = 1;

-- source_39 (Yelp reviews)
CREATE DATABASE IF NOT EXISTS source_39;
CREATE TABLE IF NOT EXISTS source_39.raw_reviews AS source_33.raw_reviews
ENGINE = MergeTree()
PARTITION BY toYYYYMM(batch_timestamp)
ORDER BY (source_product_id, source_review_id, batch_timestamp)
TTL batch_timestamp + INTERVAL 30 DAY DELETE
SETTINGS merge_with_ttl_timeout = 3600, ttl_only_drop_parts = 1;

-- source_41 (Yahoo reviews)
CREATE DATABASE IF NOT EXISTS source_41;
CREATE TABLE IF NOT EXISTS source_41.raw_reviews AS source_33.raw_reviews
ENGINE = MergeTree()
PARTITION BY toYYYYMM(batch_timestamp)
ORDER BY (source_product_id, source_review_id, batch_timestamp)
TTL batch_timestamp + INTERVAL 30 DAY DELETE
SETTINGS merge_with_ttl_timeout = 3600, ttl_only_drop_parts = 1;

-- source_63 (OpenTable reviews)
CREATE DATABASE IF NOT EXISTS source_63;
CREATE TABLE IF NOT EXISTS source_63.raw_reviews AS source_33.raw_reviews
ENGINE = MergeTree()
PARTITION BY toYYYYMM(batch_timestamp)
ORDER BY (source_product_id, source_review_id, batch_timestamp)
TTL batch_timestamp + INTERVAL 30 DAY DELETE
SETTINGS merge_with_ttl_timeout = 3600, ttl_only_drop_parts = 1;
