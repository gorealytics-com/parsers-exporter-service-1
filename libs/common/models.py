"""
Schema definitions for PostgreSQL tables.

Ported from: aragog_exporter_service/utils/models.py + delivery_models.py

These are NOT used as ORM models at runtime — pg-writer uses raw asyncpg
with dynamic column detection. This module serves as:
  1. Authoritative column reference for pg-writer's build_upsert_sql()
  2. Documentation of the DB schema
  3. Source of truth for init-scripts SQL generation

The original SQLAlchemy models used create_*_model(Base, schema) factory
functions because each source (source_33, source_42, etc.) has identical
table structures in different PG schemas. In the new architecture,
the schema name comes from the Kafka message envelope.
"""

# --- organisation_data columns (all PG schemas) ---
# Conflict key: place_id
ORGANISATION_DATA_COLUMNS = {
    "place_id":             "VARCHAR(40) PRIMARY KEY",
    "source_place_id":      "VARCHAR(512)",
    "batch_timestamp":      "TIMESTAMP DEFAULT NOW()",
    "batch_id":             "INTEGER",
    "name":                 "VARCHAR(255)",
    "categories":           "TEXT[]",
    "city":                 "VARCHAR(255)",
    "country":              "VARCHAR(255)",
    "link":                 "VARCHAR(1024)",
    "reviews":              "INTEGER",
    "rate":                 "DOUBLE PRECISION",
    "lon":                  "DOUBLE PRECISION",
    "lat":                  "DOUBLE PRECISION",
    "address":              "VARCHAR(1024)",
    "phone":                "VARCHAR(1024)",
    "website":              "VARCHAR(1024)",
    "price_range":          "VARCHAR(255)",
    "opening_hrs":          "JSONB",
    "data_hash":            "VARCHAR(40)",
    "has_changed":          "SMALLINT DEFAULT 0",
    "modification_timestamp": "TIMESTAMP",
    "additional_info":      "JSONB",
    "parent_place_id":      "VARCHAR(512)",
    "open_status":          "VARCHAR(64)",
    "alternative_name":     "VARCHAR(255)",
    "status":               "SMALLINT DEFAULT 2",
    "update_timestamp":     "TIMESTAMP DEFAULT NOW()",
    "update_batch_id":      "INTEGER",
}

# --- organisation_contacts columns ---
# Conflict key: place_id
ORGANISATION_CONTACTS_COLUMNS = {
    "place_id":       "VARCHAR(128) PRIMARY KEY",
    "link":           "VARCHAR(4096)",
    "web_link":       "VARCHAR(4096)",
    "dst_web_link":   "VARCHAR(4096)",
    "error_text":     "VARCHAR(4096)",
    "error_code":     "INTEGER DEFAULT 0",
    "mails":          "TEXT[]",
    "phones":         "TEXT[]",
    "facebook":       "TEXT[]",
    "facebook_msg":   "TEXT[]",
    "instagram":      "TEXT[]",
    "twitter":        "TEXT[]",
    "tiktok":         "TEXT[]",
    "linkedin":       "TEXT[]",
    "whatsapp":       "TEXT[]",
    "telegram":       "TEXT[]",
    "vkontakte":      "TEXT[]",
    "pinterest":      "TEXT[]",
    "viber":          "TEXT[]",
    "youtube":        "TEXT[]",
    "odnoklassniki":  "TEXT[]",
    "status":         "SMALLINT DEFAULT 0",
}

# --- menu_data columns ---
# Conflict key: id
MENU_DATA_COLUMNS = {
    "id":               "VARCHAR(40) PRIMARY KEY",
    "place_id":         "VARCHAR(40)",
    "source_id":        "VARCHAR(512)",
    "name":             "VARCHAR(1024)",
    "top_category":     "VARCHAR(1024)",
    "category":         "VARCHAR(1024)",
    "price":            "VARCHAR(1024)",
    "weight":           "VARCHAR(1024)",
    "image_url":        "VARCHAR(1024)",
    "resolution":       "VARCHAR(1024)",
    "is_available":     "BOOLEAN DEFAULT TRUE",
    "update_timestamp": "TIMESTAMP DEFAULT NOW()",
    "update_batch_id":  "INTEGER",
    "hash":             "VARCHAR(40)",
    "additional_info":  "JSONB",
    "description":      "VARCHAR(1024)",
    "volume":           "VARCHAR(1024)",
    "quantity":         "VARCHAR(1024)",
}

# --- spider_jobs (aragog_service DB) ---
SPIDER_JOBS_COLUMNS = {
    "job_id":           "VARCHAR(40) PRIMARY KEY",
    "pack_id":          "VARCHAR(100)",
    "spider_name":      "VARCHAR(100)",
    "project_name":     "VARCHAR(100)",
    "host":             "VARCHAR(20)",
    "port":             "INTEGER",
    "args":             "JSONB",
    "settings":         "JSONB",
    "status":           "VARCHAR(50) NOT NULL",
    "finish_type":      "VARCHAR(20)",
    "planned_time":     "TIMESTAMP DEFAULT NOW()",
    "running_time":     "TIMESTAMP",
    "turning_off_time": "TIMESTAMP",
    "end_time":         "TIMESTAMP",
}

# --- ClickHouse raw_reviews columns ---
# (matches COLUMN_NAMES in ch-writer/main.py)
RAW_REVIEWS_COLUMNS = [
    "source_product_id",
    "review_language",
    "review_text",
    "original_review_text",
    "source_review_id",
    "url",
    "owner_response",
    "original_owner_response",
    "source",
    "user_id",
    "username",
    "review_rating",
    "review_location",
    "review_date",
    "review_parse_date",
    "additional_info",
    "batch_id",
    "batch_timestamp",
]

# --- Delivery-specific tables (from delivery_models.py) ---
# NOTE: These were marked "TODO do not use this models so far" in original.
# Kept as reference. Delivery sources use the same organisation_data table
# but with JSONB additional_info column instead of typed sub-columns.
DELIVERY_ORG_EXTRA_NOTES = """
Delivery sources (source_23..source_61) use the same organisation_data
and menu_data tables as map sources. The delivery_models.py defined
slightly different column types (JSONB vs JSON for additional_info),
but the actual init-scripts/postgres DDL uses JSONB for all schemas.
No separate model needed — pg-writer handles dynamic columns.
"""
