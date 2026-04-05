"""Review schema — 18-field record for ClickHouse.

Kafka topic: reviews.validated.v1
Table: {source_db}.raw_reviews (MergeTree)
Dedup: Redis SADD on source_review_id before Kafka produce.
"""

import msgspec


class Review(msgspec.Struct, kw_only=True, omit_defaults=True, gc=False):
    """Review record validated by ReviewsExporter, consumed by ch-writer."""

    source_product_id: str | None = None
    review_language: str | None = None
    review_text: str | None = None
    original_review_text: str | None = None
    source_review_id: str | None = None
    url: str | None = None
    owner_response: str | None = None
    original_owner_response: str | None = None
    source: str | None = None
    user_id: str | None = None
    username: str | None = None
    review_rating: float | None = None
    review_location: str | None = None
    review_date: str | None = None
    review_parse_date: str | None = None
    additional_info: str | None = None
    batch_id: int | None = None
    batch_timestamp: str | None = None
