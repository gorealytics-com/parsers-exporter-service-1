"""Organisation data schema — 27-field contract between exporters and pg-writer.

Used by: MapExporter, DeliveryExporter, SitemapExporter → pg-writer.
Kafka topic: orgs.validated.v1
Conflict key: place_id
"""

import msgspec


class OrgData(msgspec.Struct, kw_only=True, omit_defaults=True, gc=False):
    """Organisation record validated by exporters, consumed by pg-writer."""

    place_id: str
    source_name: str
    name: str
    data_hash: str | None = None

    # Location
    address: str | None = None
    city: str | None = None
    state: str | None = None
    country: str | None = None
    latitude: float | None = None
    longitude: float | None = None

    # Business info
    categories: str | None = None
    alternative_name: str | None = None
    link: str | None = None
    rating: float | None = None
    reviews: str | None = None
    price_range: str | None = None
    opening_hrs: str | None = None
    additional_info: dict | None = None

    # Batch metadata
    update_timestamp: str | None = None
    update_batch_id: int | None = None
    batch_timestamp: str | None = None
    batch_id: int | None = None

    # Source tracking
    source_id: str | None = None
    spider_job_id: str | None = None
    crawl_date: str | None = None
    raw_data: str | None = None
