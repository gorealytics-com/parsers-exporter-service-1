"""Menu item schema — 18-field record.

Kafka topic: orgs.validated.v1 (embedded in org message envelope as list)
Conflict key: id
"""

import msgspec


class MenuItem(msgspec.Struct, kw_only=True, omit_defaults=True, gc=False):
    """Menu item record for restaurant/delivery organisations."""

    id: str
    place_id: str | None = None
    name: str | None = None
    description: str | None = None
    category: str | None = None
    subcategory: str | None = None
    price: float | None = None
    currency: str | None = None
    image_url: str | None = None
    is_available: bool | None = None
    options: str | None = None
    allergens: str | None = None
    nutritional_info: str | None = None
    rating: float | None = None
    popularity: int | None = None
    update_timestamp: str | None = None
    update_batch_id: int | None = None
    raw_data: str | None = None
