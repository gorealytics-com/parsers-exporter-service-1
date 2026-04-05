"""Sample test data for unit and integration tests."""

import msgspec

_encoder = msgspec.json.Encoder()


def _to_bytes(obj: dict) -> bytes:
    """Encode dict to JSON bytes using msgspec."""
    return _encoder.encode(obj)


# ---------------------------------------------------------------------------
# Organisation data samples (from crawlers → Redis → org-exporter)
# ---------------------------------------------------------------------------

SAMPLE_ORG_ITEM = {
    "place_id": "ChIJN1t_tDeuEmsRUsoyG83frY4",
    "name": "Google Sydney",
    "categories": "Technology Company",
    "city": "Sydney",
    "country": "Australia",
    "address": "48 Pirrama Rd, Pyrmont NSW 2009",
    "lat": -33.866489,
    "lon": 151.195677,
    "link": "https://maps.google.com/maps?cid=123",
    "reviews": "4523",
    "rate": "4.3",
    "phone": "+61 2 9374 4000",
    "website": "https://about.google/intl/en_au/",
    "email": "info@google.com.au",
    "price_range": "$$",
    "opening_hrs": "Mon-Fri 9:00-17:00",
    "additional_info": {
        "contacts": {
            "facebook": "https://facebook.com/Google",
            "instagram": "https://instagram.com/google",
        },
        "amenities": ["wifi", "parking"],
    },
    "menu": [
        {"id": "menu_001", "name": "Coffee", "price": 4.50, "category": "Beverages"},
        {"id": "menu_002", "name": "Sandwich", "price": 12.00, "category": "Food"},
    ],
}

SAMPLE_ORG_ITEM_MINIMAL = {
    "place_id": "min_place_123",
    "name": "Minimal Org",
}

SAMPLE_ORG_ITEM_NO_PLACE_ID = {
    "name": "No Place ID Org",
    "city": "Berlin",
}

SAMPLE_ORG_ITEM_BYTES = _to_bytes(SAMPLE_ORG_ITEM)
SAMPLE_ORG_ITEM_MINIMAL_BYTES = _to_bytes(SAMPLE_ORG_ITEM_MINIMAL)
SAMPLE_ORG_ITEM_NO_PLACE_ID_BYTES = _to_bytes(SAMPLE_ORG_ITEM_NO_PLACE_ID)
SAMPLE_ORG_INVALID_JSON = b"{broken json here"


# ---------------------------------------------------------------------------
# Review data samples (from crawlers → Redis → review-exporter)
# ---------------------------------------------------------------------------

SAMPLE_REVIEW_ITEM = {
    "source_product_id": "prod_12345",
    "source_review_id": "rev_67890",
    "review_text": "Great experience, highly recommended!",
    "original_review_text": "Отличный опыт, рекомендую!",
    "review_rating": "4.5",
    "review_date": "2024-03-15",
    "review_language": "en",
    "url": "https://tripadvisor.com/review/67890",
    "username": "traveler42",
    "user_id": "user_abc",
    "source": "Tripadvisor",
    "owner_response": "Thank you!",
    "additional_info": {"helpful_votes": 12},
}

SAMPLE_REVIEW_OLD_DATE = {
    "source_product_id": "prod_old",
    "source_review_id": "rev_old",
    "review_text": "Old review",
    "review_rating": "3.0",
    "review_date": "2018-01-01",
    "source": "Tripadvisor",
}

SAMPLE_REVIEW_JP_DATE = {
    "source_product_id": "prod_jp",
    "source_review_id": "rev_jp",
    "review_text": "素晴らしい",
    "review_rating": "5",
    "review_date": "2024年03月15日",
    "source": "Baidu",
}

SAMPLE_REVIEW_NO_DATE = {
    "source_product_id": "prod_nodate",
    "source_review_id": "rev_nodate",
    "review_text": "No date review",
    "review_rating": "2.0",
    "source": "Tripadvisor",
}

SAMPLE_REVIEW_BYTES = _to_bytes(SAMPLE_REVIEW_ITEM)
SAMPLE_REVIEW_OLD_DATE_BYTES = _to_bytes(SAMPLE_REVIEW_OLD_DATE)
SAMPLE_REVIEW_JP_DATE_BYTES = _to_bytes(SAMPLE_REVIEW_JP_DATE)
SAMPLE_REVIEW_NO_DATE_BYTES = _to_bytes(SAMPLE_REVIEW_NO_DATE)


# ---------------------------------------------------------------------------
# Kafka message envelopes (output of exporters, input of writers)
# ---------------------------------------------------------------------------

SAMPLE_KAFKA_ORG_MESSAGE = {
    "schema": "source_42",
    "source_name": "Bing Org Update",
    "org": {
        "place_id": "ChIJN1t_tDeuEmsRUsoyG83frY4",
        "name": "Google Sydney",
        "data_hash": "abc123",
        "city": "Sydney",
        "update_timestamp": "2024-03-15T10:00:00",
        "batch_id": 1710489600,
    },
    "contacts": {
        "place_id": "ChIJN1t_tDeuEmsRUsoyG83frY4",
        "phones": ["+61 2 9374 4000"],
        "web_link": "https://about.google/intl/en_au/",
    },
    "menu": [
        {"id": "menu_001", "name": "Coffee", "update_timestamp": "2024-03-15T10:00:00"},
    ],
}

SAMPLE_KAFKA_REVIEW_MESSAGE = {
    "schema": "source_33",
    "source_name": "Tripadvisor",
    "review": {
        "source_product_id": "prod_12345",
        "source_review_id": "rev_67890",
        "review_text": "Great experience!",
        "review_rating": 4.5,
        "review_date": "2024-03-15T00:00:00",
        "review_parse_date": "2024-03-15T10:00:00",
        "batch_timestamp": "2024-03-15T10:00:00",
        "batch_id": 1710489600,
        "source": "Tripadvisor",
    },
}
