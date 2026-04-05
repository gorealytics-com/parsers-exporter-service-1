"""Tests for libs/common/schemas/ — msgspec Struct validation."""

import msgspec

from libs.common.schemas import Contact, MenuItem, OrgData, Review


class TestOrgData:
    """OrgData Struct: 27-field organisation record."""

    def test_create_minimal(self) -> None:
        """OrgData requires only place_id, source_name, name."""
        org = OrgData(place_id="abc", source_name="Test", name="Test Org")
        assert org.place_id == "abc"
        assert org.name == "Test Org"
        assert org.city is None
        assert org.rating is None

    def test_create_full(self) -> None:
        """OrgData with all optional fields populated."""
        org = OrgData(
            place_id="abc",
            source_name="Test",
            name="Full Org",
            city="Berlin",
            country="DE",
            latitude=52.52,
            longitude=13.405,
            rating=4.5,
        )
        assert org.city == "Berlin"
        assert org.latitude == 52.52

    def test_omit_defaults_serialization(self) -> None:
        """Fields with None values are omitted from JSON output."""
        org = OrgData(place_id="abc", source_name="Test", name="Test Org")
        data = msgspec.json.encode(org)
        decoded = msgspec.json.decode(data)
        assert "city" not in decoded
        assert "rating" not in decoded
        assert decoded["place_id"] == "abc"

    def test_json_roundtrip(self) -> None:
        """Encode → decode roundtrip preserves all fields."""
        org = OrgData(
            place_id="rt_test",
            source_name="RT",
            name="Roundtrip",
            city="Tokyo",
            rating=3.7,
        )
        data = msgspec.json.encode(org)
        restored = msgspec.json.decode(data, type=OrgData)
        assert restored.place_id == "rt_test"
        assert restored.city == "Tokyo"
        assert restored.rating == 3.7

    def test_gc_false_flag(self) -> None:
        """gc=False prevents GC tracking (75x faster GC pauses)."""
        import gc

        org = OrgData(place_id="gc_test", source_name="GC", name="GC Test")
        assert not gc.is_tracked(org)


class TestContact:
    """Contact Struct: 22-field contact record."""

    def test_create_minimal(self) -> None:
        contact = Contact(place_id="abc")
        assert contact.place_id == "abc"
        assert contact.phones is None

    def test_with_phones_and_socials(self) -> None:
        contact = Contact(
            place_id="abc",
            phones=["+1-555-0100", "+1-555-0101"],
            facebook="https://facebook.com/test",
        )
        assert len(contact.phones) == 2
        assert contact.facebook.startswith("https://")


class TestMenuItem:
    """MenuItem Struct: 18-field menu item."""

    def test_create_minimal(self) -> None:
        item = MenuItem(id="m001")
        assert item.id == "m001"
        assert item.price is None

    def test_with_price(self) -> None:
        item = MenuItem(id="m002", name="Latte", price=4.50, currency="USD")
        assert item.price == 4.50


class TestReview:
    """Review Struct: 18-field review for ClickHouse."""

    def test_create_minimal(self) -> None:
        review = Review()
        assert review.source_product_id is None

    def test_full_review(self) -> None:
        review = Review(
            source_product_id="prod_1",
            source_review_id="rev_1",
            review_text="Great!",
            review_rating=4.5,
            review_date="2024-03-15T00:00:00",
            source="Tripadvisor",
        )
        assert review.review_rating == 4.5

    def test_gc_false(self) -> None:
        import gc

        review = Review(source_review_id="gc_rev")
        assert not gc.is_tracked(review)

    def test_json_roundtrip(self) -> None:
        review = Review(
            source_review_id="rt_rev",
            review_text="Test",
            review_rating=3.0,
        )
        data = msgspec.json.encode(review)
        restored = msgspec.json.decode(data, type=Review)
        assert restored.source_review_id == "rt_rev"
        assert restored.review_rating == 3.0
