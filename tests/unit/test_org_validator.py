"""Tests for org-exporter: OrgValidator batch validation logic."""

from unittest.mock import AsyncMock

import pytest

from services.exporter_base.base import ExporterConfig, ExporterMetrics
from services.org_exporter.main import OrgValidator
from tests.fixtures.sample_data import (
    SAMPLE_ORG_INVALID_JSON,
    SAMPLE_ORG_ITEM_BYTES,
    SAMPLE_ORG_ITEM_MINIMAL_BYTES,
    SAMPLE_ORG_ITEM_NO_PLACE_ID_BYTES,
)


@pytest.fixture
def config() -> ExporterConfig:
    return ExporterConfig(
        exporter_name="test_org",
        exporter_type="map",
        source_name="Test Source",
        schema="source_99",
    )


@pytest.fixture
def validator(config: ExporterConfig) -> OrgValidator:
    return OrgValidator(config)


@pytest.fixture
def metrics(config: ExporterConfig) -> ExporterMetrics:
    return ExporterMetrics(config.exporter_name, config.exporter_type)


@pytest.fixture
def mock_redis() -> AsyncMock:
    return AsyncMock()


class TestOrgValidatorBatch:
    """OrgValidator.validate_batch() — core pipeline logic."""

    async def test_valid_item(
        self,
        validator: OrgValidator,
        mock_redis: AsyncMock,
        metrics: ExporterMetrics,
    ) -> None:
        """Valid org item produces Kafka envelope with org/contacts/menu."""
        result = await validator.validate_batch(
            [SAMPLE_ORG_ITEM_BYTES],
            mock_redis,
            metrics,
        )
        assert len(result) == 1
        msg = result[0]
        assert msg["schema"] == "source_99"
        assert msg["source_name"] == "Test Source"
        assert msg["org"]["place_id"] == "ChIJN1t_tDeuEmsRUsoyG83frY4"
        assert msg["org"]["name"] == "Google Sydney"

    async def test_contacts_extracted(
        self,
        validator: OrgValidator,
        mock_redis: AsyncMock,
        metrics: ExporterMetrics,
    ) -> None:
        """Contacts are extracted from additional_info + phone/email/website."""
        result = await validator.validate_batch(
            [SAMPLE_ORG_ITEM_BYTES],
            mock_redis,
            metrics,
        )
        contacts = result[0]["contacts"]
        assert contacts["place_id"] == "ChIJN1t_tDeuEmsRUsoyG83frY4"
        assert contacts["phones"] == ["+61 2 9374 4000"]
        assert contacts["web_link"] == "https://about.google/intl/en_au/"
        assert contacts["facebook"] == "https://facebook.com/Google"

    async def test_menu_extracted(
        self,
        validator: OrgValidator,
        mock_redis: AsyncMock,
        metrics: ExporterMetrics,
    ) -> None:
        """Menu items are extracted and deduplicated within batch."""
        result = await validator.validate_batch(
            [SAMPLE_ORG_ITEM_BYTES],
            mock_redis,
            metrics,
        )
        menu = result[0]["menu"]
        assert len(menu) == 2
        assert menu[0]["id"] == "menu_001"
        assert "update_timestamp" in menu[0]

    async def test_data_hash_computed(
        self,
        validator: OrgValidator,
        mock_redis: AsyncMock,
        metrics: ExporterMetrics,
    ) -> None:
        """SHA1 data_hash is computed from key fields."""
        result = await validator.validate_batch(
            [SAMPLE_ORG_ITEM_BYTES],
            mock_redis,
            metrics,
        )
        org = result[0]["org"]
        assert "data_hash" in org
        assert len(org["data_hash"]) == 40  # SHA1 hex digest

    async def test_no_place_id_skipped(
        self,
        validator: OrgValidator,
        mock_redis: AsyncMock,
        metrics: ExporterMetrics,
    ) -> None:
        """Items without place_id are skipped."""
        result = await validator.validate_batch(
            [SAMPLE_ORG_ITEM_NO_PLACE_ID_BYTES],
            mock_redis,
            metrics,
        )
        assert len(result) == 0

    async def test_invalid_json_skipped(
        self,
        validator: OrgValidator,
        mock_redis: AsyncMock,
        metrics: ExporterMetrics,
    ) -> None:
        """Invalid JSON is logged and skipped, not raised."""
        result = await validator.validate_batch(
            [SAMPLE_ORG_INVALID_JSON],
            mock_redis,
            metrics,
        )
        assert len(result) == 0

    async def test_in_batch_dedup(
        self,
        validator: OrgValidator,
        mock_redis: AsyncMock,
        metrics: ExporterMetrics,
    ) -> None:
        """Duplicate place_ids within a batch are deduplicated."""
        result = await validator.validate_batch(
            [SAMPLE_ORG_ITEM_BYTES, SAMPLE_ORG_ITEM_BYTES],
            mock_redis,
            metrics,
        )
        assert len(result) == 1

    async def test_mixed_batch(
        self,
        validator: OrgValidator,
        mock_redis: AsyncMock,
        metrics: ExporterMetrics,
    ) -> None:
        """Batch with valid, invalid, and no-place-id items."""
        result = await validator.validate_batch(
            [
                SAMPLE_ORG_ITEM_BYTES,
                SAMPLE_ORG_INVALID_JSON,
                SAMPLE_ORG_ITEM_NO_PLACE_ID_BYTES,
                SAMPLE_ORG_ITEM_MINIMAL_BYTES,
            ],
            mock_redis,
            metrics,
        )
        assert len(result) == 2  # full + minimal
        place_ids = {r["org"]["place_id"] for r in result}
        assert "ChIJN1t_tDeuEmsRUsoyG83frY4" in place_ids
        assert "min_place_123" in place_ids

    async def test_minimal_item(
        self,
        validator: OrgValidator,
        mock_redis: AsyncMock,
        metrics: ExporterMetrics,
    ) -> None:
        """Minimal item (only place_id + name) passes validation."""
        result = await validator.validate_batch(
            [SAMPLE_ORG_ITEM_MINIMAL_BYTES],
            mock_redis,
            metrics,
        )
        assert len(result) == 1
        assert result[0]["org"]["place_id"] == "min_place_123"
        assert result[0]["contacts"] == {}
        assert result[0]["menu"] == []

    async def test_empty_batch(
        self,
        validator: OrgValidator,
        mock_redis: AsyncMock,
        metrics: ExporterMetrics,
    ) -> None:
        """Empty batch returns empty list."""
        result = await validator.validate_batch([], mock_redis, metrics)
        assert result == []


class TestOrgValidatorPartitionKey:
    """OrgValidator.partition_key() — Kafka partition routing."""

    def test_partition_key_from_place_id(self, validator: OrgValidator) -> None:
        item = {"org": {"place_id": "abc123"}}
        key = validator.partition_key(item)
        assert key == b"abc123"

    def test_partition_key_missing(self, validator: OrgValidator) -> None:
        item = {"org": {}}
        key = validator.partition_key(item)
        assert key is None

    def test_partition_key_no_org(self, validator: OrgValidator) -> None:
        item = {}
        key = validator.partition_key(item)
        assert key is None
