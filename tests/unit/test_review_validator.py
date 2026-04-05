"""Tests for review-exporter: ReviewValidator validation and dedup logic."""

from unittest.mock import AsyncMock

import pytest

from services.exporter_base.base import ExporterConfig, ExporterMetrics
from services.review_exporter.main import ReviewValidator
from tests.fixtures.sample_data import (
    SAMPLE_REVIEW_BYTES,
    SAMPLE_REVIEW_JP_DATE_BYTES,
    SAMPLE_REVIEW_NO_DATE_BYTES,
    SAMPLE_REVIEW_OLD_DATE_BYTES,
)


@pytest.fixture
def config() -> ExporterConfig:
    return ExporterConfig(
        exporter_name="test_reviews",
        exporter_type="reviews",
        source_name="Tripadvisor",
        schema="source_33",
        dupefilter_key="tripadvisor:reviews_dupefilter",
    )


@pytest.fixture
def config_no_dedup() -> ExporterConfig:
    return ExporterConfig(
        exporter_name="test_reviews",
        exporter_type="reviews",
        source_name="Tripadvisor",
        schema="source_33",
        dupefilter_key="",
    )


@pytest.fixture
def validator(config: ExporterConfig) -> ReviewValidator:
    return ReviewValidator(config)


@pytest.fixture
def validator_no_dedup(config_no_dedup: ExporterConfig) -> ReviewValidator:
    return ReviewValidator(config_no_dedup)


@pytest.fixture
def metrics(config: ExporterConfig) -> ExporterMetrics:
    return ExporterMetrics(config.exporter_name, config.exporter_type)


@pytest.fixture
def mock_redis_new() -> AsyncMock:
    """Mock Redis where SADD returns 1 (new item)."""
    redis = AsyncMock()
    pipe = AsyncMock()
    pipe.execute = AsyncMock(return_value=[1])
    pipe.__aenter__ = AsyncMock(return_value=pipe)
    pipe.__aexit__ = AsyncMock(return_value=False)
    redis.pipeline = AsyncMock(return_value=pipe)
    return redis


@pytest.fixture
def mock_redis_duplicate() -> AsyncMock:
    """Mock Redis where SADD returns 0 (already seen)."""
    redis = AsyncMock()
    pipe = AsyncMock()
    pipe.execute = AsyncMock(return_value=[0])
    pipe.__aenter__ = AsyncMock(return_value=pipe)
    pipe.__aexit__ = AsyncMock(return_value=False)
    redis.pipeline = AsyncMock(return_value=pipe)
    return redis


class TestReviewValidation:
    """ReviewValidator.validate_batch() — review processing logic."""

    async def test_valid_review(self, validator_no_dedup: ReviewValidator, metrics: ExporterMetrics) -> None:
        """Valid review item produces Kafka envelope."""
        redis = AsyncMock()
        result = await validator_no_dedup.validate_batch(
            [SAMPLE_REVIEW_BYTES],
            redis,
            metrics,
        )
        assert len(result) == 1
        msg = result[0]
        assert msg["schema"] == "source_33"
        assert msg["source_name"] == "Tripadvisor"
        assert msg["review"]["source_review_id"] == "rev_67890"
        assert msg["review"]["review_rating"] == 4.5

    async def test_old_date_filtered(self, validator_no_dedup: ReviewValidator, metrics: ExporterMetrics) -> None:
        """Reviews before 2020 are filtered out."""
        redis = AsyncMock()
        result = await validator_no_dedup.validate_batch(
            [SAMPLE_REVIEW_OLD_DATE_BYTES],
            redis,
            metrics,
        )
        assert len(result) == 0

    async def test_no_date_filtered(self, validator_no_dedup: ReviewValidator, metrics: ExporterMetrics) -> None:
        """Reviews without review_date are filtered out."""
        redis = AsyncMock()
        result = await validator_no_dedup.validate_batch(
            [SAMPLE_REVIEW_NO_DATE_BYTES],
            redis,
            metrics,
        )
        assert len(result) == 0

    async def test_japanese_date_format(self, validator_no_dedup: ReviewValidator, metrics: ExporterMetrics) -> None:
        """Japanese date format (2024年03月15日) is parsed correctly."""
        redis = AsyncMock()
        result = await validator_no_dedup.validate_batch(
            [SAMPLE_REVIEW_JP_DATE_BYTES],
            redis,
            metrics,
        )
        assert len(result) == 1
        assert "2024-03-15" in result[0]["review"]["review_date"]

    async def test_rating_normalized_to_float(
        self, validator_no_dedup: ReviewValidator, metrics: ExporterMetrics
    ) -> None:
        """String rating "4.5" is converted to float 4.5."""
        redis = AsyncMock()
        result = await validator_no_dedup.validate_batch(
            [SAMPLE_REVIEW_BYTES],
            redis,
            metrics,
        )
        assert isinstance(result[0]["review"]["review_rating"], float)

    async def test_additional_info_serialized(
        self, validator_no_dedup: ReviewValidator, metrics: ExporterMetrics
    ) -> None:
        """Dict additional_info is serialized to JSON string."""
        redis = AsyncMock()
        result = await validator_no_dedup.validate_batch(
            [SAMPLE_REVIEW_BYTES],
            redis,
            metrics,
        )
        ai = result[0]["review"]["additional_info"]
        assert isinstance(ai, str)
        assert "helpful_votes" in ai

    async def test_empty_batch(self, validator_no_dedup: ReviewValidator, metrics: ExporterMetrics) -> None:
        redis = AsyncMock()
        result = await validator_no_dedup.validate_batch([], redis, metrics)
        assert result == []


class TestReviewDedup:
    """Redis SADD pipeline deduplication."""

    async def test_new_item_passes(
        self,
        validator: ReviewValidator,
        mock_redis_new: AsyncMock,
        metrics: ExporterMetrics,
    ) -> None:
        """New review (SADD=1) passes dedup filter."""
        result = await validator.validate_batch(
            [SAMPLE_REVIEW_BYTES],
            mock_redis_new,
            metrics,
        )
        assert len(result) == 1

    async def test_duplicate_filtered(
        self,
        validator: ReviewValidator,
        mock_redis_duplicate: AsyncMock,
        metrics: ExporterMetrics,
    ) -> None:
        """Duplicate review (SADD=0) is filtered out."""
        result = await validator.validate_batch(
            [SAMPLE_REVIEW_BYTES],
            mock_redis_duplicate,
            metrics,
        )
        assert len(result) == 0

    async def test_no_dedup_key_skips_pipeline(
        self, validator_no_dedup: ReviewValidator, metrics: ExporterMetrics
    ) -> None:
        """Without dupefilter_key, all items pass (no SADD call)."""
        redis = AsyncMock()
        result = await validator_no_dedup.validate_batch(
            [SAMPLE_REVIEW_BYTES],
            redis,
            metrics,
        )
        assert len(result) == 1
        # Redis pipeline should NOT have been called
        redis.pipeline.assert_not_called()


class TestReviewPartitionKey:
    """ReviewValidator.partition_key() — Kafka partition routing."""

    def test_partition_key(self, validator: ReviewValidator) -> None:
        item = {"review": {"source_review_id": "rev_123"}}
        assert validator.partition_key(item) == b"rev_123"

    def test_partition_key_missing(self, validator: ReviewValidator) -> None:
        item = {"review": {}}
        assert validator.partition_key(item) is None
