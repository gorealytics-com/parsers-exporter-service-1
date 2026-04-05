"""Tests for exporter_base.base: json helpers, ExporterConfig, ExporterMetrics."""

import os
from unittest.mock import patch

import pytest

from services.exporter_base.base import ExporterConfig, ExporterMetrics, json_decode, json_encode


class TestJsonHelpers:
    """json_encode/json_decode: msgspec-based serialization."""

    def test_encode_dict(self) -> None:
        """Encode simple dict to JSON bytes."""
        result = json_encode({"key": "value", "num": 42})
        assert isinstance(result, bytes)
        assert b'"key"' in result

    def test_decode_dict(self) -> None:
        """Decode JSON bytes to dict."""
        result = json_decode(b'{"key": "value", "num": 42}')
        assert result == {"key": "value", "num": 42}

    def test_roundtrip(self) -> None:
        """Encode → decode roundtrip preserves data."""
        original = {"name": "test", "items": [1, 2, 3], "nested": {"a": True}}
        encoded = json_encode(original)
        decoded = json_decode(encoded)
        assert decoded == original

    def test_encode_list(self) -> None:
        """Encode list to JSON bytes."""
        result = json_encode([1, "two", 3.0, None])
        decoded = json_decode(result)
        assert decoded == [1, "two", 3.0, None]

    def test_encode_none_values(self) -> None:
        """None values are preserved as JSON null."""
        result = json_encode({"key": None})
        decoded = json_decode(result)
        assert decoded["key"] is None

    def test_decode_invalid_json(self) -> None:
        """Invalid JSON raises msgspec.DecodeError."""
        import msgspec

        with pytest.raises(msgspec.DecodeError):
            json_decode(b"{broken}")

    def test_encode_unicode(self) -> None:
        """Unicode strings survive roundtrip."""
        data = {"text": "Отличный отзыв 🌟", "jp": "素晴らしい"}
        decoded = json_decode(json_encode(data))
        assert decoded["text"] == "Отличный отзыв 🌟"
        assert decoded["jp"] == "素晴らしい"

    def test_encode_empty_dict(self) -> None:
        result = json_encode({})
        assert json_decode(result) == {}

    def test_encode_large_payload(self) -> None:
        """Large payloads (1000 items) encode without issues."""
        data = {f"key_{i}": f"value_{i}" for i in range(1000)}
        decoded = json_decode(json_encode(data))
        assert len(decoded) == 1000


class TestExporterConfig:
    """ExporterConfig: msgspec.Struct loaded from env vars."""

    def test_defaults(self) -> None:
        """Default values are sensible."""
        config = ExporterConfig()
        assert config.batch_size == 10_000
        assert config.check_interval == 30
        assert config.kafka_compression == "lz4"
        assert config.kafka_acks == "all"
        assert config.health_port == 8080

    def test_from_env(self) -> None:
        """from_env() reads environment variables."""
        env = {
            "EXPORTER_NAME": "bing_map_search",
            "EXPORTER_TYPE": "map",
            "SOURCE_NAME": "Bing Org Update",
            "SCHEMA": "source_42",
            "REDIS_URL": "redis://redis:6379/0",
            "REDIS_QUEUE": "bing_map_search:items",
            "BATCH_SIZE": "5000",
            "CHECK_INTERVAL": "15",
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
            "KAFKA_TOPIC": "orgs.validated.v1",
        }
        with patch.dict(os.environ, env, clear=False):
            config = ExporterConfig.from_env()

        assert config.exporter_name == "bing_map_search"
        assert config.exporter_type == "map"
        assert config.schema == "source_42"
        assert config.batch_size == 5000
        assert config.check_interval == 15
        assert config.redis_queue == "bing_map_search:items"

    def test_from_env_defaults(self) -> None:
        """Missing env vars use defaults."""
        with patch.dict(os.environ, {}, clear=True):
            config = ExporterConfig.from_env()
        assert config.exporter_name == "unknown"
        assert config.batch_size == 10_000


class TestExporterMetrics:
    """ExporterMetrics: Prometheus counters."""

    def test_create(self) -> None:
        """Metrics object creates without error."""
        metrics = ExporterMetrics("test", "map")
        assert metrics._name == "test"

    def test_inc_produced(self) -> None:
        """inc_produced increments counter."""
        metrics = ExporterMetrics("test_inc", "map")
        metrics.inc_produced(10)
        # Prometheus counter is global; just verify no exception

    def test_inc_skipped(self) -> None:
        metrics = ExporterMetrics("test_skip", "map")
        metrics.inc_skipped("no_place_id", 5)

    def test_inc_kafka_errors(self) -> None:
        metrics = ExporterMetrics("test_err", "map")
        metrics.inc_kafka_errors()
