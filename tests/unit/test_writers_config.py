"""Tests for pg-writer and ch-writer configuration and JSON helpers."""

# Import pg-writer module
import importlib
import os
import sys
from unittest.mock import patch

# We need to import from the service directories
pg_writer_path = os.path.join(os.path.dirname(__file__), "..", "..", "services", "pg-writer")
ch_writer_path = os.path.join(os.path.dirname(__file__), "..", "..", "services", "ch-writer")
sys.path.insert(0, pg_writer_path)
sys.path.insert(0, ch_writer_path)


class TestPgWriterConfig:
    """pg-writer WriterConfig from env vars."""

    def test_defaults(self) -> None:
        from main import WriterConfig

        config = WriterConfig()
        assert config.batch_size == 1000
        assert config.flush_interval == 5.0
        assert config.kafka_topic == "orgs.validated.v1"
        assert config.consumer_group == "aragog-pg-writers"
        assert config.pg_min_pool == 2
        assert config.pg_max_pool == 10

    def test_from_env(self) -> None:
        from main import WriterConfig

        env = {
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
            "KAFKA_TOPIC": "custom.topic",
            "KAFKA_CONSUMER_GROUP": "my-group",
            "PG_URI": "postgresql://user:pass@host/db",
            "BATCH_SIZE": "5000",
            "FLUSH_INTERVAL_MS": "3000",
            "PG_POOL_MIN": "3",
            "PG_POOL_MAX": "15",
        }
        with patch.dict(os.environ, env, clear=False):
            config = WriterConfig.from_env()
        assert config.kafka_topic == "custom.topic"
        assert config.batch_size == 5000
        assert config.flush_interval == 3.0
        assert config.pg_min_pool == 3


class TestPgWriterJsonHelpers:
    """pg-writer json_encode/json_decode (same pattern as exporter-base)."""

    def test_encode_decode_roundtrip(self) -> None:
        # Re-import from pg-writer's own module
        sys.path.insert(0, pg_writer_path)
        from main import json_decode, json_encode

        data = {"schema": "source_42", "org": {"place_id": "abc"}}
        assert json_decode(json_encode(data)) == data


class TestChWriterConfig:
    """ch-writer WriterConfig from env vars."""

    def test_defaults(self) -> None:
        sys.path.insert(0, ch_writer_path)
        # Need to import ch-writer's WriterConfig separately
        spec = importlib.util.spec_from_file_location("ch_main", os.path.join(ch_writer_path, "main.py"))
        ch_mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(ch_mod)

        config = ch_mod.WriterConfig()
        assert config.batch_size == 50000
        assert config.flush_interval == 10.0
        assert config.kafka_topic == "reviews.validated.v1"
        assert config.consumer_group == "aragog-ch-writers"
        assert config.max_partitions == 10000

    def test_from_env(self) -> None:
        env = {
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
            "CLICKHOUSE_HOST": "ch-host",
            "CLICKHOUSE_PORT": "9000",
            "CLICKHOUSE_USER": "admin",
            "CLICKHOUSE_PASSWORD": "secret",
            "BATCH_SIZE": "100000",
        }
        spec = importlib.util.spec_from_file_location("ch_main2", os.path.join(ch_writer_path, "main.py"))
        ch_mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(ch_mod)

        with patch.dict(os.environ, env, clear=False):
            config = ch_mod.WriterConfig.from_env()
        assert config.ch_host == "ch-host"
        assert config.batch_size == 100000
