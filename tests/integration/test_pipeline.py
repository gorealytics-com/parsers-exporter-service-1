"""Integration tests requiring real Redis, PostgreSQL, ClickHouse, Kafka.

Run with:
    docker compose -f docker-compose.dev.yml up -d
    uv run pytest tests/integration/ -v -m integration

Environment variables (defaults match docker-compose.dev.yml):
    REDIS_URL=redis://localhost:16379/0
    PG_URI=postgresql://dev_user:dev_pass@localhost:15432/aragog_dev
    CLICKHOUSE_HOST=localhost
    CLICKHOUSE_PORT=18123
    KAFKA_BOOTSTRAP_SERVERS=localhost:19092
"""

import asyncio
import os

import pytest

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Skip if infra not available
# ---------------------------------------------------------------------------


def _redis_available() -> bool:
    try:
        import redis

        r = redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:16379/0"))
        r.ping()
        r.close()
        return True
    except Exception:
        return False


def _pg_available() -> bool:
    try:
        import asyncpg

        loop = asyncio.new_event_loop()
        conn = loop.run_until_complete(
            asyncpg.connect(
                os.environ.get(
                    "PG_URI",
                    "postgresql://dev_user:dev_pass@localhost:15432/aragog_dev",
                )
            )
        )
        loop.run_until_complete(conn.close())
        loop.close()
        return True
    except Exception:
        return False


skip_no_redis = pytest.mark.skipif(not _redis_available(), reason="Redis not available")
skip_no_pg = pytest.mark.skipif(not _pg_available(), reason="PostgreSQL not available")


# ---------------------------------------------------------------------------
# Redis + Exporter integration
# ---------------------------------------------------------------------------


@skip_no_redis
class TestRedisExporterIntegration:
    """Full cycle: push to Redis → OrgValidator.validate_batch → verify output."""

    async def test_push_and_validate(self) -> None:
        """Push JSON to Redis queue, pop via BLMPOP, validate with OrgValidator."""
        import redis.asyncio as aioredis

        from services.exporter_base.base import ExporterConfig, ExporterMetrics, json_encode
        from services.org_exporter.main import OrgValidator
        from tests.fixtures.sample_data import SAMPLE_ORG_ITEM

        redis_url = os.environ.get("REDIS_URL", "redis://localhost:16379/0")
        queue = "test:integration:org_items"

        # Push test data to Redis
        r = aioredis.from_url(redis_url, decode_responses=False)
        await r.delete(queue)
        await r.lpush(queue, json_encode(SAMPLE_ORG_ITEM))

        # Validate
        config = ExporterConfig(
            exporter_name="integration_test",
            schema="source_99",
            source_name="Integration Test",
        )
        validator = OrgValidator(config)
        metrics = ExporterMetrics("integration_test", "map")

        # Pop raw bytes
        result = await r.execute_command("BLMPOP", "1", "1", queue, "LEFT", "COUNT", "100")
        assert result is not None
        _queue, raw_items = result

        validated = await validator.validate_batch(raw_items, r, metrics)
        assert len(validated) == 1
        assert validated[0]["org"]["place_id"] == SAMPLE_ORG_ITEM["place_id"]
        assert validated[0]["schema"] == "source_99"

        await r.close()

    async def test_review_dedup_with_real_redis(self) -> None:
        """Review dedup: first SADD is new (1), second is duplicate (0)."""
        import redis.asyncio as aioredis

        from services.exporter_base.base import ExporterConfig, ExporterMetrics, json_encode
        from services.review_exporter.main import ReviewValidator
        from tests.fixtures.sample_data import SAMPLE_REVIEW_ITEM

        redis_url = os.environ.get("REDIS_URL", "redis://localhost:16379/0")
        dupefilter_key = "test:integration:dedup"

        r = aioredis.from_url(redis_url, decode_responses=False)
        await r.delete(dupefilter_key)

        config = ExporterConfig(
            exporter_name="integration_review",
            exporter_type="reviews",
            source_name="Tripadvisor",
            schema="source_33",
            dupefilter_key=dupefilter_key,
        )
        validator = ReviewValidator(config)
        metrics = ExporterMetrics("integration_review", "reviews")

        raw = json_encode(SAMPLE_REVIEW_ITEM)

        # First pass: new item
        result1 = await validator.validate_batch([raw], r, metrics)
        assert len(result1) == 1

        # Second pass: duplicate
        result2 = await validator.validate_batch([raw], r, metrics)
        assert len(result2) == 0

        # Cleanup
        await r.delete(dupefilter_key)
        await r.close()


# ---------------------------------------------------------------------------
# PostgreSQL + pg-writer integration
# ---------------------------------------------------------------------------


@skip_no_pg
class TestPgWriterIntegration:
    """COPY-based upsert into real PostgreSQL."""

    async def test_copy_upsert_basic(self) -> None:
        """COPY upsert creates and updates records correctly."""
        import asyncpg

        pg_uri = os.environ.get(
            "PG_URI",
            "postgresql://dev_user:dev_pass@localhost:15432/aragog_dev",
        )
        conn = await asyncpg.connect(pg_uri)

        # Setup: create test schema and table
        await conn.execute("CREATE SCHEMA IF NOT EXISTS test_integration")
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS test_integration.org_test (
                place_id TEXT PRIMARY KEY,
                name TEXT,
                city TEXT,
                data_hash TEXT
            )
        """
        )
        await conn.execute("TRUNCATE test_integration.org_test")

        import sys

        sys.path.insert(
            0,
            os.path.join(os.path.dirname(__file__), "..", "..", "services", "pg-writer"),
        )
        from services.pg_writer.main import copy_upsert

        # Insert
        records = [
            {
                "place_id": "test_1",
                "name": "Place 1",
                "city": "Berlin",
                "data_hash": "aaa",
            },
            {
                "place_id": "test_2",
                "name": "Place 2",
                "city": "Tokyo",
                "data_hash": "bbb",
            },
        ]

        async with conn.transaction():
            written = await copy_upsert(conn, "test_integration", "org_test", records, ["place_id"])
        assert written == 2

        # Verify
        rows = await conn.fetch("SELECT * FROM test_integration.org_test ORDER BY place_id")
        assert len(rows) == 2
        assert rows[0]["name"] == "Place 1"

        # Update (same place_id, different data)
        records_updated = [
            {
                "place_id": "test_1",
                "name": "Updated Place 1",
                "city": "Munich",
                "data_hash": "ccc",
            },
        ]
        async with conn.transaction():
            written = await copy_upsert(
                conn,
                "test_integration",
                "org_test",
                records_updated,
                ["place_id"],
            )

        row = await conn.fetchrow("SELECT * FROM test_integration.org_test WHERE place_id = 'test_1'")
        assert row["name"] == "Updated Place 1"
        assert row["city"] == "Munich"

        # Cleanup
        await conn.execute("DROP TABLE test_integration.org_test")
        await conn.execute("DROP SCHEMA test_integration")
        await conn.close()

    async def test_copy_upsert_empty_batch(self) -> None:
        """Empty batch returns 0, no errors."""
        import asyncpg

        pg_uri = os.environ.get(
            "PG_URI",
            "postgresql://dev_user:dev_pass@localhost:15432/aragog_dev",
        )
        conn = await asyncpg.connect(pg_uri)

        import sys

        sys.path.insert(
            0,
            os.path.join(os.path.dirname(__file__), "..", "..", "services", "pg-writer"),
        )
        from services.pg_writer.main import copy_upsert

        written = await copy_upsert(conn, "public", "nonexistent", [], ["id"])
        assert written == 0

        await conn.close()

    async def test_copy_upsert_mixed_columns(self) -> None:
        """Records with different column subsets are handled correctly."""
        import asyncpg

        pg_uri = os.environ.get(
            "PG_URI",
            "postgresql://dev_user:dev_pass@localhost:15432/aragog_dev",
        )
        conn = await asyncpg.connect(pg_uri)

        await conn.execute("CREATE SCHEMA IF NOT EXISTS test_mixed")
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS test_mixed.org_test (
                place_id TEXT PRIMARY KEY,
                name TEXT,
                city TEXT
            )
        """
        )
        await conn.execute("TRUNCATE test_mixed.org_test")

        import sys

        sys.path.insert(
            0,
            os.path.join(os.path.dirname(__file__), "..", "..", "services", "pg-writer"),
        )
        from services.pg_writer.main import copy_upsert

        # Mixed: one record has city, another doesn't
        records = [
            {"place_id": "mix_1", "name": "A", "city": "Berlin"},
            {"place_id": "mix_2", "name": "B"},  # no city
        ]

        async with conn.transaction():
            written = await copy_upsert(conn, "test_mixed", "org_test", records, ["place_id"])
        assert written == 2

        rows = await conn.fetch("SELECT * FROM test_mixed.org_test ORDER BY place_id")
        assert len(rows) == 2

        await conn.execute("DROP TABLE test_mixed.org_test")
        await conn.execute("DROP SCHEMA test_mixed")
        await conn.close()
