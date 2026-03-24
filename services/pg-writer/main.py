"""pg-writer: Kafka consumer -> PostgreSQL batch writer.

Consumes from: orgs.validated.v1
Writes to: PostgreSQL (via PgBouncer) using asyncpg

Ported from: CartographyOrgDataExporter._bulk_insert_update + _create_pg_session

Tables written per message:
  - {schema}.organisation_data   (conflict key: place_id)
  - {schema}.organisation_contacts (conflict key: place_id)
  - {schema}.menu_data            (conflict key: id)
"""

import asyncio
import os
import signal
import sys
import time
from collections import defaultdict
from typing import Any, Optional

import asyncpg
import orjson
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from loguru import logger
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)


# ---------------------------------------------------------------------------
# Logging setup (loguru)
# ---------------------------------------------------------------------------

def setup_logging(level: str = "INFO") -> None:
    """Configure loguru JSON logging to stdout for Promtail/Loki ingestion."""
    logger.remove()
    logger.add(
        sys.stdout,
        format="{message}",
        serialize=True,
        level=os.environ.get("LOG_LEVEL", level).upper(),
    )


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

class WriterConfig:
    """pg-writer runtime configuration from environment variables."""

    def __init__(self) -> None:
        self.kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
        self.kafka_topic = os.environ.get("KAFKA_TOPIC", "orgs.validated.v1")
        self.consumer_group = os.environ.get("KAFKA_CONSUMER_GROUP", "aragog-pg-writers")
        self.pg_dsn = os.environ.get("PG_URI", "")  # points to PgBouncer
        self.batch_size = int(os.environ.get("BATCH_SIZE", "1000"))
        self.flush_interval = float(os.environ.get("FLUSH_INTERVAL_MS", "5000")) / 1000
        self.insert_chunk_size = int(os.environ.get("INSERT_CHUNK_SIZE", "1000"))
        self.health_port = int(os.environ.get("HEALTH_PORT", "8080"))
        self.pg_min_pool = int(os.environ.get("PG_POOL_MIN", "2"))
        self.pg_max_pool = int(os.environ.get("PG_POOL_MAX", "10"))
        self.dlq_topic = os.environ.get("DLQ_TOPIC", "orgs.dlq.v1")


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

records_written = Counter(
    "aragog_writer_records_written_total",
    "Records written to PostgreSQL",
    ["writer", "schema", "table"],
)
write_errors = Counter(
    "aragog_writer_errors_total",
    "Database write failures",
    ["writer", "schema", "table"],
)
batch_duration = Histogram(
    "aragog_writer_batch_write_seconds",
    "Batch write duration",
    ["writer"],
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60],
)
buffer_size = Gauge(
    "aragog_writer_buffer_size",
    "Records buffered in memory",
    ["writer"],
)


# ---------------------------------------------------------------------------
# SQL Builder — port of _bulk_insert_update logic
# ---------------------------------------------------------------------------

def build_upsert_sql(
    schema: str,
    table: str,
    columns: list[str],
    conflict_columns: list[str],
) -> str:
    """Build INSERT ... ON CONFLICT DO UPDATE SET ... SQL.

    Ported from CartographyOrgDataExporter._bulk_insert_update:
    the original groups by key set and builds per-group ON CONFLICT statements.
    Here we do the same but with asyncpg $N placeholders.
    """
    placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
    col_list = ", ".join(f'"{c}"' for c in columns)
    conflict_list = ", ".join(f'"{c}"' for c in conflict_columns)

    update_cols = [c for c in columns if c not in conflict_columns]
    update_set = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)

    if update_set:
        return (
            f'INSERT INTO "{schema}"."{table}" ({col_list}) '
            f"VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_list}) DO UPDATE SET {update_set}"
        )
    else:
        return (
            f'INSERT INTO "{schema}"."{table}" ({col_list}) '
            f"VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_list}) DO NOTHING"
        )


# ---------------------------------------------------------------------------
# PG Writer
# ---------------------------------------------------------------------------

class PgWriter:
    """Kafka consumer that micro-batches org records and writes to PostgreSQL.

    Flow:
      1. Consume messages from Kafka (aiokafka)
      2. Buffer in memory, grouped by schema
      3. Flush on batch_size reached OR flush_interval elapsed
      4. Write via asyncpg executemany through PgBouncer
      5. Commit Kafka offsets after successful write
    """

    def __init__(self, config: WriterConfig) -> None:
        self.config = config
        self._running = False
        self._ready = asyncio.Event()

        self._buffer: list[dict[str, Any]] = []
        self._last_flush = time.monotonic()

        self._consumer: AIOKafkaConsumer | None = None
        self._pool: asyncpg.Pool | None = None
        self._dlq_producer: AIOKafkaProducer | None = None
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        """Connect to PG/Kafka and enter the consume loop."""
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, self._request_shutdown)
        loop.add_signal_handler(signal.SIGINT, self._request_shutdown)

        # --- asyncpg pool (connects to PgBouncer) ---
        logger.info("Connecting to PostgreSQL (via PgBouncer)")
        self._pool = await asyncpg.create_pool(
            dsn=self.config.pg_dsn,
            min_size=self.config.pg_min_pool,
            max_size=self.config.pg_max_pool,
            command_timeout=60,
        )
        logger.info(f"PG pool created: min={self.config.pg_min_pool} max={self.config.pg_max_pool}")

        # --- Kafka consumer ---
        logger.info(f"Starting Kafka consumer: topic={self.config.kafka_topic} group={self.config.consumer_group}")
        self._consumer = AIOKafkaConsumer(
            self.config.kafka_topic,
            bootstrap_servers=self.config.kafka_bootstrap,
            group_id=self.config.consumer_group,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=lambda v: orjson.loads(v),
            max_poll_records=self.config.batch_size,
        )
        await self._consumer.start()
        logger.info("Kafka consumer started")

        # --- DLQ producer ---
        self._dlq_producer = AIOKafkaProducer(
            bootstrap_servers=self.config.kafka_bootstrap,
            value_serializer=lambda v: orjson.dumps(v),
        )
        await self._dlq_producer.start()
        logger.info(f"DLQ producer started, topic={self.config.dlq_topic}")

        # --- Health server ---
        self._tasks.append(asyncio.create_task(
            self._run_health_server(), name="health-server",
        ))

        # --- Flush timer ---
        self._tasks.append(asyncio.create_task(
            self._flush_timer(), name="flush-timer",
        ))

        self._running = True
        self._ready.set()
        logger.info("PG Writer ready")

        try:
            await self._consume_loop()
        finally:
            await self.stop()

    async def _consume_loop(self) -> None:
        """Main consumer loop."""
        while self._running:
            try:
                batch = await self._consumer.getmany(
                    timeout_ms=1000,
                    max_records=self.config.batch_size,
                )

                for tp, messages in batch.items():
                    for msg in messages:
                        self._buffer.append(msg.value)

                buffer_size.labels(writer="pg").set(len(self._buffer))

                if len(self._buffer) >= self.config.batch_size:
                    await self._flush()

            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error in consume loop")
                await asyncio.sleep(2)

    async def _flush_timer(self) -> None:
        """Periodic flush to avoid holding data too long."""
        while self._running:
            await asyncio.sleep(1)
            elapsed = time.monotonic() - self._last_flush
            if elapsed >= self.config.flush_interval and self._buffer:
                logger.debug(f"Flush timer triggered ({len(self._buffer)} items)")
                await self._flush()

    async def _flush(self) -> None:
        """Write buffered records to PostgreSQL and commit Kafka offsets."""
        if not self._buffer:
            return

        to_write = self._buffer[:]
        self._buffer.clear()
        buffer_size.labels(writer="pg").set(0)

        t0 = time.monotonic()
        logger.info(f"Flushing {len(to_write)} records to PostgreSQL")

        by_schema: dict[str, dict[str, list]] = defaultdict(
            lambda: {"orgs": [], "contacts": [], "menu": []}
        )

        for msg in to_write:
            schema = msg.get("schema", "default")
            if msg.get("org"):
                by_schema[schema]["orgs"].append(msg["org"])
            if msg.get("contacts") and msg["contacts"].get("place_id"):
                by_schema[schema]["contacts"].append(msg["contacts"])
            if msg.get("menu"):
                by_schema[schema]["menu"].extend(msg["menu"])

        success = True
        async with self._pool.acquire() as conn:
            for schema, tables in by_schema.items():
                try:
                    async with conn.transaction():
                        if tables["orgs"]:
                            await self._write_table(
                                conn, schema, "organisation_data",
                                tables["orgs"], ["place_id"],
                            )
                        if tables["contacts"]:
                            await self._write_table(
                                conn, schema, "organisation_contacts",
                                tables["contacts"], ["place_id"],
                            )
                        if tables["menu"]:
                            await self._write_table(
                                conn, schema, "menu_data",
                                tables["menu"], ["id"],
                            )
                except Exception:
                    logger.exception(f"Failed to write schema={schema}")
                    write_errors.labels(writer="pg", schema=schema, table="all").inc()
                    success = False
                    await self._send_to_dlq(schema, tables)

        if success:
            try:
                await self._consumer.commit()
            except Exception:
                logger.exception("Kafka commit failed")

        duration = time.monotonic() - t0
        batch_duration.labels(writer="pg").observe(duration)
        self._last_flush = time.monotonic()
        logger.info(f"Flush complete: {len(to_write)} records in {duration:.2f}s")

    async def _write_table(
        self,
        conn: asyncpg.Connection,
        schema: str,
        table: str,
        records: list[dict[str, Any]],
        conflict_columns: list[str],
    ) -> None:
        """Batch upsert records into a specific table.

        Ported from CartographyOrgDataExporter._bulk_insert_update:
        group by key set, build INSERT ON CONFLICT SQL, execute in chunks.
        """
        if not records:
            return

        records = [r for r in records if r]
        if not records:
            return

        grouped: dict[tuple, list[dict]] = defaultdict(list)
        for record in records:
            keys = tuple(sorted(record.keys()))
            grouped[keys].append(record)

        total_written = 0

        for columns_tuple, group_records in grouped.items():
            columns = list(columns_tuple)
            sql = build_upsert_sql(schema, table, columns, conflict_columns)

            for i in range(0, len(group_records), self.config.insert_chunk_size):
                chunk = group_records[i : i + self.config.insert_chunk_size]
                rows = [tuple(rec.get(col) for col in columns) for rec in chunk]

                try:
                    await conn.executemany(sql, rows)
                    total_written += len(chunk)
                except Exception:
                    logger.exception(f"Upsert failed: {schema}.{table} ({len(chunk)} records)")
                    write_errors.labels(writer="pg", schema=schema, table=table).inc(len(chunk))

        records_written.labels(writer="pg", schema=schema, table=table).inc(total_written)
        logger.debug(f"Wrote {total_written} records to {schema}.{table}")

    async def _send_to_dlq(self, schema: str, tables: dict) -> None:
        """Send failed records to dead-letter topic for later reprocessing."""
        if not self._dlq_producer:
            return
        try:
            for table_name, records in tables.items():
                for record in records:
                    await self._dlq_producer.send(
                        self.config.dlq_topic,
                        value={"schema": schema, "table": table_name, "record": record},
                    )
            await self._dlq_producer.flush()
            logger.info(f"Sent {sum(len(r) for r in tables.values())} failed records to DLQ for schema={schema}")
        except Exception:
            logger.exception("Failed to send to DLQ")

    async def stop(self) -> None:
        """Flush remaining buffer and shut down connections."""
        logger.info("Stopping PG Writer")
        self._running = False
        self._ready.clear()

        if self._buffer:
            logger.info(f"Flushing {len(self._buffer)} remaining buffered records")
            await self._flush()

        if self._consumer:
            try:
                await self._consumer.stop()
            except Exception:
                logger.exception("Error stopping Kafka consumer")

        if self._dlq_producer:
            try:
                await self._dlq_producer.stop()
            except Exception:
                logger.exception("Error stopping DLQ producer")

        if self._pool:
            try:
                await self._pool.close()
            except Exception:
                logger.exception("Error closing PG pool")

        for t in self._tasks:
            t.cancel()

        logger.info("PG Writer stopped")

    def _request_shutdown(self) -> None:
        """Handle SIGTERM/SIGINT."""
        logger.info("Shutdown signal received")
        self._running = False
        self._ready.clear()

    # ---- Health server --------------------------------------------------

    async def _run_health_server(self) -> None:
        """Run K8s probe and Prometheus metrics HTTP server."""
        from fastapi import FastAPI
        from fastapi.responses import PlainTextResponse
        import uvicorn

        app = FastAPI(docs_url=None, redoc_url=None)

        @app.get("/health")
        async def health() -> dict[str, str]:
            return {"status": "ok", "service": "pg-writer"}

        @app.get("/ready")
        async def ready() -> dict[str, str] | PlainTextResponse:
            if not self._ready.is_set():
                return PlainTextResponse("not ready", status_code=503)
            return {"status": "ready"}

        @app.get("/metrics")
        async def metrics() -> PlainTextResponse:
            return PlainTextResponse(
                generate_latest().decode("utf-8"),
                media_type=CONTENT_TYPE_LATEST,
            )

        server = uvicorn.Server(uvicorn.Config(
            app, host="0.0.0.0", port=self.config.health_port,
            log_level="warning", access_log=False,
        ))
        await server.serve()


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

async def main() -> None:
    """Start the pg-writer service."""
    setup_logging(level=os.environ.get("LOG_LEVEL", "INFO"))
    config = WriterConfig()
    writer = PgWriter(config)
    await writer.start()


if __name__ == "__main__":
    asyncio.run(main())
