"""ch-writer: Kafka consumer -> ClickHouse batch writer.

Consumes from: reviews.validated.v1
Writes to: ClickHouse {schema}.raw_reviews tables

Ported from: ReviewsExporter.process_batch (insert section)

Key differences from pg-writer:
  - Much larger batch sizes (50k+ rows) to avoid ClickHouse 'Too many parts'
  - Groups by target database (source_33, source_47, etc.)
  - Uses max_partitions_per_insert_block=10000 setting (from original)
  - Dedup already done by review-exporter (Redis SADD), no ON CONFLICT needed
"""

import asyncio
import os
import signal
import sys
import time
from collections import defaultdict
from typing import Any, Optional

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

# Column order for raw_reviews table (must match ClickHouse schema)
COLUMN_NAMES = [
    "source_product_id",
    "review_language",
    "review_text",
    "original_review_text",
    "source_review_id",
    "url",
    "owner_response",
    "original_owner_response",
    "source",
    "user_id",
    "username",
    "review_rating",
    "review_location",
    "review_date",
    "review_parse_date",
    "additional_info",
    "batch_id",
    "batch_timestamp",
]


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
    """ch-writer runtime configuration from environment variables."""

    def __init__(self) -> None:
        self.kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
        self.kafka_topic = os.environ.get("KAFKA_TOPIC", "reviews.validated.v1")
        self.consumer_group = os.environ.get("KAFKA_CONSUMER_GROUP", "aragog-ch-writers")
        self.ch_host = os.environ.get("CLICKHOUSE_HOST", "")
        self.ch_port = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
        self.ch_user = os.environ.get("CLICKHOUSE_USER", "")
        self.ch_password = os.environ.get("CLICKHOUSE_PASSWORD", "")
        self.batch_size = int(os.environ.get("BATCH_SIZE", "50000"))
        self.flush_interval = float(os.environ.get("FLUSH_INTERVAL_MS", "10000")) / 1000
        self.health_port = int(os.environ.get("HEALTH_PORT", "8080"))
        self.max_partitions = int(os.environ.get("CH_MAX_PARTITIONS", "10000"))
        self.dlq_topic = os.environ.get("DLQ_TOPIC", "reviews.dlq.v1")


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

records_written = Counter(
    "aragog_writer_records_written_total",
    "Records written to ClickHouse",
    ["writer", "database"],
)
write_errors = Counter(
    "aragog_writer_errors_total",
    "Database write failures",
    ["writer", "database"],
)
batch_duration = Histogram(
    "aragog_writer_batch_write_seconds",
    "Batch write duration",
    ["writer"],
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60, 120],
)
buffer_size_gauge = Gauge(
    "aragog_writer_buffer_size",
    "Records buffered in memory",
    ["writer"],
)


# ---------------------------------------------------------------------------
# CH Writer
# ---------------------------------------------------------------------------

class ChWriter:
    """Kafka consumer that accumulates large batches of reviews and bulk-inserts into ClickHouse.

    Flow:
      1. Consume from Kafka
      2. Buffer records grouped by target database
      3. Flush when any database buffer >= batch_size OR flush_interval elapsed
      4. INSERT into {database}.raw_reviews with column ordering
      5. Commit Kafka offsets after successful insert
    """

    def __init__(self, config: WriterConfig) -> None:
        self.config = config
        self._running = False
        self._ready = asyncio.Event()

        self._buffers: dict[str, list[list]] = defaultdict(list)
        self._total_buffered = 0
        self._last_flush = time.monotonic()

        self._consumer: AIOKafkaConsumer | None = None
        self._dlq_producer: AIOKafkaProducer | None = None
        self._ch_clients: dict[str, Any] = {}
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        """Connect to Kafka and enter the consume loop."""
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, self._request_shutdown)
        loop.add_signal_handler(signal.SIGINT, self._request_shutdown)

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

        self._dlq_producer = AIOKafkaProducer(
            bootstrap_servers=self.config.kafka_bootstrap,
            value_serializer=lambda v: orjson.dumps(v),
        )
        await self._dlq_producer.start()
        logger.info(f"DLQ producer started, topic={self.config.dlq_topic}")

        self._tasks.append(asyncio.create_task(self._run_health_server(), name="health-server"))
        self._tasks.append(asyncio.create_task(self._flush_timer(), name="flush-timer"))

        self._running = True
        self._ready.set()
        logger.info("CH Writer ready")

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
                        self._buffer_message(msg.value)

                buffer_size_gauge.labels(writer="ch").set(self._total_buffered)

                for db, rows in self._buffers.items():
                    if len(rows) >= self.config.batch_size:
                        await self._flush_database(db)

            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error in consume loop")
                await asyncio.sleep(2)

    def _buffer_message(self, msg: dict[str, Any]) -> None:
        """Deserialize Kafka message and buffer as a row array."""
        database = msg.get("schema", "")
        review = msg.get("review")

        if not database or not review:
            return

        row = [review.get(col) for col in COLUMN_NAMES]
        self._buffers[database].append(row)
        self._total_buffered += 1

    async def _flush_timer(self) -> None:
        """Periodic flush to avoid holding data too long."""
        while self._running:
            await asyncio.sleep(1)
            elapsed = time.monotonic() - self._last_flush
            if elapsed >= self.config.flush_interval and self._total_buffered > 0:
                logger.debug(f"Flush timer: {self._total_buffered} total buffered")
                await self._flush_all()

    async def _flush_database(self, database: str) -> None:
        """Insert buffered rows into {database}.raw_reviews via clickhouse-connect."""
        rows = self._buffers.get(database)
        if not rows:
            return

        to_insert = rows[:]
        self._buffers[database] = []
        self._total_buffered -= len(to_insert)
        buffer_size_gauge.labels(writer="ch").set(max(self._total_buffered, 0))

        t0 = time.monotonic()
        logger.info(f"Flushing {len(to_insert)} rows to ClickHouse database '{database}'")

        try:
            client = self._get_ch_client(database)
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                lambda: client.insert(
                    table="raw_reviews",
                    data=to_insert,
                    column_names=COLUMN_NAMES,
                    settings={"max_partitions_per_insert_block": self.config.max_partitions},
                ),
            )

            records_written.labels(writer="ch", database=database).inc(len(to_insert))
            duration = time.monotonic() - t0
            batch_duration.labels(writer="ch").observe(duration)
            logger.info(f"ClickHouse insert: database={database} rows={len(to_insert)} in {duration:.2f}s")

        except Exception:
            logger.exception(f"ClickHouse insert failed: database={database} rows={len(to_insert)}")
            write_errors.labels(writer="ch", database=database).inc(len(to_insert))
            await self._send_to_dlq(database, to_insert)

    async def _flush_all(self) -> None:
        """Flush all database buffers and commit Kafka offsets."""
        for db in list(self._buffers.keys()):
            if self._buffers[db]:
                await self._flush_database(db)

        if self._consumer:
            try:
                await self._consumer.commit()
            except Exception:
                logger.exception("Kafka commit failed")

        self._last_flush = time.monotonic()

    def _get_ch_client(self, database: str) -> Any:
        """Get or lazily create a clickhouse-connect client for a specific database."""
        if database not in self._ch_clients:
            import clickhouse_connect

            self._ch_clients[database] = clickhouse_connect.get_client(
                host=self.config.ch_host,
                port=self.config.ch_port,
                username=self.config.ch_user,
                password=self.config.ch_password,
                database=database,
            )
            logger.info(f"Created ClickHouse client for database '{database}'")

        return self._ch_clients[database]

    async def _send_to_dlq(self, database: str, rows: list[list]) -> None:
        """Send failed rows to dead-letter topic for reprocessing."""
        if not self._dlq_producer:
            return
        try:
            for row in rows:
                record = dict(zip(COLUMN_NAMES, row))
                await self._dlq_producer.send(
                    self.config.dlq_topic,
                    value={"database": database, "record": record},
                )
            await self._dlq_producer.flush()
            logger.info(f"Sent {len(rows)} failed rows to DLQ for database={database}")
        except Exception:
            logger.exception("Failed to send to DLQ")

    async def stop(self) -> None:
        """Flush remaining buffer and shut down connections."""
        logger.info("Stopping CH Writer")
        self._running = False
        self._ready.clear()

        if self._total_buffered > 0:
            logger.info(f"Flushing {self._total_buffered} remaining buffered records")
            await self._flush_all()

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

        for db, client in self._ch_clients.items():
            try:
                client.close()
            except Exception:
                pass
        self._ch_clients.clear()

        for t in self._tasks:
            t.cancel()

        logger.info("CH Writer stopped")

    def _request_shutdown(self) -> None:
        """Handle SIGTERM/SIGINT."""
        logger.info("Shutdown signal received")
        self._running = False
        self._ready.clear()

    async def _run_health_server(self) -> None:
        """Run K8s probe and Prometheus metrics HTTP server."""
        from fastapi import FastAPI
        from fastapi.responses import PlainTextResponse
        import uvicorn

        app = FastAPI(docs_url=None, redoc_url=None)

        @app.get("/health")
        async def health() -> dict[str, str]:
            return {"status": "ok", "service": "ch-writer"}

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
    """Start the ch-writer service."""
    setup_logging(level=os.environ.get("LOG_LEVEL", "INFO"))
    config = WriterConfig()
    writer = ChWriter(config)
    await writer.start()


if __name__ == "__main__":
    asyncio.run(main())
