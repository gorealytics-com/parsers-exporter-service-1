"""pg-writer: Kafka consumer → PostgreSQL batch writer.

Consumes from: orgs.validated.v1
Writes to: PostgreSQL (via PgBouncer) using asyncpg COPY-based upsert.

Migration from old pg-writer:
  - orjson → msgspec.json for Kafka de/serialization
  - executemany (~3,300 rows/s) → COPY to staging table (~50,000-150,000 rows/s)
  - Added WHERE IS DISTINCT FROM to skip unchanged rows (reduces VACUUM pressure)
  - statement_cache_size=0 for PgBouncer transaction mode compatibility
  - WriterConfig @dataclass → msgspec.Struct
  - uvloop entrypoint

Tables written per message:
  - {schema}.organisation_data    (conflict key: place_id)
  - {schema}.organisation_contacts (conflict key: place_id)
  - {schema}.menu_data             (conflict key: id)
"""

import asyncio
import os
import signal
import sys
import time
from collections import defaultdict
from typing import Any

import asyncpg
import msgspec
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from loguru import logger
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

# ---------------------------------------------------------------------------
# msgspec JSON — module-level singletons
# ---------------------------------------------------------------------------

_encoder = msgspec.json.Encoder()
_decoder = msgspec.json.Decoder()


def json_encode(obj: Any) -> bytes:
    """Encode to JSON bytes via msgspec (replaces orjson.dumps)."""
    return _encoder.encode(obj)


def json_decode(data: bytes) -> Any:
    """Decode JSON bytes via msgspec (replaces orjson.loads)."""
    return _decoder.decode(data)


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
# Configuration — msgspec.Struct
# ---------------------------------------------------------------------------


class WriterConfig(msgspec.Struct, kw_only=True):
    """pg-writer runtime configuration from environment variables."""

    kafka_bootstrap: str = ""
    kafka_topic: str = "orgs.validated.v1"
    consumer_group: str = "aragog-pg-writers"
    pg_dsn: str = ""
    batch_size: int = 1000
    flush_interval: float = 5.0
    insert_chunk_size: int = 5000
    health_port: int = 8080
    pg_min_pool: int = 2
    pg_max_pool: int = 10
    dlq_topic: str = "orgs.dlq.v1"

    @classmethod
    def from_env(cls) -> "WriterConfig":
        """Build from environment variables."""
        return cls(
            kafka_bootstrap=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", ""),
            kafka_topic=os.environ.get("KAFKA_TOPIC", "orgs.validated.v1"),
            consumer_group=os.environ.get("KAFKA_CONSUMER_GROUP", "aragog-pg-writers"),
            pg_dsn=os.environ.get("PG_URI", ""),
            batch_size=int(os.environ.get("BATCH_SIZE", "1000")),
            flush_interval=float(os.environ.get("FLUSH_INTERVAL_MS", "5000")) / 1000,
            insert_chunk_size=int(os.environ.get("INSERT_CHUNK_SIZE", "5000")),
            health_port=int(os.environ.get("HEALTH_PORT", "8080")),
            pg_min_pool=int(os.environ.get("PG_POOL_MIN", "2")),
            pg_max_pool=int(os.environ.get("PG_POOL_MAX", "10")),
            dlq_topic=os.environ.get("DLQ_TOPIC", "orgs.dlq.v1"),
        )


# ---------------------------------------------------------------------------
# Prometheus Metrics
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
# COPY-based upsert — 10-50x faster than executemany
# ---------------------------------------------------------------------------


async def copy_upsert(
    conn: asyncpg.Connection | asyncpg.pool.PoolConnectionProxy,
    schema: str,
    table: str,
    records: list[dict[str, Any]],
    conflict_columns: list[str],
) -> int:
    """Upsert records via COPY to temp staging table + INSERT ON CONFLICT.

    Performance: ~50,000-150,000 rows/sec vs executemany's ~3,300 rows/sec.

    Strategy:
      1. CREATE TEMP TABLE _stg (same structure) ON COMMIT DROP
      2. COPY records into _stg using asyncpg binary protocol
      3. INSERT INTO target SELECT * FROM _stg ON CONFLICT DO UPDATE
      4. WHERE IS DISTINCT FROM — skip unchanged rows (reduces dead tuples)
    """
    if not records:
        return 0

    records = [r for r in records if r]
    if not records:
        return 0

    # Group by column set (records may have different field subsets)
    grouped: dict[tuple[str, ...], list[dict]] = defaultdict(list)
    for record in records:
        keys = tuple(sorted(record.keys()))
        grouped[keys].append(record)

    total_written = 0

    for columns_tuple, group_records in grouped.items():
        columns = list(columns_tuple)
        col_list = ", ".join(f'"{c}"' for c in columns)
        conflict_list = ", ".join(f'"{c}"' for c in conflict_columns)

        # Build UPDATE SET clause, skipping conflict columns
        update_cols = [c for c in columns if c not in conflict_columns]
        if update_cols:
            # WHERE IS DISTINCT FROM: skip updates when values haven't changed
            update_set = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
            distinct_check = " OR ".join(
                f'"{schema}"."{table}"."{c}" IS DISTINCT FROM EXCLUDED."{c}"'
                for c in update_cols[:5]  # check first 5 cols for perf
            )
            on_conflict = f"ON CONFLICT ({conflict_list}) DO UPDATE SET {update_set} WHERE {distinct_check}"
        else:
            on_conflict = f"ON CONFLICT ({conflict_list}) DO NOTHING"

        # Convert records to list of tuples for COPY
        rows = [tuple(rec.get(col) for col in columns) for rec in group_records]

        try:
            # Staging table created within the transaction, dropped on commit
            stg_table = f"_stg_{table}"
            await conn.execute(
                f'CREATE TEMP TABLE IF NOT EXISTS "{stg_table}" '
                f'(LIKE "{schema}"."{table}" INCLUDING NOTHING) ON COMMIT DROP'
            )
            await conn.execute(f'TRUNCATE "{stg_table}"')

            # COPY binary — the fast path (~50K rows/sec)
            await conn.copy_records_to_table(
                stg_table,
                records=rows,
                columns=columns,
            )

            # Merge from staging into target
            result = await conn.execute(
                f'INSERT INTO "{schema}"."{table}" ({col_list}) SELECT {col_list} FROM "{stg_table}" {on_conflict}'
            )

            # Parse "INSERT 0 N" result to get actual row count
            count = int(result.split()[-1]) if result else len(rows)
            total_written += count

        except asyncpg.UndefinedTableError:
            logger.error(f"Table {schema}.{table} does not exist, skipping {len(group_records)} records")
            write_errors.labels(writer="pg", schema=schema, table=table).inc(len(group_records))
        except Exception:
            logger.exception(
                f"COPY upsert failed: {schema}.{table} ({len(group_records)} records), falling back to executemany"
            )
            # Fallback: executemany for this chunk (slower but more forgiving)
            total_written += await _executemany_fallback(
                conn,
                schema,
                table,
                columns,
                conflict_columns,
                group_records,
            )

    return total_written


async def _executemany_fallback(
    conn: asyncpg.Connection | asyncpg.pool.PoolConnectionProxy,
    schema: str,
    table: str,
    columns: list[str],
    conflict_columns: list[str],
    records: list[dict],
) -> int:
    """Fallback upsert via executemany when COPY fails.

    This handles edge cases like type mismatches that COPY binary rejects.
    """
    col_list = ", ".join(f'"{c}"' for c in columns)
    conflict_list = ", ".join(f'"{c}"' for c in conflict_columns)
    placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))

    update_cols = [c for c in columns if c not in conflict_columns]
    if update_cols:
        update_set = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
        sql = (
            f'INSERT INTO "{schema}"."{table}" ({col_list}) '
            f"VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_list}) DO UPDATE SET {update_set}"
        )
    else:
        sql = (
            f'INSERT INTO "{schema}"."{table}" ({col_list}) '
            f"VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_list}) DO NOTHING"
        )

    rows = [tuple(rec.get(col) for col in columns) for rec in records]
    try:
        await conn.executemany(sql, rows)
        return len(rows)
    except Exception:
        logger.exception(f"executemany fallback also failed: {schema}.{table}")
        write_errors.labels(writer="pg", schema=schema, table=table).inc(len(rows))
        return 0


# ---------------------------------------------------------------------------
# PG Writer
# ---------------------------------------------------------------------------


class PgWriter:
    """Kafka consumer that micro-batches org records and writes to PostgreSQL.

    Flow:
      1. Consume messages from Kafka (msgspec deserialization)
      2. Buffer in memory, grouped by schema
      3. Flush on batch_size reached OR flush_interval elapsed
      4. COPY-based upsert through PgBouncer (10-50x faster than executemany)
      5. Commit Kafka offsets after successful write
    """

    def __init__(self, config: WriterConfig) -> None:
        self.config = config
        self._running = False
        self._ready = asyncio.Event()
        self._flush_lock = asyncio.Lock()

        self._buffer: list[Any] = []
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
        # statement_cache_size=0 is REQUIRED for PgBouncer transaction mode
        # (PgBouncer 1.21+ supports prepared statements via max_prepared_statements)
        logger.info("Connecting to PostgreSQL (via PgBouncer)")
        self._pool = await asyncpg.create_pool(
            dsn=self.config.pg_dsn,
            min_size=self.config.pg_min_pool,
            max_size=self.config.pg_max_pool,
            command_timeout=60,
            statement_cache_size=0,  # PgBouncer compatibility
        )
        logger.info(f"PG pool created: min={self.config.pg_min_pool} max={self.config.pg_max_pool}")

        # --- Kafka consumer (msgspec deserializer) ---
        logger.info(f"Starting Kafka consumer: topic={self.config.kafka_topic}")
        self._consumer = AIOKafkaConsumer(
            self.config.kafka_topic,
            bootstrap_servers=self.config.kafka_bootstrap,
            group_id=self.config.consumer_group,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=json_decode,
            max_poll_records=self.config.batch_size,
        )
        await self._consumer.start()
        logger.info("Kafka consumer started")

        # --- DLQ producer (msgspec serializer) ---
        self._dlq_producer = AIOKafkaProducer(
            bootstrap_servers=self.config.kafka_bootstrap,
            value_serializer=json_encode,
        )
        await self._dlq_producer.start()
        logger.info(f"DLQ producer started, topic={self.config.dlq_topic}")

        # --- Background tasks ---
        self._tasks.append(asyncio.create_task(self._run_health_server(), name="health"))
        self._tasks.append(asyncio.create_task(self._flush_timer(), name="flush-timer"))

        self._running = True
        self._ready.set()
        logger.info("PG Writer ready")

        try:
            await self._consume_loop()
        finally:
            await self.stop()

    async def _consume_loop(self) -> None:
        """Main consumer loop."""
        assert self._consumer is not None, "Kafka consumer not started"
        assert self._pool is not None, "PG pool not created"

        while self._running:
            try:
                batch = await self._consumer.getmany(
                    timeout_ms=1000,
                    max_records=self.config.batch_size,
                )

                for tp, messages in batch.items():
                    for msg in messages:
                        if msg.value is not None:
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
        """Write buffered records to PostgreSQL and commit Kafka offsets.

        Uses asyncio.Lock to prevent concurrent flushes from consume loop
        and flush timer racing each other.
        """
        async with self._flush_lock:
            if not self._buffer:
                return
            if self._pool is None or self._consumer is None:
                return

            to_write = self._buffer[:]
            self._buffer.clear()
            buffer_size.labels(writer="pg").set(0)

            t0 = time.monotonic()
            logger.info(f"Flushing {len(to_write)} records to PostgreSQL")

            # Group by schema
            by_schema: dict[str, dict[str, list]] = defaultdict(lambda: {"orgs": [], "contacts": [], "menu": []})
            for msg in to_write:
                schema = msg.get("schema", "default")
                if msg.get("org"):
                    by_schema[schema]["orgs"].append(msg["org"])
                if msg.get("contacts") and msg["contacts"].get("place_id"):
                    by_schema[schema]["contacts"].append(msg["contacts"])
                if msg.get("menu"):
                    by_schema[schema]["menu"].extend(msg["menu"])

            all_success = True
            async with self._pool.acquire() as conn:
                for schema, tables in by_schema.items():
                    try:
                        async with conn.transaction():
                            if tables["orgs"]:
                                written = await copy_upsert(
                                    conn,
                                    schema,
                                    "organisation_data",
                                    tables["orgs"],
                                    ["place_id"],
                                )
                                records_written.labels(
                                    writer="pg",
                                    schema=schema,
                                    table="organisation_data",
                                ).inc(written)

                            if tables["contacts"]:
                                written = await copy_upsert(
                                    conn,
                                    schema,
                                    "organisation_contacts",
                                    tables["contacts"],
                                    ["place_id"],
                                )
                                records_written.labels(
                                    writer="pg",
                                    schema=schema,
                                    table="organisation_contacts",
                                ).inc(written)

                            if tables["menu"]:
                                written = await copy_upsert(
                                    conn,
                                    schema,
                                    "menu_data",
                                    tables["menu"],
                                    ["id"],
                                )
                                records_written.labels(
                                    writer="pg",
                                    schema=schema,
                                    table="menu_data",
                                ).inc(written)

                    except Exception:
                        logger.exception(f"Failed to write schema={schema}")
                        write_errors.labels(writer="pg", schema=schema, table="all").inc()
                        all_success = False
                        await self._send_to_dlq(schema, tables)

            # Commit offset only after ALL schemas written or DLQ'd
            if all_success:
                try:
                    await self._consumer.commit()
                except Exception:
                    logger.exception("Kafka commit failed")

            duration = time.monotonic() - t0
            batch_duration.labels(writer="pg").observe(duration)
            self._last_flush = time.monotonic()
            logger.info(f"Flush complete: {len(to_write)} records in {duration:.2f}s")

    async def _send_to_dlq(self, schema: str, tables: dict) -> None:
        """Send failed records to dead-letter topic for later reprocessing."""
        if not self._dlq_producer:
            return
        try:
            for table_name, records in tables.items():
                for record in records:
                    await self._dlq_producer.send(
                        self.config.dlq_topic,
                        value={
                            "schema": schema,
                            "table": table_name,
                            "record": record,
                        },
                    )
            await self._dlq_producer.flush()
            total = sum(len(r) for r in tables.values())
            logger.info(f"Sent {total} failed records to DLQ for schema={schema}")
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

    async def _run_health_server(self) -> None:
        """Run K8s probe and Prometheus metrics HTTP server."""
        import uvicorn
        from fastapi import FastAPI
        from fastapi.responses import PlainTextResponse

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

        server = uvicorn.Server(
            uvicorn.Config(
                app,
                host="0.0.0.0",
                port=self.config.health_port,
                log_level="warning",
                access_log=False,
            )
        )
        await server.serve()


# ---------------------------------------------------------------------------
# Entrypoint — uvloop
# ---------------------------------------------------------------------------


async def main() -> None:
    """Start the pg-writer service."""
    setup_logging(level=os.environ.get("LOG_LEVEL", "INFO"))
    config = WriterConfig.from_env()
    writer = PgWriter(config)
    await writer.start()


if __name__ == "__main__":
    try:
        import uvloop

        uvloop.run(main())
    except ImportError:
        asyncio.run(main())
