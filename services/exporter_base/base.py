"""exporter-base: Core async runtime for all Aragog exporters.

Pipeline: Redis (BLMPOP) → validate → Kafka (produce)
One asyncio event loop per K8s pod. HPA scales pods horizontally.

Serialization: msgspec.json for full Redis→Kafka lifecycle.
Event loop: uvloop for 2-4x I/O throughput improvement.
"""

import abc
import asyncio
import os
import signal
import sys
import time
from collections.abc import Callable
from typing import Any

import msgspec
import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer
from loguru import logger
from prometheus_client import Counter, Gauge, Histogram

# ---------------------------------------------------------------------------
# Logging setup (loguru)
# ---------------------------------------------------------------------------


def setup_logging(level: str = "INFO") -> None:
    """Configure loguru JSON logging to stdout for Promtail/Loki ingestion.

    With ``serialize=True``, loguru outputs Loki-compatible JSON
    automatically including timestamp, level, module, line, and function.
    """
    logger.remove()
    logger.add(
        sys.stdout,
        format="{message}",
        serialize=True,
        level=os.environ.get("LOG_LEVEL", level).upper(),
    )


# ---------------------------------------------------------------------------
# msgspec JSON encoder/decoder — module-level singletons, thread-safe
# ---------------------------------------------------------------------------

_encoder = msgspec.json.Encoder()
_decoder = msgspec.json.Decoder()


def json_encode(obj: Any) -> bytes:
    """Encode Python object to JSON bytes via msgspec.

    Replaces orjson.dumps(). ~2x faster for dict-heavy payloads,
    uses less memory (no intermediate Python objects).
    """
    return _encoder.encode(obj)


def json_decode(data: bytes) -> Any:
    """Decode JSON bytes to Python dict/list via msgspec.

    Replaces orjson.loads(). Returns untyped dict — same interface.
    For typed decode with validation, use msgspec.json.Decoder(Type).
    """
    return _decoder.decode(data)


# ---------------------------------------------------------------------------
# Configuration — msgspec.Struct replaces @dataclass
# ---------------------------------------------------------------------------


class ExporterConfig(msgspec.Struct, kw_only=True):
    """Runtime configuration loaded from environment variables (set by Helm).

    msgspec.Struct advantages over @dataclass:
    - 0.09 μs creation vs 1.54 μs (Pydantic) — 17x faster
    - Immutable by default (safer in async code)
    """

    exporter_name: str = ""
    exporter_type: str = "organization"
    source_name: str = ""
    schema: str = ""

    redis_url: str = ""
    redis_queue: str = ""

    batch_size: int = 10_000
    check_interval: int = 30
    blmpop_timeout: int = 5

    kafka_bootstrap: str = ""
    kafka_topic: str = ""
    kafka_compression: str = "lz4"
    kafka_acks: str = "all"
    kafka_linger_ms: int = 50
    kafka_max_request_size: int = 10_485_760  # 10 MB

    dupefilter_key: str = ""
    health_port: int = 8080

    @classmethod
    def from_env(cls) -> "ExporterConfig":
        """Build configuration from environment variables."""
        return cls(
            exporter_name=os.environ.get("EXPORTER_NAME", "unknown"),
            exporter_type=os.environ.get("EXPORTER_TYPE", "organization"),
            source_name=os.environ.get("SOURCE_NAME", ""),
            schema=os.environ.get("SCHEMA", ""),
            redis_url=os.environ.get("REDIS_URL", ""),
            redis_queue=os.environ.get("REDIS_QUEUE", ""),
            batch_size=int(os.environ.get("BATCH_SIZE", "10000")),
            check_interval=int(os.environ.get("CHECK_INTERVAL", "30")),
            kafka_bootstrap=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", ""),
            kafka_topic=os.environ.get("KAFKA_TOPIC", ""),
            kafka_compression=os.environ.get("KAFKA_COMPRESSION", "lz4"),
            kafka_acks=os.environ.get("KAFKA_ACKS", "all"),
            kafka_linger_ms=int(os.environ.get("KAFKA_LINGER_MS", "50")),
            kafka_max_request_size=int(os.environ.get("KAFKA_MAX_REQUEST_SIZE", "10485760")),
            dupefilter_key=os.environ.get("DUPEFILTER_KEY", ""),
            health_port=int(os.environ.get("HEALTH_PORT", "8080")),
        )


# ---------------------------------------------------------------------------
# Prometheus Metrics
# ---------------------------------------------------------------------------


class ExporterMetrics:
    """One set of Prometheus counters per pod process."""

    def __init__(self, name: str, etype: str) -> None:
        self._name = name
        self._type = etype

        self.items_produced = Counter(
            "aragog_exporter_items_produced_total",
            "Items validated and sent to Kafka",
            ["exporter", "type"],
        )
        self.items_skipped = Counter(
            "aragog_exporter_items_skipped_total",
            "Items skipped during validation",
            ["exporter", "type", "reason"],
        )
        self.items_errored = Counter(
            "aragog_exporter_items_errored_total",
            "Items that raised exceptions during validation",
            ["exporter", "type"],
        )
        self.batch_duration = Histogram(
            "aragog_exporter_batch_duration_seconds",
            "Time to process one batch end-to-end",
            ["exporter"],
            buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60],
        )
        self.redis_queue_len = Gauge(
            "aragog_exporter_redis_queue_length",
            "Current Redis queue length",
            ["exporter", "queue"],
        )
        self.kafka_errors = Counter(
            "aragog_exporter_kafka_produce_errors_total",
            "Kafka produce failures",
            ["exporter"],
        )

    def inc_produced(self, n: int = 1) -> None:
        """Increment produced items counter."""
        self.items_produced.labels(exporter=self._name, type=self._type).inc(n)

    def inc_skipped(self, reason: str, n: int = 1) -> None:
        """Increment skipped items counter with reason label."""
        self.items_skipped.labels(exporter=self._name, type=self._type, reason=reason).inc(n)

    def inc_errored(self, n: int = 1) -> None:
        """Increment errored items counter."""
        self.items_errored.labels(exporter=self._name, type=self._type).inc(n)

    def observe_batch(self, duration: float) -> None:
        """Record batch duration observation."""
        self.batch_duration.labels(exporter=self._name).observe(duration)

    def set_queue_len(self, queue: str, length: int) -> None:
        """Set current Redis queue length gauge."""
        self.redis_queue_len.labels(exporter=self._name, queue=queue).set(length)

    def inc_kafka_errors(self, n: int = 1) -> None:
        """Increment Kafka produce errors counter."""
        self.kafka_errors.labels(exporter=self._name).inc(n)


# ---------------------------------------------------------------------------
# Abstract Validator
# ---------------------------------------------------------------------------


class BaseValidator(abc.ABC):
    """Stateless item validator/transformer.

    No DB access — only reads from Redis (via runner), outputs dicts for Kafka.
    Subclasses implement validate_batch() and partition_key().
    """

    def __init__(self, config: ExporterConfig) -> None:
        self.config = config
        self.middlewares: list[Callable[..., Any]] = []

    def add_middleware(self, fn: Callable[..., Any]) -> None:
        """Register a middleware function for item transformation."""
        self.middlewares.append(fn)

    def apply_middlewares(self, item: dict[str, Any]) -> dict[str, Any]:
        """Apply all registered middlewares to an item sequentially."""
        for mw in self.middlewares:
            item = mw(item)
        return item

    @abc.abstractmethod
    async def validate_batch(
        self,
        raw_items: list[bytes],
        redis: aioredis.Redis,
        metrics: ExporterMetrics,
    ) -> list[dict[str, Any]]:
        """Validate a batch of raw bytes from Redis BLMPOP.

        Returns list of dicts ready for Kafka serialization via json_encode().
        Failed items are logged/counted but never re-raised.
        """
        ...

    @abc.abstractmethod
    def partition_key(self, item: dict[str, Any]) -> bytes | None:
        """Return Kafka partition key for ordering. None = round-robin."""
        ...

    async def on_startup(self, redis: aioredis.Redis) -> None:
        """Called once before main loop (e.g. dupefilter prefill)."""

    async def on_shutdown(self) -> None:
        """Called on graceful shutdown."""


# ---------------------------------------------------------------------------
# Health / Metrics HTTP Server
# ---------------------------------------------------------------------------


async def _run_health_server(config: ExporterConfig, readiness_flag: asyncio.Event) -> None:
    """Minimal uvicorn+FastAPI for K8s probes and Prometheus /metrics."""
    import uvicorn
    from fastapi import FastAPI
    from fastapi.responses import PlainTextResponse
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

    app = FastAPI(docs_url=None, redoc_url=None)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "service": config.exporter_name}

    @app.get("/ready")
    async def ready() -> dict[str, str] | PlainTextResponse:
        if not readiness_flag.is_set():
            return PlainTextResponse("not ready", status_code=503)
        return {"status": "ready"}

    @app.get("/metrics")
    async def metrics_endpoint() -> PlainTextResponse:
        body = generate_latest().decode("utf-8")
        return PlainTextResponse(body, media_type=CONTENT_TYPE_LATEST)

    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host="0.0.0.0",
            port=config.health_port,
            log_level="warning",
            access_log=False,
        )
    )
    await server.serve()


# ---------------------------------------------------------------------------
# Exporter Runner — the main loop
# ---------------------------------------------------------------------------


class ExporterRunner:
    """Async main loop: Redis BLMPOP → validate → Kafka produce.

    Lifecycle:
      1. start()  — connect Redis + Kafka, run health server, enter loop
      2. _process_batch() — one cycle: pop → validate → produce
      3. stop()   — flush Kafka, close connections

    Kafka value_serializer uses json_encode() (msgspec) for all payloads.
    """

    def __init__(self, validator: BaseValidator, config: ExporterConfig) -> None:
        self.validator = validator
        self.config = config
        self.metrics = ExporterMetrics(config.exporter_name, config.exporter_type)

        self._running = False
        self._ready = asyncio.Event()
        self._redis: aioredis.Redis | None = None
        self._producer: AIOKafkaProducer | None = None
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        """Connect to Redis/Kafka, start health server, and enter the main loop."""
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, self._request_shutdown)
        loop.add_signal_handler(signal.SIGINT, self._request_shutdown)

        # --- Redis ---
        logger.info(f"Connecting to Redis: {_mask_url(self.config.redis_url)}")
        self._redis = aioredis.from_url(
            self.config.redis_url,
            decode_responses=False,
            max_connections=4,
        )
        await self._redis.ping()
        logger.info("Redis connected")

        # --- Kafka Producer (msgspec serializer) ---
        logger.info(f"Starting Kafka producer: {self.config.kafka_bootstrap}")
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.config.kafka_bootstrap,
            compression_type=self.config.kafka_compression,
            acks=self.config.kafka_acks,
            linger_ms=self.config.kafka_linger_ms,
            max_request_size=self.config.kafka_max_request_size,
            value_serializer=json_encode,
            key_serializer=lambda k: k if isinstance(k, bytes) else k.encode("utf-8") if k else None,
        )
        await self._producer.start()
        logger.info(f"Kafka producer started, topic={self.config.kafka_topic}")

        # --- Startup hook ---
        logger.info("Running validator on_startup hook")
        await self.validator.on_startup(self._redis)

        # --- Health server in background ---
        self._tasks.append(
            asyncio.create_task(
                _run_health_server(self.config, self._ready),
                name="health-server",
            )
        )

        # --- Ready ---
        self._running = True
        self._ready.set()
        logger.info(
            f"Exporter '{self.config.exporter_name}' ready — "
            f"queue={self.config.redis_queue} topic={self.config.kafka_topic} "
            f"batch={self.config.batch_size}"
        )

        try:
            await self._main_loop()
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Flush Kafka, close Redis, and cancel background tasks."""
        logger.info(f"Stopping exporter '{self.config.exporter_name}'")
        self._running = False
        self._ready.clear()

        await self.validator.on_shutdown()

        if self._producer:
            try:
                await self._producer.flush()
                await self._producer.stop()
                logger.info("Kafka producer stopped")
            except Exception:
                logger.exception("Error stopping Kafka producer")

        if self._redis:
            try:
                await self._redis.close()
                logger.info("Redis closed")
            except Exception:
                logger.exception("Error closing Redis")

        for t in self._tasks:
            t.cancel()

        logger.info(f"Exporter '{self.config.exporter_name}' stopped")

    # ---- internals ------------------------------------------------------

    async def _main_loop(self) -> None:
        """Core event loop: process batches until shutdown."""
        while self._running:
            try:
                produced = await self._process_batch()
                if produced == 0:
                    await self._report_queue_length()
                    await self._interruptible_sleep(self.config.check_interval)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Unhandled error in main loop, sleeping 5s")
                await self._interruptible_sleep(5)

    async def _process_batch(self) -> int:
        """Pop → validate → produce. Returns number of Kafka messages sent."""
        assert self._redis is not None, "Redis not connected"
        assert self._producer is not None, "Kafka producer not started"

        t0 = time.monotonic()

        # 1. Redis BLMPOP
        result = await self._redis.execute_command(
            "BLMPOP",
            str(self.config.blmpop_timeout),
            "1",
            self.config.redis_queue,
            "LEFT",
            "COUNT",
            str(self.config.batch_size),
        )

        if result is None:
            return 0

        _queue_name, raw_items = result
        if not raw_items:
            return 0

        batch_len = len(raw_items)
        logger.info(f"Popped {batch_len} items from '{self.config.redis_queue}'")

        # 2. Validate
        validated = await self.validator.validate_batch(
            raw_items,
            self._redis,
            self.metrics,
        )

        if not validated:
            logger.info(f"All {batch_len} items filtered out during validation")
            return 0

        # 3. Produce to Kafka — sequential send, NOT ensure_future!
        # aiokafka accumulator bug: concurrent send() drops throughput 25x
        produced = 0
        for item in validated:
            try:
                key = self.validator.partition_key(item)
                await self._producer.send(
                    self.config.kafka_topic,
                    value=item,
                    key=key,
                )
                produced += 1
            except Exception:
                logger.exception("Kafka send failed")
                self.metrics.inc_kafka_errors()

        await self._producer.flush()

        # 4. Metrics
        self.metrics.inc_produced(produced)
        duration = time.monotonic() - t0
        self.metrics.observe_batch(duration)
        logger.info(
            f"Batch complete: popped={batch_len} validated={len(validated)} produced={produced} {duration:.2f}s"
        )
        return produced

    async def _report_queue_length(self) -> None:
        """Report current Redis queue length to Prometheus."""
        if self._redis is None:
            return
        try:
            length = await self._redis.llen(self.config.redis_queue)
            self.metrics.set_queue_len(self.config.redis_queue, length)
        except Exception:
            pass

    async def _interruptible_sleep(self, seconds: float) -> None:
        """Sleep that exits early on shutdown signal."""
        try:
            for _ in range(int(seconds)):
                if not self._running:
                    return
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            return

    def _request_shutdown(self) -> None:
        """Handle SIGTERM/SIGINT gracefully."""
        logger.info("Shutdown signal received")
        self._running = False
        self._ready.clear()


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------


def _mask_url(url: str) -> str:
    """Mask password in redis:// or postgres:// URLs for safe logging."""
    if "@" in url:
        scheme_end = url.index("://") + 3 if "://" in url else 0
        at_pos = url.index("@")
        return url[:scheme_end] + "***:***@" + url[at_pos + 1 :]
    return url
