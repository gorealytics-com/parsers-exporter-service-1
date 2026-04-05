"""libs/observability: Shared metrics and logging setup.

Used by all services (exporters + writers).
Provides consistent Prometheus metrics and loguru JSON structured logging.
"""

import os
import sys

from loguru import logger

# --- Structured JSON Logger (loguru) ---


def setup_logging(level: str = "INFO", component: str = "unknown") -> None:
    """Configure loguru JSON logging to stdout for Promtail/Loki ingestion.

    Call once at service startup. With ``serialize=True``, loguru outputs
    Loki-compatible JSON automatically including timestamp, level, module,
    line, and function name.

    Args:
        level: Log level string (DEBUG, INFO, WARNING, ERROR).
        component: Service component name for identification.
    """
    logger.remove()
    logger.add(
        sys.stdout,
        format="{message}",
        serialize=True,
        level=os.environ.get("LOG_LEVEL", level).upper(),
    )
    logger.info(f"Logging initialized for component={component}")


# --- Prometheus Metrics ---


def create_exporter_metrics(prefix: str = "aragog_exporter") -> dict:
    """Create standard Prometheus metrics for exporters.

    Returns dict of metric objects. Call .labels(...).inc() etc.

    Metrics: items_processed_total (Counter), items_skipped_total (Counter),
    batch_duration_seconds (Histogram), redis_queue_length (Gauge),
    kafka_produce_errors_total (Counter).
    """
    from prometheus_client import Counter, Gauge, Histogram

    return {
        "items_processed": Counter(
            f"{prefix}_items_processed_total",
            "Total items successfully validated and produced to Kafka",
            ["exporter_name", "type"],
        ),
        "items_skipped": Counter(
            f"{prefix}_items_skipped_total",
            "Items skipped (validation failure, dedup, etc.)",
            ["exporter_name", "type", "reason"],
        ),
        "batch_duration": Histogram(
            f"{prefix}_batch_duration_seconds",
            "Time to process one batch",
            ["exporter_name"],
            buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60],
        ),
        "redis_queue_length": Gauge(
            f"{prefix}_redis_queue_length",
            "Current Redis queue length",
            ["exporter_name", "queue"],
        ),
        "kafka_produce_errors": Counter(
            f"{prefix}_kafka_produce_errors_total",
            "Kafka produce failures",
            ["exporter_name"],
        ),
    }


def create_writer_metrics(prefix: str = "aragog_writer") -> dict:
    """Create standard Prometheus metrics for writers.

    Metrics: records_written_total (Counter), write_errors_total (Counter),
    batch_write_duration_seconds (Histogram), buffer_size (Gauge),
    kafka_consumer_lag (Gauge).
    """
    from prometheus_client import Counter, Gauge, Histogram

    return {
        "records_written": Counter(
            f"{prefix}_records_written_total",
            "Total records written to database",
            ["writer_name", "database"],
        ),
        "write_errors": Counter(
            f"{prefix}_write_errors_total",
            "Database write failures",
            ["writer_name", "database"],
        ),
        "batch_write_duration": Histogram(
            f"{prefix}_batch_write_duration_seconds",
            "Time to write one batch to database",
            ["writer_name"],
            buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60, 120],
        ),
        "buffer_size": Gauge(
            f"{prefix}_buffer_size",
            "Current in-memory buffer size (records pending flush)",
            ["writer_name"],
        ),
        "kafka_consumer_lag": Gauge(
            f"{prefix}_kafka_consumer_lag",
            "Kafka consumer lag (messages behind)",
            ["writer_name", "topic", "partition"],
        ),
    }
