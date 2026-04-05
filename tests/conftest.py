"""Root conftest: shared fixtures for unit and integration tests."""

import os
import sys

import pytest

# Add project root to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "services", "exporter-base"),
)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "org-exporter"))
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "services", "review-exporter"),
)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "pg-writer"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "ch-writer"))


def pytest_configure(config: pytest.Config) -> None:
    """Set default env vars for tests."""
    os.environ.setdefault("LOG_LEVEL", "WARNING")
    os.environ.setdefault("EXPORTER_NAME", "test_exporter")
    os.environ.setdefault("EXPORTER_TYPE", "map")
    os.environ.setdefault("SOURCE_NAME", "Test Source")
    os.environ.setdefault("SCHEMA", "source_99")
    os.environ.setdefault("REDIS_URL", "redis://localhost:16379/0")
    os.environ.setdefault("REDIS_QUEUE", "test:items")
    os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    os.environ.setdefault("KAFKA_TOPIC", "orgs.validated.v1")
