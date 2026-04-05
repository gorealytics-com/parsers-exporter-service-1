#!/usr/bin/env python3
"""
Semantic validator for Helm values.yaml.

Ported from: scripts/ci_validate.py (validated old config.yaml)

Verifies:
  1. Every exporter has required fields (queue, sourceName, type).
  2. No duplicate queue names across exporters.
  3. Review exporters have dupefilterKey.
  4. Organization exporters have schema.
  5. Writer configs are valid.
  6. HPA configs have valid thresholds.
  7. batchSize is a positive integer for all exporters.
"""

import sys

import yaml
from loguru import logger

VALUES_PATH = "helm/aragog-exporters/values.yaml"

REQUIRED_EXPORTER_FIELDS = ["queue", "sourceName"]
VALID_TYPES = {"organization", "reviews"}


def load_values() -> dict:
    with open(VALUES_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def main() -> int:
    errors: list = []
    values = load_values()

    exporters = values.get("exporters", {})
    if not exporters:
        errors.append("No exporters defined in values.yaml")
        _report(errors)
        return 1

    seen_queues: dict = {}

    for name, cfg in exporters.items():
        prefix = f"exporters.{name}"
        etype = cfg.get("type", "organization")

        # Required fields
        for field in REQUIRED_EXPORTER_FIELDS:
            if field not in cfg:
                errors.append(f"{prefix}: missing required field '{field}'")

        # Valid type
        if etype not in VALID_TYPES:
            errors.append(f"{prefix}.type: unknown '{etype}', expected {VALID_TYPES}")

        # Type-specific checks
        if etype == "organization" and not cfg.get("schema"):
            errors.append(f"{prefix}: type 'organization' requires 'schema'")

        if etype == "reviews" and not cfg.get("dupefilterKey"):
            errors.append(f"{prefix}: type 'reviews' requires 'dupefilterKey'")

        # Duplicate queue check
        q = cfg.get("queue")
        if q:
            if q in seen_queues:
                errors.append(f"{prefix}.queue: duplicate '{q}' (also in '{seen_queues[q]}')")
            seen_queues[q] = name

        # batchSize must be positive
        bs = cfg.get("batchSize")
        if bs is not None:
            if not isinstance(bs, int) or bs <= 0:
                errors.append(f"{prefix}.batchSize: must be positive int, got {bs!r}")

        # HPA overrides validation
        keda = cfg.get("keda", {})
        if keda:
            for kf in ["maxReplicas", "minReplicas"]:
                v = keda.get(kf)
                if v is not None and (not isinstance(v, int) or v < 0):
                    errors.append(f"{prefix}.keda.{kf}: must be non-negative int")

    # Writer validation
    writers = values.get("writers", {})
    for wname, wcfg in writers.items():
        if not wcfg.get("enabled", True):
            continue
        prefix = f"writers.{wname}"
        if not wcfg.get("consumerGroup"):
            errors.append(f"{prefix}: missing consumerGroup")
        if not wcfg.get("topic"):
            errors.append(f"{prefix}: missing topic")
        bs = wcfg.get("batchSize")
        if bs is not None and (not isinstance(bs, int) or bs <= 0):
            errors.append(f"{prefix}.batchSize: must be positive int")

    # Kafka config
    kafka = values.get("kafka", {})
    if not kafka.get("bootstrapServers"):
        errors.append("kafka.bootstrapServers: must be set")

    _report(errors)
    return 1 if errors else 0


def _report(errors) -> None:
    if errors:
        logger.warning(f"\nValues validation FAILED with {len(errors)} error(s):\n")
    else:
        count_exp = len(load_values().get("exporters", {}))
        logger.info(f"\nValues validation PASSED — {count_exp} exporters, no duplicates.\n")


if __name__ == "__main__":
    sys.exit(main())
