"""org-exporter (map-exporter): Validates and transforms organization data.

Ported from: cartography.CartographyOrgDataExporter
Keeps:   hash middleware, _prepare_item_data, in-batch place_id dedup
Removes: SQLAlchemy, QueuePool, INSERT ON CONFLICT (moved to pg-writer)

Used by: map-exporter, delivery-exporter, sitemap-exporter
(all 3 share OrgValidator — separated by Deployment type for HPA isolation)

Serialization: msgspec.json for decode (Redis bytes) and encode (Kafka).

Kafka message schema::

    {
        "schema": "source_42",
        "source_name": "Bing Org Update",
        "org": { ...organisation_data fields... },
        "contacts": { ...contacts fields or empty dict... },
        "menu": [ ...menu items list... ]
    }
"""

import asyncio
import datetime
import json
import os
import sys
from hashlib import sha1
from typing import Any

import redis.asyncio as aioredis
from loguru import logger

sys.path.insert(0, "/app")
from exporter_base.base import (  # TODO Import "exporter_base.base" could not be resolved
    BaseValidator,
    ExporterConfig,
    ExporterMetrics,
    ExporterRunner,
    json_decode,
    setup_logging,
)


class OrgValidator(BaseValidator):
    """Stateless org data validator/transformer.

    Input:  Raw JSON bytes from Redis (decoded via msgspec.json)
    Output: Structured dict for Kafka (org + contacts + menu envelope)
    Key:    place_id (guarantees per-org ordering on Kafka partition)
    """

    HASH_KEYS = [
        "name",
        "categories",
        "city",
        "country",
        "link",
        "reviews",
        "rate",
        "lon",
        "lat",
        "address",
        "phone",
        "website",
        "price_range",
        "opening_hrs",
        "additional_info",
        "alternative_name",
    ]

    def __init__(self, config: ExporterConfig) -> None:
        super().__init__(config)
        self.add_middleware(self._hash_middleware)

    async def validate_batch(
        self,
        raw_items: list[bytes],
        redis: aioredis.Redis,
        metrics: ExporterMetrics,
    ) -> list[dict[str, Any]]:
        """Validate a batch of raw org items.

        Ported from CartographyOrgDataExporter.process_batch():
        msgspec decode, hash middleware, extract contacts/menu,
        dedup by place_id within batch, build Kafka message envelope.
        """
        now = datetime.datetime.utcnow()
        batch_timestamp = now
        batch_id = int(now.timestamp())

        validated: list[dict[str, Any]] = []
        seen_place_ids: set[str] = set()
        seen_menu_ids: set[str] = set()

        for raw in raw_items:
            try:
                # msgspec.json.decode replaces orjson.loads
                item = json_decode(raw)
            except Exception:
                logger.warning("Failed to deserialize item, skipping")
                metrics.inc_errored()
                continue

            try:
                item = self.apply_middlewares(item)

                place_id = item.get("place_id")
                if not place_id:
                    metrics.inc_skipped("no_place_id")
                    continue

                if place_id in seen_place_ids:
                    metrics.inc_skipped("duplicate_place_id")
                    continue
                seen_place_ids.add(place_id)

                org_data, contacts, menu_items = self._prepare_item_data(
                    item,
                    batch_timestamp,
                    batch_id,
                )

                deduped_menu: list[dict[str, Any]] = []
                for mi in menu_items:
                    mid = mi.get("id")
                    if mid and mid not in seen_menu_ids:
                        seen_menu_ids.add(mid)
                        mi["update_timestamp"] = batch_timestamp.isoformat()
                        mi["update_batch_id"] = batch_id
                        deduped_menu.append(mi)

                validated.append(
                    {
                        "schema": self.config.schema,
                        "source_name": self.config.source_name,
                        "org": org_data,
                        "contacts": contacts,
                        "menu": deduped_menu,
                    }
                )

            except Exception:
                logger.exception("Validation error, skipping item")
                metrics.inc_errored()

        skipped = len(raw_items) - len(validated)
        if skipped > 0:
            logger.info(f"Batch: {len(validated)} validated, {skipped} skipped/errored")

        return validated

    def partition_key(self, item: dict[str, Any]) -> bytes | None:
        """Partition by place_id for per-org ordering."""
        place_id = item.get("org", {}).get("place_id", "")
        return place_id.encode("utf-8") if place_id else None

    # ---- Middlewares (ported from cartography.py) -----------------------

    def _hash_middleware(self, item: dict[str, Any]) -> dict[str, Any]:
        """Compute SHA1 data_hash from key fields for change detection.

        Runs in event loop (~<1 μs) — no executor needed.
        """
        data_to_hash = {k: item[k] for k in self.HASH_KEYS if item.get(k)}
        if data_to_hash:
            item["data_hash"] = sha1(json.dumps(data_to_hash, sort_keys=True).encode()).hexdigest()
        return item

    # ---- Data preparation (ported from cartography.py) -----------------

    @staticmethod
    def _prepare_item_data(
        item: dict[str, Any],
        batch_timestamp: datetime.datetime,
        batch_id: int,
    ) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
        """Split raw item into (org_data, contacts, menu).

        Ported from CartographyOrgDataExporter._prepare_item_data.
        """
        additional_info = item.get("additional_info") or {}
        contact_record: dict[str, Any] = {}
        if isinstance(additional_info, dict):
            contact_record = additional_info.pop("contacts", {})
            if not isinstance(contact_record, dict):
                contact_record = {}

        phone = item.pop("phone", None)
        if phone:
            contact_record["phones"] = phone.split(",")

        email = item.pop("email", None)
        if email:
            contact_record["mails"] = [email]

        website = item.pop("website", None)
        if website:
            contact_record["web_link"] = website

        menu = item.pop("menu", [])
        if not isinstance(menu, list):
            menu = []

        if "update_timestamp" not in item:
            item["update_timestamp"] = batch_timestamp.isoformat()

        item["update_batch_id"] = batch_id
        item["batch_timestamp"] = batch_timestamp.isoformat()
        item["batch_id"] = batch_id

        if contact_record:
            contact_record["place_id"] = item.get("place_id", "")

        return item, contact_record, menu


# ---------------------------------------------------------------------------
# Entrypoint — uvloop for 2-4x I/O improvement
# ---------------------------------------------------------------------------


async def main() -> None:
    """Start the org-exporter service."""
    setup_logging(level=os.environ.get("LOG_LEVEL", "INFO"))
    config = ExporterConfig.from_env()
    validator = OrgValidator(config)
    runner = ExporterRunner(validator, config)
    await runner.start()


if __name__ == "__main__":
    try:
        import uvloop

        uvloop.run(main())
    except ImportError:
        asyncio.run(main())
