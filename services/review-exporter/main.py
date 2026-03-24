"""review-exporter: Validates, deduplicates, and transforms review data.

Ported from: reviews.ReviewsExporter
Keeps:   SADD pipeline dedup, date validation (>=2020), review_middleware,
         dupefilter_prefill_loader on startup
Removes: clickhouse-connect insert (moved to ch-writer)

Kafka message schema::

    {
        "schema": "source_33",
        "source_name": "Tripadvisor",
        "review": {
            "source_product_id": "...",
            "review_language": "...",
            "review_text": "...",
            ...all review fields...
        }
    }
"""

import asyncio
import datetime
import json
import os
import sys
from typing import Any, Optional

import orjson
import redis.asyncio as aioredis
from dateutil import parser as dateparser
from loguru import logger

sys.path.insert(0, "/app")
from exporter_base.base import (
    BaseValidator,
    ExporterConfig,
    ExporterMetrics,
    ExporterRunner,
    setup_logging,
)

# Cutoff year for review date filtering (same as original)
MIN_REVIEW_YEAR = 2020

# Japanese date format used by some sources (Baidu, Yahoo Japan)
JP_DATE_FORMAT = "%Y年%m月%d日"


# ---------------------------------------------------------------------------
# Review Validator
# ---------------------------------------------------------------------------

class ReviewValidator(BaseValidator):
    """Stateless review validator with Redis-based deduplication.

    Dedup flow (ported from reviews.py):
      1. Collect all source_review_id from the batch
      2. Pipeline SADD to dupefilter Redis set (O(1) per item)
      3. SADD returns 1 for new items, 0 for already-seen — filter out 0s
      4. Only new items go through date validation and to Kafka
    """

    def __init__(self, config: ExporterConfig) -> None:
        super().__init__(config)
        self.add_middleware(self._review_middleware)

    # ---- BaseValidator interface ----------------------------------------

    async def validate_batch(
        self,
        raw_items: list[bytes],
        redis: aioredis.Redis,
        metrics: ExporterMetrics,
    ) -> list[dict[str, Any]]:
        """Validate and deduplicate a batch of raw review items."""
        now = datetime.datetime.utcnow()
        batch_timestamp = now
        batch_id = int(now.timestamp())

        # --- Phase 1: Deserialize + middleware ---
        parsed: list[tuple[bytes, dict[str, Any]]] = []
        review_ids: list[str] = []

        for raw in raw_items:
            try:
                item = orjson.loads(raw)
                item = self.apply_middlewares(item)
                parsed.append((raw, item))
                review_ids.append(item.get("source_review_id", ""))
            except Exception:
                logger.warning("Deserialization/middleware error, skipping item")
                metrics.inc_errored()

        if not parsed:
            return []

        # --- Phase 2: Batch dedup via Redis SADD pipeline ---
        if self.config.dupefilter_key:
            is_new_flags = await self._dedup_pipeline(redis, review_ids)
        else:
            is_new_flags = [True] * len(parsed)

        # --- Phase 3: Validate unique items ---
        validated: list[dict[str, Any]] = []

        for (raw, item), is_new in zip(parsed, is_new_flags):
            if not is_new:
                metrics.inc_skipped("duplicate")
                continue

            try:
                review_data = self._validate_review(item, batch_timestamp, batch_id)
                if review_data is None:
                    metrics.inc_skipped("date_filter")
                    continue

                validated.append({
                    "schema": self.config.schema,
                    "source_name": self.config.source_name,
                    "review": review_data,
                })
            except Exception:
                logger.exception("Review validation error, skipping")
                metrics.inc_errored()

        deduped_count = sum(1 for f in is_new_flags if not f)
        if deduped_count > 0:
            logger.info(
                f"Dedup: {deduped_count}/{len(parsed)} already seen, "
                f"{len(parsed) - deduped_count} new, {len(validated)} validated"
            )

        return validated

    def partition_key(self, item: dict[str, Any]) -> Optional[bytes]:
        """Partition by source_review_id for even distribution."""
        rid = item.get("review", {}).get("source_review_id", "")
        return rid.encode("utf-8") if rid else None

    async def on_startup(self, redis: aioredis.Redis) -> None:
        """Dupefilter prefill: load existing review IDs from ClickHouse into Redis set.

        Ported from dupefilter_prefill_loader.fill_reviews_dupefilter_by_source.
        Runs once at pod startup. Uses sync clickhouse-connect (blocking but one-time).
        """
        if not self.config.dupefilter_key or not self.config.schema:
            logger.info("No dupefilter configured, skipping prefill")
            return

        ch_host = os.environ.get("CLICKHOUSE_HOST")
        if not ch_host:
            logger.info("No CLICKHOUSE_HOST set, skipping dupefilter prefill")
            return

        logger.info(
            f"Starting dupefilter prefill: schema={self.config.schema} "
            f"key={self.config.dupefilter_key}"
        )

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._sync_prefill_dupefilter)
        logger.info("Dupefilter prefill complete")

    def _sync_prefill_dupefilter(self) -> None:
        """Load existing source_review_ids from ClickHouse into the Redis dedup set.

        Ported from: dupefilter_prefill_loader.fill_reviews_dupefilter_by_source.
        Sync blocking function run in a thread executor.
        """
        import clickhouse_connect
        import redis as sync_redis

        source_id = self.config.schema
        dupefilter_key = self.config.dupefilter_key

        ch_client = clickhouse_connect.get_client(
            host=os.environ.get("CLICKHOUSE_HOST", ""),
            port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
            username=os.environ.get("CLICKHOUSE_USER", ""),
            password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
            database=source_id,
        )
        redis_conn = sync_redis.from_url(self.config.redis_url)

        try:
            total_in_ch = ch_client.query(
                f"SELECT count(source_review_id) FROM {source_id}.raw_reviews"
            ).result_rows[0][0]

            existing_in_redis = redis_conn.scard(dupefilter_key)

            if existing_in_redis >= total_in_ch and total_in_ch > 0:
                logger.info(
                    f"Dupefilter already filled: redis={existing_in_redis} ch={total_in_ch}"
                )
                return

            logger.info(
                f"Prefilling dupefilter: ch={total_in_ch} redis={existing_in_redis}, loading delta..."
            )

            processed = 0
            with ch_client.query_column_block_stream(
                f"SELECT source_review_id FROM {source_id}.raw_reviews"
            ) as stream:
                for columns in stream:
                    values = columns[0]
                    pipe = redis_conn.pipeline(transaction=False)
                    for i in range(0, len(values), 10_000):
                        pipe.sadd(dupefilter_key, *values[i : i + 10_000])
                    pipe.execute()
                    processed += len(values)

                    if total_in_ch > 0 and processed % 500_000 < len(values):
                        pct = processed * 100 // total_in_ch
                        logger.info(f"Prefill progress: {pct}% ({processed}/{total_in_ch})")

            logger.info(f"Prefill done: loaded {processed} review IDs")

        finally:
            ch_client.close()
            redis_conn.close()

    # ---- Dedup pipeline (ported from reviews.py) -----------------------

    async def _dedup_pipeline(
        self, redis: aioredis.Redis, review_ids: list[str],
    ) -> list[bool]:
        """Batch dedup via Redis SADD pipeline.

        SADD returns 1 if the member was added (new), 0 if already existed.
        """
        async with redis.pipeline(transaction=False) as pipe:
            for rid in review_ids:
                if rid:
                    pipe.sadd(self.config.dupefilter_key, rid)
                else:
                    pipe.echo(b"1")
            results = await pipe.execute()

        return [bool(r) for r in results]

    # ---- Review validation (ported from reviews.py) --------------------

    def _validate_review(
        self,
        item: dict[str, Any],
        batch_timestamp: datetime.datetime,
        batch_id: int,
    ) -> dict[str, Any] | None:
        """Validate a single review item.

        Ported from ReviewsExporter._process_item:
        parse and validate review_date (>= 2020), Japanese date format fallback,
        set defaults for url/review_language, add batch metadata.
        """
        raw_date = item.get("review_date")
        if not raw_date:
            return None

        review_date = None

        try:
            review_date = dateparser.parse(str(raw_date))
            if review_date.year < MIN_REVIEW_YEAR:
                return None
        except Exception:
            try:
                review_date = datetime.datetime.strptime(str(raw_date), JP_DATE_FORMAT)
                if review_date.year < MIN_REVIEW_YEAR:
                    return None
            except Exception:
                return None

        item["review_date"] = review_date.isoformat()
        item["review_parse_date"] = batch_timestamp.isoformat()
        item["batch_timestamp"] = batch_timestamp.isoformat()
        item["batch_id"] = batch_id
        item["url"] = item.get("url", "")
        item["review_language"] = item.get("review_language", "")

        return item

    # ---- Middlewares (ported from reviews.py) ---------------------------

    def _review_middleware(self, item: dict[str, Any]) -> dict[str, Any]:
        """Normalize review fields.

        Ported from ReviewsExporter._review_middleware:
        review_rating -> float, additional_info -> JSON string,
        default source from config.
        """
        try:
            item["review_rating"] = float(item.get("review_rating", "0.0"))
        except (ValueError, TypeError):
            item["review_rating"] = 0.0

        if "additional_info" in item:
            ai = item["additional_info"]
            if not isinstance(ai, str):
                item["additional_info"] = json.dumps(ai, ensure_ascii=False)

        item.setdefault("source", self.config.source_name)

        return item


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

async def main() -> None:
    """Start the review-exporter service."""
    setup_logging(level=os.environ.get("LOG_LEVEL", "INFO"))
    config = ExporterConfig.from_env()
    validator = ReviewValidator(config)
    runner = ExporterRunner(validator, config)
    await runner.start()


if __name__ == "__main__":
    asyncio.run(main())
