# AI Agent Development Guide — aragog-exporters

## Project overview

aragog-exporters is an Event-Driven Architecture (EDA) pipeline processing ~1M crawled records/hour from 37+ data sources. Data flows through 5 layers: Redis queues → 4 exporter types → Kafka topics → 2 writer types → PostgreSQL/ClickHouse. Each layer scales independently via HPA on bare-metal Kubernetes (Hetzner VMs managed via Netbox). The K8s cluster is managed by DevOps — we only deploy our applications through ArgoCD.

## Tech stack

| Component | Library | Version | Notes |
|-----------|---------|---------|-------|
| Runtime | Python | 3.10+ | asyncio single event loop per pod |
| Event loop | uvloop | ≥0.21 | 2-4x I/O improvement, Linux only |
| Serialization | msgspec | ≥0.19 | JSON encode/decode, Struct validation, replaces orjson |
| Kafka | aiokafka | ≥0.10 | Async producer (exporters) and consumer (writers) |
| PostgreSQL | asyncpg | ≥0.29 | COPY-based upsert via PgBouncer, statement_cache_size=0 |
| ClickHouse | clickhouse-connect | ≥0.7 | Sync native protocol, run_in_executor (justified: >1s) |
| Redis | redis-py async | ≥5.0 | BLMPOP, SADD pipeline, connection pool |
| Logging | loguru | ≥0.7 | `serialize=True` for Loki-compatible JSON |
| Metrics | prometheus-client | ≥0.20 | Per-pod /metrics, /health, /ready |
| HTTP | FastAPI + uvicorn | ≥0.110 | Health/ready probes + Prometheus metrics endpoint |
| Deps | UV | latest | Workspace with lockfile |
| Formatting | ruff + isort | ≥0.4 | Pre-commit hooks |
| Helm | Helm v3 | latest | Unified chart, per-env values |
| Secrets | Vault Agent Injector | - | Pod annotations, source /vault/secrets/env |
| Deploy | ArgoCD | - | GitOps: dev (auto) / stage (auto) / prod (manual) |
| Versioning | Semantic Release | - | Git tag = Docker tag = Chart appVersion |

## Code conventions

### Formatting
- **ruff** + **isort** via pre-commit hooks
- Line length: 120 characters
- Quote style: double quotes
- Run: `ruff check --fix . && ruff format .`

### Type annotations
- Python 3.10+ built-in types: `dict`, `list`, `tuple`, `set`
- Union types: `str | None` instead of `Optional[str]`
- Return types: `dict[str, Any] | None` not `Optional[dict[str, Any]]`
- **Never use**: `typing.Dict`, `typing.List`, `typing.Optional`, `typing.Tuple`

### Serialization — msgspec only
```python
# CORRECT — module-level singletons from exporter_base.base
from exporter_base.base import json_encode, json_decode

data = json_decode(raw_bytes)        # replaces orjson.loads()
kafka_bytes = json_encode(data)      # replaces orjson.dumps()

# CORRECT — typed decode with validation
import msgspec
decoder = msgspec.json.Decoder(OrgData)
struct = decoder.decode(raw_bytes)   # parse + validate + construct in one pass

# WRONG — never use these
import orjson         # ❌ removed from project
import json           # ❌ use msgspec
from pydantic import  # ❌ 12x slower
```

### Struct schemas (data contracts)
```python
import msgspec

class OrgData(msgspec.Struct, kw_only=True, omit_defaults=True, gc=False):
    """gc=False: no GC tracking for pipeline records (75x faster GC pauses)."""
    place_id: str
    name: str
    rating: float | None = None  # NOT Optional[float]
```

### Docstrings
- Google style on all public classes and functions
- Include Args/Returns for non-trivial functions:
```python
def compute_hash(item: dict[str, Any]) -> str:
    """Compute SHA1 hash of key org fields for change detection.

    Args:
        item: Raw organisation dict from Redis.

    Returns:
        Hex digest of SHA1 hash, or empty string if no hashable fields.
    """
```

### Logging
- **loguru only** — `from loguru import logger`
- Never use `import logging` or `print()`
- f-strings in log messages: `logger.info(f"Processed {count} items")`
- JSON output via `serialize=True` (Loki-compatible)
- Log at **batch level**, not per-record (avoid log spam at 1M/hr)

### Banned patterns
| Pattern | Why banned | Alternative |
|---------|-----------|-------------|
| `import multiprocessing` | Breaks K8s cgroup accounting | Scale via HPA pods |
| `from dask` / `import dask` | 200MB overhead per cluster | asyncio event loop |
| `import logging` | Replaced by loguru | `from loguru import logger` |
| `print()` | Not structured, not JSON | `logger.info()` |
| `import orjson` | Replaced by msgspec | `json_encode()` / `json_decode()` |
| `typing.Optional` | Python 3.10+ syntax | `str \| None` |
| `asyncio.ensure_future(producer.send())` | aiokafka throughput bug (25x drop) | Sequential `await producer.send()` |
| `run_in_executor` for <1ms ops | 40-50μs overhead per call | Run in event loop directly |

## Project structure

```
services/
├── exporter-base/base.py        Core: ExporterRunner + BaseValidator + ExporterConfig
│                                 + json_encode() / json_decode() msgspec helpers
├── org-exporter/main.py          OrgValidator: hash → split org/contacts/menu → produce
├── delivery-exporter/main.py     Uses OrgValidator, separate Deployment for delivery sources
├── sitemap-exporter/main.py      Uses OrgValidator, separate Deployment for sitemap sources
├── review-exporter/main.py       ReviewValidator: SADD dedup → date filter → produce
├── pg-writer/main.py             COPY staging → INSERT ON CONFLICT (10-50x faster)
└── ch-writer/main.py             Buffer 50K+ rows → clickhouse-connect INSERT

libs/common/
├── schemas/                      msgspec Struct contracts (OrgData, Contact, MenuItem, Review)
├── health.py                     Health check utilities
├── models.py                     Shared data models
└── notifier.py                   Telegram/Slack notifications

libs/observability/
└── metrics.py                    Prometheus metric definitions

helm/aragog-exporters/            Unified Helm chart (replaces old config.yaml)
├── values.yaml                   All 37 exporter configs
├── values-{dev,stage,prod}.yaml  Vault paths + resource overrides
├── dashboards/                   3 Grafana dashboard JSONs
└── templates/                    12 K8s templates
```

## How to add a new source (zero code changes)

### Organization source (map/delivery)
1. Add entry to `helm/aragog-exporters/values.yaml` under `exporters:`:
   ```yaml
   my_new_source:
     type: organization    # uses OrgValidator automatically
     queue: "my_source:items"
     schema: source_99
     sourceName: "My Source"
     batchSize: 10000
   ```
2. Create PostgreSQL schema DDL:
   ```sql
   CREATE SCHEMA IF NOT EXISTS source_99;
   -- Then create organisation_data, organisation_contacts, menu_data tables
   -- (copy from any existing schema in init-scripts/postgres/)
   ```
3. `git push` → ArgoCD syncs → new exporter pod starts automatically.
4. **No Python code changes needed.**

### Review source
1. Add to `values.yaml` with `type: reviews` and `dupefilterKey`:
   ```yaml
   my_reviews:
     type: reviews
     queue: "my_reviews:items"
     schema: source_99
     sourceName: "My Source"
     dupefilterKey: "my_source:reviews_dupefilter"
     batchSize: 10000
   ```
2. Create ClickHouse database + raw_reviews table (copy from init-scripts/clickhouse/).
3. `git push` → done.

### Custom validation logic
If a new source requires different validation (not OrgValidator or ReviewValidator):
1. Create `services/my-exporter/main.py` with a new class extending `BaseValidator`.
2. Implement `validate_batch()` and `partition_key()`.
3. Add to `pyproject.toml` workspace members.
4. Create Dockerfile entry or extend `Dockerfile.exporter`.

## How to add a new writer

1. Create `services/my-writer/main.py` following `pg-writer/main.py` pattern.
2. Create `services/my-writer/pyproject.toml` with dependencies.
3. Add build matrix entry to `.github/workflows/cd-*.yml`.
4. Add writer section to `values.yaml` and extend `deployment-writer.yaml`.

## Concurrency model

**One asyncio event loop per pod.** No multiprocessing, no threading (except run_in_executor for ch-writer sync INSERT). Scale horizontally via HPA pod count.

```
Pod process:
  └── asyncio event loop (uvloop)
      ├── consume_loop()       ← Redis BLMPOP, msgspec decode (~5μs), validate
      ├── produce_loop()       ← Sequential Kafka send (NOT ensure_future!)
      └── health_server()      ← FastAPI /health /ready /metrics
```

**Performance budget per pod:**
- Current: 278 msg/s (1M/hr average) — uses ~1% of event loop capacity
- Max: 10,000-50,000 msg/s per pod — 36-180x headroom
- msgspec decode: ~5 μs/record, SHA1 hash: <1 μs, Kafka send: ~200 μs (batched)

**Critical rule:** `run_in_executor` only for operations >1ms:
- ✅ clickhouse-connect INSERT (1-60s) — justified
- ✅ dupefilter prefill from CH (10-60s) — justified
- ❌ msgspec decode (~5μs) — 40μs executor overhead makes it 8x slower
- ❌ SHA1 hash (<1μs) — same problem

## Testing

- **Framework**: pytest + pytest-asyncio
- **Unit tests**: `uv run pytest tests/unit/ -v` (no infra needed)
- **Integration tests**: `uv run pytest tests/integration/ -v -m integration`
  - Requires: `docker compose -f docker-compose.dev.yml up -d`
  - Services: Redis (:16379), PG (:15432), CH (:18123), Kafka (:19092)
- **Fixtures**: fakeredis for Redis mocking, testcontainers for integration

## Deployment flow

```
develop branch → ci-tests → docker build (6 images, dev-{sha}) → ArgoCD auto-sync → aragog-dev
staging branch → ci-tests → docker build (stage-{sha})          → ArgoCD auto-sync → aragog-stage
main branch    → ci-tests → semantic-release (v1.3.0)
                           → docker build (v1.3.0 + latest)
                           → Chart.yaml bump (appVersion: v1.3.0)
                           → ArgoCD manual sync → aragog-prod
```

**Environments:**
| Env | Namespace | Vault path | Slack channel | ArgoCD |
|-----|-----------|-----------|---------------|--------|
| dev | aragog-dev | `org/data/dev/aragog_exporters` | `#parsing-exporters-dev` | [argocd.dev.rea](https://argocd.dev.rea/applications) |
| stage | aragog-stage | `org/data/stage/aragog_exporters` | `#parsing-exporters-stage` | — |
| prod | aragog-prod | `org/data/prod/aragog_exporters` | `#parsing-exporters-prod` | [argocd.prod.rea](https://argocd.prod.rea/applications) |

## Monitoring

- **Grafana dev**: https://grafana.dev.k8s.rea/login
- **Grafana prod**: https://grafana.prod.k8s.rea
- **Vault**: https://vault.rea:8200/ui

### Key Prometheus metrics

| Metric | Type | Labels | Alert threshold |
|--------|------|--------|----------------|
| `aragog_exporter_items_produced_total` | Counter | exporter, type | — |
| `aragog_exporter_items_skipped_total` | Counter | exporter, type, reason | — |
| `aragog_exporter_batch_duration_seconds` | Histogram | exporter | >30s warning |
| `aragog_exporter_redis_queue_length` | Gauge | exporter, queue | >100K warning |
| `aragog_exporter_kafka_produce_errors_total` | Counter | exporter | >0 for 10m critical |
| `aragog_writer_records_written_total` | Counter | writer, schema, table | — |
| `aragog_writer_errors_total` | Counter | writer, schema, table | >0 for 10m critical |
| `aragog_writer_batch_write_seconds` | Histogram | writer | >60s warning |
| `aragog_writer_buffer_size` | Gauge | writer | >100K for 15m warning |

## Common pitfalls

1. **ClickHouse "Too many parts"** — use batch sizes ≥50K for ch-writer, never insert row-by-row. Each INSERT creates data parts; too many small inserts fragment storage.

2. **PG connection limits** — all writers connect through PgBouncer in transaction mode. Use `statement_cache_size=0` in asyncpg pool (PgBouncer can't track prepared statements). Never create direct pools bypassing PgBouncer.

3. **Kafka partition key** — orgs use `place_id` (per-org ordering preserved), reviews use `source_review_id` (even distribution). Wrong keys break ordering guarantees.

4. **aiokafka concurrent send bug** — never use `asyncio.ensure_future(producer.send(...))`. The accumulator has a known recursion issue that drops throughput from ~10K to ~400 msg/s. Always sequential `await producer.send()`.

5. **Dupefilter prefill** — review-exporter loads existing review IDs from ClickHouse at startup via `run_in_executor`. If ClickHouse is unreachable, the pod starts without dedup (logs a warning, doesn't crash).

6. **HPA scaleDown stabilization** — 300s window. After a burst, pods stay up for 5 minutes. This prevents flapping during intermittent queue spikes.

7. **Vault unavailability** — pods use `source /vault/secrets/env` before starting Python. If Vault Agent can't inject secrets, the pod enters CrashLoopBackOff. Check Vault health first.

8. **COPY upsert fallback** — pg-writer uses COPY to staging table for 10-50x speed. If COPY fails (e.g., type mismatch), it automatically falls back to executemany for that batch. Check logs for "falling back to executemany" warnings.

9. **msgspec gc=False** — schemas use `gc=False` for pipeline Structs (75x faster GC pauses). This is safe because pipeline records don't have cyclic references. Do NOT use `gc=False` on long-lived objects that reference each other.

10. **Redis BLMPOP starvation** — BLMPOP pops from the first non-empty list only. If one queue is always full, others starve. The current design uses one exporter pod per queue, avoiding this issue.
