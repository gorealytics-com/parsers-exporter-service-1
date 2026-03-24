# AI Agent Development Guide — aragog-exporters

## Project overview

aragog-exporters is an Event-Driven Architecture (EDA) system that processes organization and review data from 37 data sources. Data flows through Redis queues → stateless exporter pods → Apache Kafka → database writer pods → PostgreSQL/ClickHouse. Each layer scales independently via HPA on Kubernetes.

## Tech stack

- **Python 3.10+** — asyncio event loop, single-process per pod
- **aiokafka** — async Kafka producer (exporters) and consumer (writers)
- **asyncpg** — async PostgreSQL client, connects via PgBouncer
- **clickhouse-connect** — sync ClickHouse native protocol (run in executor)
- **redis.asyncio** — async Redis client for BLMPOP and SADD dedup
- **loguru** — structured JSON logging with `serialize=True`
- **prometheus-client** — per-pod `/metrics` endpoint
- **UV** — dependency management with workspace and lockfile
- **Helm** — unified chart, per-environment values
- **HPA** — autoscaling by CPU/memory (built-in K8s, no extra operators)
- **Vault Agent Injector** — secrets injected via pod annotations
- **ArgoCD** — GitOps deployment for dev/stage/prod

## Code conventions

### Formatting
- **ruff** + **isort** via pre-commit hooks
- Line length: 120 characters
- Quote style: double quotes
- Run: `ruff check --fix . && ruff format .`

### Type annotations
- Use Python 3.10+ built-in types: `dict`, `list`, `tuple`, `set`
- Union types: `str | None` instead of `Optional[str]`
- Never use: `typing.Dict`, `typing.List`, `typing.Optional`

### Docstrings
- Google style on all public classes and functions
- Include Args/Returns sections for non-trivial functions

### Logging
- **loguru only** — `from loguru import logger`
- Never use `import logging` or `print()`
- f-strings in log messages: `logger.info(f"Processed {count} items")`
- JSON output via `serialize=True` (Loki-compatible)

### Banned patterns
- `import multiprocessing` — breaks K8s cgroup accounting
- `from dask` / `import dask` — replaced by horizontal pod scaling
- `import logging` — replaced by loguru
- `print()` — replaced by loguru
- Sync Redis in exporters — use `redis.asyncio` only

## Project structure

```
services/
├── exporter-base/base.py    Core: ExporterRunner + BaseValidator + ExporterConfig
├── org-exporter/main.py     OrgValidator: hash + split + produce
├── review-exporter/main.py  ReviewValidator: SADD dedup + date filter
├── pg-writer/main.py        PgWriter: asyncpg + PgBouncer + DLQ
└── ch-writer/main.py        ChWriter: 50k batches + lazy clients + DLQ
libs/
├── common/                  Models, health checks, Telegram notifier
└── observability/           Prometheus metrics, loguru setup
helm/aragog-exporters/       Unified Helm chart (37 exporters, 2 writers)
```

## How to add a new exporter

1. Add entry to `helm/aragog-exporters/values.yaml` under `exporters:`:
   ```yaml
   my-new-source:
     type: organization   # or "reviews"
     queue: "my_source:items"
     schema: source_99
     sourceName: "My Source"
     batchSize: 10000
   ```
2. If `type: organization` — the existing `OrgValidator` handles it. No code change needed.
3. If `type: reviews` — add `dupefilterKey: "my_source:reviews_dupefilter"` to the values entry.
4. If custom validation logic is needed — create a new validator class extending `BaseValidator`.
5. Deploy: `helm upgrade` or push to Git for ArgoCD sync.

## How to add a new writer

1. Create `services/my-writer/main.py` following the pattern in `pg-writer/main.py` or `ch-writer/main.py`.
2. Create `services/my-writer/pyproject.toml` with dependencies.
3. Create `docker/Dockerfile.my-writer` (copy from `Dockerfile.writer`).
4. Add the writer to `values.yaml` under `writers:`.
5. Add a deployment template or extend `deployment-writer.yaml`.
6. Add an HPA for the writer.

## Testing

- **Framework**: pytest + pytest-asyncio
- **Fixtures**: fakeredis for Redis mocking
- **Integration**: docker-compose services (Redis, PG, CH, Kafka)
- **Run**: `uv run pytest tests/unit/ -v` or `uv run pytest tests/integration/ -v -m integration`

## Deployment flow

```
Git push → Semantic Release (tag) → Docker build (ghcr.io) → Chart.yaml bump → ArgoCD sync
```

- **dev**: push to `develop` → build dev images → ArgoCD auto-sync `aragog-dev`
- **stage**: push to `staging` → ArgoCD auto-sync `aragog-stage`
- **prod**: push to `main` → Semantic Release → Docker matrix → Chart bump → ArgoCD auto-sync `aragog-prod`

## Common pitfalls

1. **ClickHouse "Too many parts"** — use batch sizes ≥50k for ch-writer, never insert row-by-row.
2. **PG connection limits** — all writers go through PgBouncer (max 50 backend connections). Never create direct pools.
3. **Kafka partition key** — orgs use `place_id` (sequential per-org), reviews use `source_review_id` (even distribution).
4. **Dupefilter prefill** — review-exporter loads existing IDs from ClickHouse at startup. If ClickHouse is down, the pod will fail to start.
5. **HPA scaleDown stabilization: 300s. After a burst, pods stay up for 5 minutes before scaling down.
6. **Vault secrets** — pods use `source /vault/secrets/env` before starting Python. If Vault is unavailable, pods won't start.
