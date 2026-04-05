# aragog-exporters

Event-Driven pipeline for processing ~1M crawled records/hour: 37+ Redis queues
→ 4 exporter types → Kafka → PostgreSQL/ClickHouse on bare-metal Kubernetes.

## Architecture

```mermaid
graph TD
    subgraph Redis["Redis (37+ input queues)"]
        R1["bing_map_search:items"]
        R2["tripadvisor_search:items"]
        R3["delivery:source_27:items"]
        R4["delivery:source_27:sitemap:items"]
        R5["tripadvisor_reviews:items"]
        R6["...34 more queues"]
    end

    subgraph Exporters["Exporter Pods (4 Deployment types)"]
        E1["MapExporter<br/>8 map sources"]
        E2["DeliveryExporter<br/>23 delivery sources"]
        E3["SitemapExporter<br/>3 sitemap sources"]
        E4["ReviewsExporter<br/>6 review sources"]
    end

    subgraph Kafka["Kafka (4 data-type topics)"]
        K1["orgs.validated.v1<br/>6 partitions"]
        K2["reviews.validated.v1<br/>6 partitions"]
        K3["orgs.dlq.v1"]
        K4["reviews.dlq.v1"]
    end

    subgraph Writers["Writer Pods (2 Deployment types)"]
        W1["pg-writer<br/>COPY upsert via PgBouncer"]
        W2["ch-writer<br/>batch INSERT 50K+ rows"]
    end

    subgraph DB["Databases"]
        PG["PostgreSQL<br/>28 schemas, 84 tables"]
        CH["ClickHouse<br/>6 databases, MergeTree"]
    end

    R1 & R2 --> E1
    R3 --> E2
    R4 --> E3
    R5 --> E4

    E1 & E2 & E3 -->|"msgspec encode"| K1
    E4 -->|"msgspec encode"| K2

    K1 --> W1
    K2 --> W2

    W1 -->|"asyncpg COPY"| PG
    W2 -->|"clickhouse-connect"| CH

    W1 -.->|"on failure"| K3
    W2 -.->|"on failure"| K4
```

## Exporter class hierarchy

```mermaid
classDiagram
    class BaseValidator {
        <<abstract>>
        +config: ExporterConfig
        +validate_batch(raw_items, redis, metrics)*
        +partition_key(item)*
        +on_startup(redis)
        +on_shutdown()
        +apply_middlewares(item)
    }

    class OrgValidator {
        +HASH_KEYS: list
        +validate_batch() → org+contacts+menu envelope
        +partition_key() → place_id
        -_hash_middleware()
        -_prepare_item_data()
    }

    class ReviewValidator {
        +validate_batch() → review envelope
        +partition_key() → source_review_id
        +on_startup() → dupefilter prefill from CH
        -_dedup_pipeline() → Redis SADD
        -_validate_review() → date filter ≥2020
        -_review_middleware()
    }

    BaseValidator <|-- OrgValidator
    BaseValidator <|-- ReviewValidator

    OrgValidator <-- MapExporter : "uses"
    OrgValidator <-- DeliveryExporter : "same validator"
    OrgValidator <-- SitemapExporter : "same validator"
    ReviewValidator <-- ReviewsExporter : "uses"

    class ExporterRunner {
        +start() → connect + main loop
        +stop() → flush + close
        -_process_batch() → pop → validate → produce
    }

    ExporterRunner --> BaseValidator : "delegates to"
```

## CI/CD pipeline

```mermaid
graph LR
    A["git push"] --> B["GitHub Actions CI<br/>ruff + pytest"]
    B --> C["Semantic Release<br/>v1.3.0 tag"]
    C --> D["Docker matrix<br/>6 images → ghcr.io"]
    D --> E["Chart.yaml bump<br/>appVersion: v1.3.0"]
    E --> F["ArgoCD sync"]
    F --> G1["dev auto-sync"]
    F --> G2["stage auto-sync"]
    F --> G3["prod manual sync"]
```

## Tech stack

- **Python 3.10+**, asyncio single event loop per pod, **uvloop** for 2-4x I/O
- **msgspec** — JSON serialization/validation (replaces orjson, 12x faster than
  Pydantic)
- **aiokafka** — async Kafka producer/consumer
- **asyncpg** — async PostgreSQL with COPY-based upsert via PgBouncer
- **clickhouse-connect** — ClickHouse native protocol (sync, in executor)
- **loguru** — structured JSON logging (Loki-compatible)
- **prometheus-client** — per-pod `/metrics` endpoint
- **UV** — dependency management with workspace and lockfile
- **Helm** — unified chart with per-environment values
- **HPA** — autoscaling by CPU/memory
- **Vault Agent Injector** — secrets injection via pod annotations
- **ArgoCD** — GitOps deployment (dev/stage/prod)
- **Semantic Release** — auto-versioning, Docker tag = Git tag = Chart
  appVersion

## Quick start

```bash
# Start infrastructure (Redis, PG, PgBouncer, CH, Kafka, Kafka UI)
docker compose -f docker-compose.dev.yml up -d

# Start full pipeline including exporters and writers
docker compose -f docker-compose.dev.yml --profile app up -d --build

# Or run a single exporter locally
uv sync
REDIS_URL=redis://localhost:16379/0 \
KAFKA_BOOTSTRAP_SERVERS=localhost:19092 \
EXPORTER_NAME=bing_map_search \
REDIS_QUEUE=bing_map_search:items \
KAFKA_TOPIC=orgs.validated.v1 \
SCHEMA=source_42 \
SOURCE_NAME="Bing Org Update" \
BATCH_SIZE=100 \
LOG_LEVEL=DEBUG \
  uv run python -m org_exporter.main
```

## Adding a new source (zero code changes)

1. Add entry to `helm/aragog-exporters/values.yaml`:
   ```yaml
   my_new_source:
       type: organization
       queue: "my_source:items"
       schema: source_99
       sourceName: "My Source"
       batchSize: 10000
   ```
2. Create PostgreSQL schema: `CREATE SCHEMA source_99` + 3 tables DDL
3. `git push` → ArgoCD syncs → new pod starts automatically

## Code conventions

- **Formatting**: ruff + isort (pre-commit hooks), line length 120
- **Types**: Python 3.10+ built-in (`dict`, `list`, `str | None`), never
  `typing.Optional`
- **Serialization**: msgspec only, never orjson/json/pydantic
- **Docstrings**: Google style on all public functions
- **Logging**: loguru only, never `print()` or stdlib `logging`
- **Banned**: `multiprocessing`, `dask`, sync Redis in exporters

## Monitoring

- **Grafana**: [dev](https://grafana.dev.k8s.rea/login) |
  [prod](https://grafana.prod.k8s.rea)
- **ArgoCD**: [dev](https://argocd.dev.rea/applications) |
  [prod](https://argocd.prod.rea/applications)
- **Vault**: [vault.rea:8200/ui](https://vault.rea:8200/ui)
- **Slack alerts**: `#parsing-exporters-dev` / `#parsing-exporters-stage` /
  `#parsing-exporters-prod`

## Deployment

| Branch    | Environment | Docker tag             | Sync   |
| --------- | ----------- | ---------------------- | ------ |
| `develop` | dev         | `dev-{sha}`            | auto   |
| `staging` | stage       | `stage-{sha}`          | auto   |
| `main`    | prod        | `v{semver}` + `latest` | manual |

See `docs/agents.md` for the full AI agent development guide.
