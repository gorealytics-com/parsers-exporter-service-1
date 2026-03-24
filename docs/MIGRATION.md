# Migration Map: parsers-exporter-service → aragog-exporters

Complete file-by-file mapping. Every file from the old project is accounted for.

## Root Files

| Old File | New Location | Action |
|---|---|---|
| `enhanced_main.py` | — | **Eliminated.** Per-service `main.py` replaces central entrypoint. |
| `main.py` | — | **Eliminated.** Legacy entrypoint, superseded by `enhanced_main.py` already. |
| `api.py` | `services/exporter-base/base.py` (`_run_health_server`) | **Simplified.** Only `/health`, `/ready`, `/metrics`. Pause/resume/recreate endpoints removed (K8s handles lifecycle). |
| `cartography.py` | `services/org-exporter/main.py` (validation) + `services/pg-writer/main.py` (DB writes) | **Split.** Business logic in exporter, SQL in writer. |
| `reviews.py` | `services/review-exporter/main.py` (validation+dedup) + `services/ch-writer/main.py` (CH insert) | **Split.** |
| `config.yaml` | `helm/aragog-exporters/values.yaml` | **Migrated.** All 37 exporters transferred. |
| `config.staging.yaml` | `helm/aragog-exporters/values-staging.yaml` | **Migrated.** Helm override file. |
| `Dockerfile` | `docker/Dockerfile.exporter` + `docker/Dockerfile.writer` | **Upgraded.** Multi-stage, Python 3.11, non-root user, HEALTHCHECK. |
| `Dockerfile.seeder` | `docker/Dockerfile.seeder` | **Ported.** Updated to read from values.yaml. |
| `docker-compose.yml` | — | **Eliminated.** Replaced by Helm chart for K8s. |
| `docker-compose.staging.yml` | `docker-compose.dev.yml` | **Rewritten.** Adds Kafka, PgBouncer, Kafka UI. |
| `requirements.txt` | Per-service `requirements.txt` in each `services/*/` | **Split.** Each service has minimal deps. |
| `requirements-dev.txt` | `requirements-dev.txt` | **Updated.** Added pytest-asyncio, aiokafka, asyncpg. |
| `ruff.toml` | `ruff.toml` | **Updated.** Target Python 3.11, isort config. |
| `pytest.ini` | `pytest.ini` | **Updated.** Added `asyncio_mode = auto`. |
| `.gitignore` | `.gitignore` | **Updated.** Added Helm chart paths. |
| `.env.example` | `.env.example` | **Updated.** New env vars (KAFKA, PG_URI, etc.). |
| `.python-version` | `.python-version` | **Bumped.** 3.9 → 3.11. |
| `__init__.py` | — | **Eliminated.** Root package not needed. |

## aragog_exporter_service/

| Old File | New Location | Action |
|---|---|---|
| `enhanced_export_manager.py` | — | **Eliminated.** K8s Deployments + HPA replace process orchestration. |
| `export_manager.py` | — | **Eliminated.** Older version of above, already unused. |
| `exporter.py` (`BaseExporter` + `ExporterService`) | `services/exporter-base/base.py` | **Rewritten.** `BaseExporter` → `BaseValidator` (async). `ExporterService` main loop → `ExporterRunner` (asyncio, no Dask). |
| `threaded_process_monitor.py` | — | **Eliminated.** K8s liveness/readiness probes. |

## aragog_exporter_service/utils/

| Old File | New Location | Action |
|---|---|---|
| `config_loader.py` | `services/exporter-base/base.py` (`ExporterConfig.from_env()`) | **Replaced.** YAML+env-var substitution → pure env vars from Helm. |
| `configs.py` (`ExporterConfig` dataclass) | `services/exporter-base/base.py` (`ExporterConfig`) | **Replaced.** |
| `conflict_strategy.py` | `services/pg-writer/main.py` (`build_upsert_sql()`) | **Inlined.** `OnConflictUpdateStrategy.apply()` logic is now in the SQL builder. |
| `db.py` (`are_all_parsers_finished`) | — | **Eliminated.** Was used by triggers. HPA replaces trigger logic. |
| `default_triggers.py` | — | **Eliminated.** K8s HPA replaces queue-length triggers. |
| `delivery_models.py` | `libs/common/models.py` (`DELIVERY_ORG_EXTRA_NOTES`) | **Documented.** Was marked "TODO do not use". Kept as schema reference. |
| `dupefilter_prefill_loader.py` | `services/review-exporter/main.py` (`on_startup`, `_sync_prefill_dupefilter`) | **Ported.** Runs at pod startup in thread executor. |
| `dynamic_importer.py` | — | **Eliminated.** One exporter class per Docker image, no dynamic loading needed. |
| `logger.py` | `services/exporter-base/base.py` (`setup_logging`, `_JSONFmt`) | **Replaced.** JSON structured logging for Promtail/Loki. |
| `models.py` | `libs/common/models.py` | **Ported.** Column definitions as dicts (not SQLAlchemy ORM). Schema reference for pg-writer and init-scripts. |
| `notifier.py` (`TelegramNotifier`) | `libs/common/notifier.py` | **Ported.** Rewritten as async (aiohttp). Transition period before Alertmanager. |
| `shared_metrics.py` (`SharedMetricsManager`) | — | **Eliminated.** Dask Variables → Prometheus counters per pod. |
| `shared_state.py` (`SharedStateManager`) | — | **Eliminated.** Redis-based pause/resume → K8s `replicas=0` to pause. |
| `staging_triggers.py` | — | **Eliminated.** K8s HPA with `values-staging.yaml` overrides. |
| `stats.py` (`ServiceStats`, `BatchProcessingResult`) | — | **Eliminated.** Prometheus counters replace in-memory stats. |
| `trigger.py` (`TriggerEngine`) | — | **Eliminated.** HPA. |
| `trigger_loader.py` | — | **Eliminated.** HPA. |

## init-scripts/

| Old File | New Location | Action |
|---|---|---|
| `postgres/01-init-schemas.sql` | `init-scripts/postgres/01-init-schemas.sql` | **Copied as-is.** Same 28 schemas, same table structures. |
| `clickhouse/01-init-tables.sql` | `init-scripts/clickhouse/01-init-tables.sql` | **Copied as-is.** Same 6 review databases. |

## scripts/

| Old File | New Location | Action |
|---|---|---|
| `ci_validate.py` | `scripts/ci_validate.py` | **Rewritten.** Validates `values.yaml` instead of `config.yaml`. |
| `deploy.sh` | `scripts/deploy.sh` | **Rewritten.** Helm-based deploy instead of docker-compose + API calls. |
| `staging_seeder.py` | `scripts/staging_seeder.py` | **Ported.** Reads from `values.yaml`, same Faker data generation. |
| `generate_staging_config.py` | — | **Eliminated.** `values-staging.yaml` Helm override replaces runtime config generation. |
| `bootstrap_staging.sh` | — | **Eliminated.** `docker-compose.dev.yml` + `helm install` replaces manual bootstrap. |
| `staging_cleanup.sh` | — | **Eliminated.** `helm uninstall` + `docker compose down -v`. |
| `staging_smoke_test.sh` | `.github/workflows/cd-staging.yml` (smoke-tests job) | **Inlined.** kubectl checks in CI workflow. |

## .github/workflows/

| Old File | New Location | Action |
|---|---|---|
| `ci-tests.yml` | `.github/workflows/ci-tests.yml` | **Ported.** Added Kafka service, Helm lint, Python 3.11. |
| `cd-production.yml` | `.github/workflows/cd-production.yml` | **Ported.** Semantic Release + Docker matrix build + Helm deploy. |
| `cd-staging.yml` | `.github/workflows/cd-staging.yml` | **Ported.** Helm deploy with staging overrides. |

## docs/

| Old File | New Location | Action |
|---|---|---|
| `DATA_EXPORTERS.md` | `docs/DATA_EXPORTERS.md` | See below. |
| `DEPLOY_HOWTO.md` | `docs/DEPLOY_HOWTO.md` | See below. |
| `DEV_DOC.md` | `docs/DEV_DOC.md` | See below. |
| `cicd-todo.md` | — | **Eliminated.** All CI/CD items implemented. |

> **Note:** Documentation needs rewriting for the new architecture.
> The old docs reference docker-compose, multiprocessing, Dask, and config.yaml.
> New docs should cover: Helm values, HPA scaling, Kafka topics, asyncio services.
> This is a Phase 5 (Hardening) task.

## tests/

| Old File | Status |
|---|---|
| `conftest.py` | Needs rewrite for async fixtures (pytest-asyncio). |
| `unit/test_queue_and_transform.py` | Business logic tests still valid, need async adaptation. |
| `integration/test_upsert_and_flow.py` | Needs Kafka + asyncpg fixtures. |
| `fixtures/` | Needs update for new message envelope format. |
