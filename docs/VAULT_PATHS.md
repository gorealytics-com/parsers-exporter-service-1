# Vault Secrets Structure

Vault UI: https://vault.rea:8200/ui

## Path layout

```
org/data/
├── dev/
│   └── aragog_exporters          ← all secrets for dev environment
├── stage/
│   └── aragog_exporters
└── prod/
    └── aragog_exporters
```

## Secret keys at `org/data/{env}/aragog_exporters`

```
REDIS_URL=redis://redis-master.infra.svc:6379/0
PG_URI=postgresql://aragog_user:PASSWORD@pg-master.infra.svc:5432/aragog_db
CLICKHOUSE_HOST=clickhouse.infra.svc
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=PASSWORD
KAFKA_BOOTSTRAP_SERVERS=kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
TELEGRAM_BOT_TOKEN=FAKE_TOKEN
TELEGRAM_CHAT_ID=FAKE_CHAT_ID
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/FAKE
LOG_LEVEL=INFO
```

## How secrets are injected

Vault Agent Injector runs as a sidecar in every pod.
Pod annotations (set by Helm deployment template):

```yaml
vault.hashicorp.com/agent-inject: "true"
vault.hashicorp.com/role: "aragog-{env}-read"
vault.hashicorp.com/agent-inject-secret-env: "org/data/{env}/aragog_exporters"
vault.hashicorp.com/tls-skip-verify: "true"
vault.hashicorp.com/agent-inject-template-env: |
  {{- with secret "org/data/{env}/aragog_exporters" -}}
  {{ range $k, $v := .Data.data }}
  export {{ $k }}="{{ $v }}"
  {{ end }}
  {{- end }}
```

Container startup command sources the file:
```bash
source /vault/secrets/env && exec python -m service.main
```

## Vault roles to create

| Role | Policy | Bound namespace |
|------|--------|-----------------|
| `aragog-dev-read` | read `org/data/dev/*` | `aragog-dev` |
| `aragog-stage-read` | read `org/data/stage/*` | `aragog-stage` |
| `aragog-prod-read` | read `org/data/prod/*` | `aragog-prod` |

## Setup commands (Vault CLI)

```bash
# Enable KV v2 engine (if not already)
vault secrets enable -path=org kv-v2

# Write dev secrets
vault kv put org/data/dev/aragog_exporters \
  REDIS_URL="redis://redis-master.infra.svc:6379/0" \
  PG_URI="postgresql://aragog_user:dev_pass@pg-master.infra.svc:5432/aragog_db" \
  CLICKHOUSE_HOST="clickhouse.infra.svc" \
  CLICKHOUSE_PORT="8123" \
  CLICKHOUSE_USER="default" \
  CLICKHOUSE_PASSWORD="dev_pass" \
  KAFKA_BOOTSTRAP_SERVERS="kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092" \
  TELEGRAM_BOT_TOKEN="fake" \
  TELEGRAM_CHAT_ID="fake" \
  LOG_LEVEL="DEBUG"

# Create policy
vault policy write aragog-dev-read - <<EOF
path "org/data/dev/*" {
  capabilities = ["read"]
}
EOF

# Create K8s auth role
vault write auth/kubernetes/role/aragog-dev-read \
  bound_service_account_names=aragog-exporters-sa \
  bound_service_account_namespaces=aragog-dev \
  policies=aragog-dev-read \
  ttl=1h
```
