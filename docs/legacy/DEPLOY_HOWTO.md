> **⚠️ LEGACY DOCUMENT** — Ported from parsers-exporter-service. References to docker-compose, multiprocessing, Dask, and config.yaml are outdated. See README.md for the new architecture.

# Parsers Exporter Service — Deployment Guide

## Оглавление

- [Архитектура CI/CD](#архитектура-cicd)
- [Предварительная настройка](#предварительная-настройка)
- [Повседневный деплой: staging → main → production](#повседневный-деплой)
- [Production deploy: как это работает](#production-deploy-как-это-работает)
- [Стратегии деплоя (smart deploy)](#стратегии-деплоя)
- [Мониторинг и проверка](#мониторинг-и-проверка)
- [Откат (rollback)](#откат)
- [Экстренный хотфикс](#экстренный-хотфикс)
- [Секреты и переменные окружения](#секреты-и-переменные-окружения)
- [GitHub Environments](#github-environments)
- [Устранение неполадок](#устранение-неполадок)

---
## NOTE

теперь staging data seeder one shot (пишу эту пометку чтобы протестить пайплайн в последний раз)

## Архитектура CI/CD

```
feature branch ──► staging (auto-deploy) ──► main (deploy с approval)
                       │                         │
                       ▼                         ▼
               Staging-сервер              Production-сервер
             $STAGING_DEPLOY_DIR          $PROD_DEPLOY_DIR
              порт 18000                    порт 8000
```

Пути к проекту на серверах не зашиты в код — они задаются через GitHub Secrets
(`STAGING_DEPLOY_DIR`, `PROD_DEPLOY_DIR`) и передаются в SSH-сессию при деплое.
Скрипты работают из той директории, в которую склонирован репозиторий.

Пайплайн состоит из трёх workflow:

| Workflow            | Триггер                       | Что делает                                                           |
| ------------------- | ----------------------------- | -------------------------------------------------------------------- |
| `ci-tests.yml`      | push в main/staging, любой PR | Lint (ruff, yamllint), валидация config.yaml, unit/integration тесты |
| `cd-staging.yml`    | push в staging                | CI → detect changes → SSH-деплой → smoke-тесты → Telegram            |
| `cd-production.yml` | push в main или ручной запуск | CI → manual approval → SSH-деплой → health check → Telegram          |
---

## Предварительная настройка

### 1. GitHub Secrets

Следующие секреты должны быть настроены в **Settings → Secrets and variables →
Actions**:

| Секрет               | Описание                                    | Пример                                        |
| -------------------- | ------------------------------------------- | --------------------------------------------- |
| `STAGING_HOST`       | IP/hostname staging-сервера                 | `10.2.2.XX`                                   |
| `STAGING_USER`       | SSH-пользователь на staging                 | `idauhalevich`                                |
| `STAGING_DEPLOY_DIR` | Абсолютный путь к репозиторию на staging    | `/home/idauhalevich/parsers-exporter-service` |
| `PROD_HOST`          | IP/hostname production-сервера              | `10.2.2.19`                                   |
| `PROD_USER`          | SSH-пользователь на production              | `deploy`                                      |
| `PROD_DEPLOY_DIR`    | Абсолютный путь к репозиторию на production | `/opt/aragog/parsers-exporter-service`        |
| `SSH_PRIVATE_KEY`    | Приватный SSH-ключ (ed25519)                | содержимое `~/.ssh/id_ed25519`                |
| `TELEGRAM_BOT_TOKEN` | Токен Telegram-бота для нотификаций         | `123456:ABC-DEF...`                           |
| `TELEGRAM_CHAT_ID`   | ID чата/группы для нотификаций              | `-100123456789`                               |

### 2. GitHub Environments

Создать три environment в **Settings → Environments**:

**staging** — без дополнительных ограничений.

**production** — включить:

- **Required reviewers**: минимум 2 reviewer (те, кто имеют право одобрять
  прод-деплой).

**production-hotfix** — для экстренных деплоев:

- **Required reviewers**: 1 reviewer.

### 3. Branch protection rules

Для ветки `main`:

- Require pull request before merging (source: `staging`).
- Require approvals: 1+.
- Require status checks to pass: `ci-tests`.

### 4. Production-сервер: начальная настройка

```bash
# Клонировать репозиторий в любую удобную директорию
# (этот путь потом указать в секрете PROD_DEPLOY_DIR)
mkdir -p /opt/aragog
cd /opt/aragog
git clone git@github.com:<org>/parsers-exporter-service.git
cd parsers-exporter-service
git checkout main

# Создать .env из примера и заполнить реальными значениями
cp .env.example .env
nano .env  # заполнить REDIS_*, PG_*, CLICKHOUSE_*, TELEGRAM_*, ARAGOG_SERVICE_PG

# Первый запуск
docker compose build --no-cache
docker compose up -d

# Проверить
curl http://localhost:8000/health
```

### 5. Staging-сервер: начальная настройка

```bash
# Клонировать репозиторий в любую удобную директорию
# (этот путь потом указать в секрете STAGING_DEPLOY_DIR)
git clone git@github.com:<org>/parsers-exporter-service.git
cd parsers-exporter-service
git checkout staging

# Или запустить bootstrap-скрипт из корня проекта
bash scripts/bootstrap_staging.sh
```

Bootstrap-скрипт автоматически определяет директорию проекта относительно
собственного расположения. Если нужна ручная настройка:

```bash
cp .env.example .env.staging
nano .env.staging  # заполнить staging-значениями

# Сгенерировать staging-конфиг
python3 scripts/generate_staging_config.py

# Первый запуск (поднимает Redis, PostgreSQL, ClickHouse + сервис)
docker compose -f docker-compose.staging.yml up -d --build

# Проверить
curl http://localhost:18000/health
```

---

## Повседневный деплой

### Шаг 1: Разработка и пуш в staging

```bash
# Разработка ведётся в feature-ветке
git checkout -b feature/my-change
# ... коммиты ...
git push origin feature/my-change

# Создать PR в staging, пройти review, мержить
# → cd-staging.yml запускается автоматически
```

После мержа в `staging` автоматически:

1. Проходят CI-тесты (lint + unit + integration).
2. Определяется стратегия деплоя (rebuild / recreate / reload / none).
3. Деплой на staging по SSH.
4. Smoke-тесты проверяют health, статус, Redis/PG/CH connectivity.
5. Уведомление в Telegram.

### Шаг 2: PR из staging в main

```bash
# Создать PR: staging → main
# На GitHub: Pull requests → New pull request → base: main, compare: staging
```

PR должен:

- Пройти CI-тесты.
- Получить необходимое количество approvals.

После мержа в `main` запускается `cd-production.yml`.

### Шаг 3: Production deploy

Workflow автоматически:

1. Проверяет CI.
2. Запрашивает **manual approval** через GitHub Environment `production` (2
   reviewer).
3. Выполняет деплой по SSH через `scripts/deploy.sh`.
4. Проверяет health (до 12 попыток по 10 секунд).
5. Отправляет результат в Telegram.

**Approval:** перейти в **Actions → Production Deploy → Review deployments →
Approve**.

---

## Production deploy: как это работает

Скрипт `scripts/deploy.sh` на production-сервере:

1. `git fetch origin main` — получает изменения.
2. `git diff --name-only HEAD origin/main` — определяет, какие файлы изменились.
3. `git pull origin main` — применяет изменения.
4. Проверяет health сервиса (`/health`). Если сервис не отвечает — полный
   rebuild.
5. В зависимости от типа изменённых файлов выбирает стратегию деплоя.

---

## Стратегии деплоя

Система автоматически определяет минимально необходимое воздействие:

| Стратегия          | Триггер (изменённые файлы)                             | Что происходит                                                       | Даунтайм              |
| ------------------ | ------------------------------------------------------ | -------------------------------------------------------------------- | --------------------- |
| **infrastructure** | `requirements.txt`, `Dockerfile`, `docker-compose.yml` | Graceful stop → `docker compose down` → `build --no-cache` → `up -d` | Есть (обычно 1-3 мин) |
| **python**         | Любые `.py` файлы                                      | `POST /recreate` — горячая перезагрузка через DynamicImporter        | Нет                   |
| **config**         | Только `config.yaml`                                   | `POST /config/reload` — подхватывает новые/удалённые экспортеры      | Нет                   |
| **none**           | Только тесты, документация и пр.                       | Ничего не происходит                                                 | Нет                   |

Если API-вызов для стратегий `python` или `config` не удался — автоматический
fallback на полный rebuild.

---

## Мониторинг и проверка

### API эндпоинты сервиса

```bash
# Production
SERVICE=http://localhost:8000

# Health check
curl -s $SERVICE/health | python3 -m json.tool

# Статус менеджера (кол-во сервисов, аптайм)
curl -s $SERVICE/status | python3 -m json.tool

# Список всех зарегистрированных экспортеров
curl -s $SERVICE/services | python3 -m json.tool

# Перезагрузка конфигурации (без рестарта)
curl -s -X POST $SERVICE/config/reload | python3 -m json.tool
```

### Docker-контейнеры

```bash
# Production
docker compose ps
docker compose logs -f --tail=100 exporter

# Staging (все сервисы включая Redis/PG/CH)
docker compose -f docker-compose.staging.yml ps
docker compose -f docker-compose.staging.yml logs -f exporter
```

### Smoke-тесты (staging)

```bash
# Ручной запуск smoke-тестов
bash scripts/staging_smoke_test.sh http://localhost:18000
```

Проверяет: health, статус менеджера, список сервисов, config reload, Redis,
PostgreSQL, ClickHouse, наличие схем.

### Telegram-уведомления

Все деплои (staging и production) отправляют статус в Telegram:

- Результат деплоя (success/failure).
- Стратегия деплоя.
- SHA коммита.
- Кто инициировал.

---

## Откат

### Быстрый откат на production

```bash
ssh $PROD_USER@$PROD_HOST
cd <PROD_DEPLOY_DIR>  # путь из секрета PROD_DEPLOY_DIR

# Посмотреть историю коммитов
git log --oneline -10

# Откатиться на предыдущий коммит
git checkout HEAD~1

# Перезапустить сервис
docker compose down
docker compose up -d --build

# Проверить
curl -s http://localhost:8000/health
```

### Откат через Git revert (рекомендуемый способ)

Создаёт новый коммит, сохраняя историю:

```bash
# Локально
git checkout main
git pull origin main

# Откатить последний мерж-коммит
git revert -m 1 HEAD
git push origin main

# cd-production.yml запустится автоматически
```

### Откат staging

```bash
ssh $STAGING_USER@$STAGING_HOST
cd <STAGING_DEPLOY_DIR>  # путь из секрета STAGING_DEPLOY_DIR

git checkout HEAD~1
docker compose -f docker-compose.staging.yml down
docker compose -f docker-compose.staging.yml up -d --build
```

---

## Экстренный хотфикс

Для случаев, когда нужен ускоренный деплой с упрощённым процессом approval.

### Запуск через GitHub UI

1. Перейти в **Actions → Production Deploy → Run workflow**.
2. Заполнить поля:
   - **emergency_bypass**: `true`
   - **bypass_reason**: описание проблемы (обязательно).
   - **hotfix_ticket**: номер тикета/issue.
3. Запустить. Workflow потребует **1 approval** (вместо 2) через environment
   `production-hotfix`.

### Что происходит при хотфиксе

- CI тесты всё равно проходят.
- Требуется 1 reviewer вместо 2.
- Автоматически создаётся GitHub Issue с чек-листом reconciliation:
  - Cherry-pick хотфикса обратно в `staging`.
  - Полный прогон тестов на staging.
  - Верификация данных в PG + ClickHouse.
  - Smoke-тесты на staging.
  - Post-mortem (если применимо).
- Отдельное уведомление в Telegram с пометкой HOTFIX.

### Хотфикс: пошагово

```bash
# 1. Фикс в отдельной ветке от main
git checkout main
git pull origin main
git checkout -b hotfix/critical-fix

# ... фикс ...
git commit -m "hotfix: описание"
git push origin hotfix/critical-fix

# 2. Создать PR в main, получить approval, мержить

# 3. Запустить workflow с emergency_bypass=true через Actions UI

# 4. После фикса — обязательно cherry-pick в staging
git checkout staging
git cherry-pick <commit-sha>
git push origin staging
```

---

## Секреты и переменные окружения

### Файл .env на production

```bash
# <PROD_DEPLOY_DIR>/.env

# Redis
REDIS_222_URI=redis://<redis-host>:6379/0
REDIS_223_URI=redis://<redis-host>:6379/1

# PostgreSQL
PG_222_URI=postgresql://<user>:<pass>@<pg-host>:5432/<db>
PG_223_URI=postgresql://<user>:<pass>@<pg-host>:5432/<db>

# ClickHouse
CLICKHOUSE_HOST=<clickhouse-host>
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=<password>
CLICKHOUSE_REVIEWS_DICT={"host":"...","port":8123,"username":"...","password":"..."}

# Telegram
TELEGRAM_BOT_TOKEN=<token>
TELEGRAM_CHAT_ID=<chat_id>

# Aragog Service
ARAGOG_SERVICE_PG=postgresql://<user>:<pass>@<host>:5432/aragog_service

# Logging
LOG_LEVEL=INFO
```

Файл `.env` **не коммитится** (включён в `.gitignore`). На каждом сервере —
свой.

---

## GitHub Environments

| Environment         | Назначение              | Reviewers    | Используется в                            |
| ------------------- | ----------------------- | ------------ | ----------------------------------------- |
| `staging`           | Staging-деплой          | Не требуется | `cd-staging.yml`                          |
| `production`        | Стандартный прод-деплой | 2            | `cd-production.yml` → `deploy-production` |
| `production-hotfix` | Экстренный деплой       | 1            | `cd-production.yml` → `deploy-hotfix`     |

---

## Устранение неполадок

### Сервис не отвечает после деплоя

```bash
ssh $PROD_USER@$PROD_HOST
cd <PROD_DEPLOY_DIR>  # путь из секрета PROD_DEPLOY_DIR

# Проверить логи
docker compose logs --tail=200 exporter

# Если контейнер упал — полный перезапуск
docker compose down
docker compose up -d --build

# Если проблема в коде — откат
git log --oneline -5
git checkout <рабочий-sha>
docker compose down
docker compose up -d --build
```

### CI тесты падают

```bash
# Запустить локально
pip install ruff yamllint pytest pytest-cov fakeredis
ruff check .
yamllint -d "{extends: relaxed, rules: {line-length: {max: 200}}}" config.yaml
python scripts/ci_validate.py
pytest tests/unit/ -v
```

### Smoke-тесты staging падают

Проверить, что все контейнеры staging запущены:

```bash
docker compose -f docker-compose.staging.yml ps
# Все контейнеры должны быть healthy

# Перезапустить проблемный сервис
docker compose -f docker-compose.staging.yml restart <service>

# Полный перезапуск
docker compose -f docker-compose.staging.yml down
docker compose -f docker-compose.staging.yml up -d --build
```

### Graceful recreate не работает

Если `POST /recreate` возвращает ошибку, deploy.sh автоматически делает fallback
на rebuild. Если нужно вручную:

```bash
docker compose down
docker compose up -d --build
```

### Staging seeder не генерирует данные

```bash
docker compose -f docker-compose.staging.yml logs data-seeder
docker compose -f docker-compose.staging.yml restart data-seeder
```
