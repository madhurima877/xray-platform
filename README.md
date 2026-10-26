# X-Ray platform services (Go)

The services around the X-Ray inspection API. `xray-api`, which runs the 12-step check, lives in `../x-ray`. This repo has everything after it and around it: 8 Go services, all in one module.

```
                    ┌──────────── A. real time (../x-ray) ────────────┐
 relay ── POST /inspect ──► xray-api ──► Redis  (reads only)          │
   ▲          callback ◄──┘    │                                      │
   │                           └─ step 12: event ─► Kafka xray.verdicts
   │                                                  │
   │        B. consumers (one group each)             ├─► stats-worker   → stats_minute, stats_reason_daily (S-01, S-05)
   │                                                  ├─► audit-worker   → verdict_history, daily partitions (S-12)
   │                                                  ├─► wl-collector   → wl_candidates (S-03)
   │                                                  └─► kisa-reporter  → kisa_outbox → KISA (S-11)
   │
   │        C. jobs (ticker + "sync now")
   ├──────── master-sync    relay customers + API keys → customers, api_keys → Redis
   └──────── keyword-sync   keywords → relay (on change + daily)          → Redis
             kisa-collector KISA bad-URL list → list_entries (black)      → Redis

            D. admin web ── REST ──► admin-api ──► PostgreSQL ──► Redis
```

Rule: **PostgreSQL first, then Redis.** xray-api only reads Redis. Every service here writes PostgreSQL, then refreshes the Redis keys (`xray:customer:*`, `xray:apikey:*`, `xray:bl*`, `xray:wl*`, `xray:keywords`, same layout as `x-ray/internal/store`).

## Layout

```
cmd/admin-api        REST API for the admin web (:8090)
cmd/stats-worker     Kafka group xray-stats   (ops :9101)
cmd/audit-worker     Kafka group xray-audit   (ops :9102)
cmd/wl-collector     Kafka group xray-wl      (ops :9103)
cmd/kisa-reporter    Kafka group xray-kisa    (ops :9104)
cmd/kisa-collector   job, every 1 h           (ops :9105)
cmd/master-sync      job, every 5 min         (ops :9106)
cmd/keyword-sync     job, every 30 s          (ops :9107)
cmd/mockext          local stand-in for the relay master/keyword APIs and KISA

internal/app         JSON logs, SIGTERM context, /healthz /readyz /metrics, counters
internal/config      env parsing (every bad value reported at once)
internal/boot        Postgres + migrations + Redis for a binary
internal/db          pool, embedded migrations (advisory lock), dedupe helper, sync_runs
internal/model       the result event contract (mirror of x-ray model.Event)
internal/consumer    Kafka batch consumer: commit only after the batch is stored
internal/cache       PostgreSQL → Redis rebuild (temp key + RENAME)
internal/jobs        scheduled job loop + Redis pub/sub "sync now"
internal/hosts       URL → list key (lower-case host, no www.)
internal/stats, audit, wl, kisa, mastersync, keywordsync, admin
```

## How each service is safe to restart

| Service | Delivery | Why a redelivered event does no harm |
|---|---|---|
| stats-worker | at least once | event key stored in `processed_events` in the same transaction as the counts |
| audit-worker | at least once | primary key `(customer_id, message_id, decided_at)`, `ON CONFLICT DO NOTHING` |
| wl-collector | at least once | same dedupe table as stats |
| kisa-reporter | at least once | outbox primary key; sends use a lease (`FOR UPDATE SKIP LOCKED`) so copies can run side by side. KISA should de-duplicate by `report_id` |
| jobs | — | each run replaces state; an empty KISA list or relay customer list is refused instead of wiping data |

Every consumer commits Kafka offsets only after PostgreSQL has the batch. If the database is down, the batch is retried with backoff (up to 30 s) and nothing is committed. A message that is not valid JSON is logged, counted (`events_bad`) and skipped. On SIGTERM a consumer stores the batch it is holding, commits, and exits.

## admin-api

Log in with `POST /api/login` and send `Authorization: Bearer <token>` on every other call. There are three roles, each with the rights of the one below: `viewer` reads everything, `operator` also edits, `admin` also manages users and reads the audit log. The user is reloaded on each request, so deactivating someone or changing their role takes effect at once. After 10 failed logins, a username is locked for 15 minutes.

| Screen | Endpoints |
|---|---|
| S-01 Dashboard | `GET /api/stats/summary`, `GET /api/stats/series?interval=minute\|hour\|day` |
| S-02 Black List | `GET/POST /api/lists/black`, `DELETE /api/lists/black/{id}` |
| S-03 White List | `GET/POST /api/lists/white`, `DELETE /api/lists/white/{id}`, `GET /api/wl-candidates`, `POST /api/wl-candidates/approve\|reject` |
| S-04 Inspection level | `PATCH /api/customers/{id}` (level, rate_limit, keyword_action, ai_block, ai_hold) |
| S-05 Statistics | `GET /api/stats/series`, `GET /api/stats/reasons` |
| S-06/07 Users, permissions | `GET/POST /api/users`, `PATCH /api/users/{id}` (admin) |
| S-08 Master data | `GET /api/customers`, `GET /api/customers/{id}` |
| S-11 KISA | `GET /api/kisa/reports`, `POST /api/kisa/reports/{customer}/{message}/retry` |
| S-12 History | `GET /api/history?customer_id&verdict&message_id&sender&from&to&before` |
| S-14 Sync | `GET /api/sync/status`, `GET /api/sync/runs`, `POST /api/sync/{kisa-collector\|master-sync\|keyword-sync}` |
| Keywords | `GET/POST /api/keywords`, `PATCH/DELETE /api/keywords/{id}` |
| Audit | `GET /api/audit` (admin) |

Each edit writes a row to `audit_log` in the same transaction and then updates Redis. If the Redis update fails, the response says `"redis_refreshed": false` and the next scheduled sync copies the change over.

## Run

```bash
docker-compose up --build
```

If port 8080 is taken, run on another port:

```bash
XRAY_PORT=8088 docker-compose up --build
```

```bash
XRAY=http://localhost:8088 ./scripts/demo.sh
```

The demo script logs in as the bootstrap admin, edits a customer and a keyword, sends messages through xray-api, and then reads back stats, history, White List candidates, KISA reports, sync status and the audit log.

Tests:

```bash
go test -race ./...
```

## Configuration (env)

Common: `DATABASE_URL` (required), `DB_MAX_CONNS` (10), `REDIS_ADDR` (`localhost:6379`), `REDIS_PASSWORD`, `REDIS_DB`, `OPS_ADDR`.

Consumers: `KAFKA_BROKERS` (required), `KAFKA_TOPIC` (`xray.verdicts`), `KAFKA_GROUP`, `BATCH_SIZE` (500), `FLUSH_EVERY` (1s), `DEDUPE_KEEP` (72h).

| Service | Own settings |
|---|---|
| audit-worker | `HISTORY_RETENTION` (2160h = 90 days) |
| wl-collector | `WL_MIN_ALLOW` (100 ALLOWs with no HOLD/BLOCK → candidate) |
| kisa-reporter | `KISA_REPORT_URL` (required), `KISA_API_KEY`, `KISA_TIMEOUT` (5s), `KISA_SEND_EVERY` (2s), `KISA_SEND_BATCH` (50), `KISA_MAX_ATTEMPTS` (20), `KISA_LEASE` (2m) |
| kisa-collector | `KISA_LIST_URL` (required), `KISA_API_KEY`, `EVERY` (1h), `RUN_TIMEOUT` (5m) |
| master-sync | `RELAY_CUSTOMERS_URL` (required), `RELAY_API_TOKEN`, `EVERY` (5m), `RUN_TIMEOUT` (1m) |
| keyword-sync | `RELAY_KEYWORDS_URL` (required), `RELAY_API_TOKEN`, `EVERY` (30s), `FULL_EVERY` (24h) |
| admin-api | `ADMIN_ADDR` (`:8090`), `JWT_SECRET` (required, ≥ 32 chars), `TOKEN_TTL` (8h), `CORS_ORIGIN`, `ADMIN_BOOTSTRAP_USER` / `ADMIN_BOOTSTRAP_PASSWORD` (creates the first admin when there are no users) |

## Still assumed

- **KISA report and list formats.** `kisa.Report` and `kisa.ParseList`, which accepts plain text, a JSON string array or `[{"url"}]`, are placeholders until the KISA spec arrives.
- **Relay master-data and keyword APIs.** `GET {customers:[{id,name,callback_url,active,api_keys}]}` and `PUT {version, keywords}` are our guess at the contract.
- **Sender and URLs in the result event.** xray-api's Kafka event now carries `sender` and `urls` (`x-ray/internal/model.Event`). wl-collector and kisa-reporter need them, while the relay callback body is unchanged.
