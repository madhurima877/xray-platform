-- PostgreSQL is the source of truth. xray-api only reads Redis; every service
-- here writes PostgreSQL first and then refreshes Redis.

-- Customers: copied from the relay by master-sync (id, name, callback,
-- active, API keys). Level and policies are owned by X-Ray (admin-api, S-04).
CREATE TABLE customers (
    id             text PRIMARY KEY,
    name           text NOT NULL DEFAULT '',
    callback_url   text NOT NULL DEFAULT '',
    active         boolean NOT NULL DEFAULT true,
    level          smallint NOT NULL DEFAULT 1 CHECK (level BETWEEN 1 AND 5),
    rate_limit     integer NOT NULL DEFAULT 0 CHECK (rate_limit >= 0),
    keyword_action text NOT NULL DEFAULT 'HOLD' CHECK (keyword_action IN ('HOLD', 'BLOCK')),
    ai_block       real NOT NULL DEFAULT 0.9 CHECK (ai_block BETWEEN 0 AND 1),
    ai_hold        real NOT NULL DEFAULT 0.7 CHECK (ai_hold BETWEEN 0 AND 1),
    synced_at      timestamptz,
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE api_keys (
    key_hash    text PRIMARY KEY, -- sha256 hex, same as xray:apikey:{hash}
    customer_id text NOT NULL REFERENCES customers (id) ON DELETE CASCADE
);
CREATE INDEX api_keys_customer ON api_keys (customer_id);

-- Black and White List entries. customer_id '' is the global list.
CREATE TABLE list_entries (
    id          bigserial PRIMARY KEY,
    list        text NOT NULL CHECK (list IN ('black', 'white')),
    kind        text NOT NULL CHECK (kind IN ('url_host', 'sender')),
    value       text NOT NULL,
    customer_id text NOT NULL DEFAULT '',
    source      text NOT NULL CHECK (source IN ('admin', 'kisa', 'wl-collector')),
    note        text NOT NULL DEFAULT '',
    created_by  text NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (list, customer_id, value)
);
CREATE INDEX list_entries_source ON list_entries (source, list);

CREATE TABLE keywords (
    id         bigserial PRIMARY KEY,
    phrase     text NOT NULL UNIQUE,
    active     boolean NOT NULL DEFAULT true,
    created_by text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- One row. admin-api bumps version on every keyword change; keyword-sync
-- pushes to the relay when pushed_version is behind.
CREATE TABLE keyword_state (
    id             smallint PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    version        bigint NOT NULL DEFAULT 1,
    pushed_version bigint NOT NULL DEFAULT 0,
    pushed_at      timestamptz
);
INSERT INTO keyword_state (id) VALUES (1);

-- Consumers that must not count a redelivered event twice record its key
-- here in the same transaction as their write.
CREATE TABLE processed_events (
    consumer     text NOT NULL,
    event_key    text NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_key)
);
CREATE INDEX processed_events_at ON processed_events (processed_at);

-- stats-worker: per customer, minute and verdict (S-01 dashboard, S-05).
CREATE TABLE stats_minute (
    customer_id    text NOT NULL,
    bucket         timestamptz NOT NULL,
    verdict        text NOT NULL,
    count          bigint NOT NULL,
    latency_sum_ms double precision NOT NULL DEFAULT 0,
    latency_max_ms double precision NOT NULL DEFAULT 0,
    PRIMARY KEY (customer_id, bucket, verdict)
);
CREATE INDEX stats_minute_bucket ON stats_minute (bucket);

-- stats-worker: which step decided, per customer and day (S-05 detection).
CREATE TABLE stats_reason_daily (
    customer_id text NOT NULL,
    day         date NOT NULL,
    step        text NOT NULL,
    action      text NOT NULL,
    count       bigint NOT NULL,
    PRIMARY KEY (customer_id, day, step, action)
);

-- audit-worker: every verdict (S-12), one partition per day, created on
-- demand and dropped after the retention period.
CREATE TABLE verdict_history (
    customer_id   text NOT NULL,
    message_id    text NOT NULL,
    decided_at    timestamptz NOT NULL,
    verdict       text NOT NULL,
    level         smallint NOT NULL,
    sender        text NOT NULL DEFAULT '',
    urls          text[] NOT NULL DEFAULT '{}',
    reasons       jsonb NOT NULL DEFAULT '[]',
    ai_score      real,
    model_version text NOT NULL DEFAULT '',
    latency_ms    real NOT NULL DEFAULT 0,
    PRIMARY KEY (customer_id, message_id, decided_at)
) PARTITION BY RANGE (decided_at);
CREATE INDEX verdict_history_time ON verdict_history (decided_at DESC);

-- wl-collector: senders and hosts seen only in ALLOW verdicts become White
-- List candidates an operator approves on S-03.
CREATE TABLE wl_candidates (
    customer_id text NOT NULL,
    kind        text NOT NULL CHECK (kind IN ('url_host', 'sender')),
    value       text NOT NULL,
    allow_count bigint NOT NULL DEFAULT 0,
    bad_count   bigint NOT NULL DEFAULT 0,
    status      text NOT NULL DEFAULT 'watching'
                CHECK (status IN ('watching', 'pending', 'approved', 'rejected')),
    first_seen  timestamptz NOT NULL DEFAULT now(),
    last_seen   timestamptz NOT NULL DEFAULT now(),
    decided_by  text NOT NULL DEFAULT '',
    PRIMARY KEY (customer_id, kind, value)
);
CREATE INDEX wl_candidates_status ON wl_candidates (status, allow_count DESC);

-- kisa-reporter outbox (S-11): a BLOCK verdict is stored before the Kafka
-- offset is committed, then sent with retries.
CREATE TABLE kisa_outbox (
    customer_id     text NOT NULL,
    message_id      text NOT NULL,
    payload         jsonb NOT NULL,
    status          text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'failed')),
    attempts        integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error      text NOT NULL DEFAULT '',
    created_at      timestamptz NOT NULL DEFAULT now(),
    sent_at         timestamptz,
    PRIMARY KEY (customer_id, message_id)
);
CREATE INDEX kisa_outbox_due ON kisa_outbox (next_attempt_at) WHERE status = 'pending';

-- One row per scheduled job run (S-14 sync screen).
CREATE TABLE sync_runs (
    id          bigserial PRIMARY KEY,
    job         text NOT NULL,
    started_at  timestamptz NOT NULL,
    finished_at timestamptz NOT NULL DEFAULT now(),
    ok          boolean NOT NULL,
    items       integer NOT NULL DEFAULT 0,
    error       text NOT NULL DEFAULT ''
);
CREATE INDEX sync_runs_job ON sync_runs (job, started_at DESC);

-- Operators of the admin web (S-06, S-07).
CREATE TABLE admin_users (
    id            bigserial PRIMARY KEY,
    username      text NOT NULL UNIQUE,
    password_hash text NOT NULL,
    role          text NOT NULL CHECK (role IN ('admin', 'operator', 'viewer')),
    active        boolean NOT NULL DEFAULT true,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- Every change made through admin-api.
CREATE TABLE audit_log (
    id        bigserial PRIMARY KEY,
    at        timestamptz NOT NULL DEFAULT now(),
    user_id   bigint,
    username  text NOT NULL,
    action    text NOT NULL,
    entity    text NOT NULL,
    entity_id text NOT NULL DEFAULT '',
    detail    jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX audit_log_at ON audit_log (at DESC);
