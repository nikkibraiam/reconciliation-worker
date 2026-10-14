-- Our side: transfers we asked a provider to execute.
CREATE TABLE IF NOT EXISTS transfers (
    id           bigserial   PRIMARY KEY,
    provider     text        NOT NULL,
    reference    text        NOT NULL UNIQUE,
    amount_minor bigint      NOT NULL CHECK (amount_minor > 0),
    currency     char(3)     NOT NULL,
    status       text        NOT NULL DEFAULT 'pending',
    provider_id  text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- Audit trail of every status change made by the reconciler.
CREATE TABLE IF NOT EXISTS transfer_events (
    id          bigserial   PRIMARY KEY,
    transfer_id bigint      NOT NULL REFERENCES transfers (id),
    from_status text        NOT NULL,
    to_status   text        NOT NULL,
    job_id      bigint      NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- Provider side, ingested idempotently: re-fetching a page upserts, never duplicates.
CREATE TABLE IF NOT EXISTS provider_records (
    provider     text        NOT NULL,
    provider_id  text        NOT NULL,
    reference    text,
    amount_minor bigint      NOT NULL,
    currency     char(3)     NOT NULL,
    status       text        NOT NULL,
    settled_at   timestamptz NOT NULL,
    fetched_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (provider, provider_id)
);
CREATE INDEX IF NOT EXISTS provider_records_reference ON provider_records (provider, reference);
CREATE INDEX IF NOT EXISTS provider_records_settled_at ON provider_records (provider, settled_at);

-- Work queue. Workers lease jobs with FOR UPDATE SKIP LOCKED; no session
-- state or advisory locks, so it works behind PgBouncer in transaction mode.
CREATE TABLE IF NOT EXISTS reconciliation_jobs (
    id           bigserial   PRIMARY KEY,
    provider     text        NOT NULL,
    window_start timestamptz NOT NULL,
    window_end   timestamptz NOT NULL,
    status       text        NOT NULL DEFAULT 'queued',  -- queued | running | done | failed
    attempts     integer     NOT NULL DEFAULT 0,
    locked_until timestamptz,
    last_error   text,
    stats        jsonb,
    created_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz,
    UNIQUE (provider, window_start, window_end)
);

-- Open problems for a human. One row per (kind, subject) no matter how many
-- runs see it; closed automatically when a later run no longer finds it.
CREATE TABLE IF NOT EXISTS discrepancies (
    id            bigserial   PRIMARY KEY,
    provider      text        NOT NULL,
    fingerprint   text        NOT NULL,
    kind          text        NOT NULL,
    subject       text        NOT NULL,
    transfer_id   bigint      REFERENCES transfers (id),
    provider_id   text,
    details       jsonb       NOT NULL DEFAULT '{}',
    status        text        NOT NULL DEFAULT 'open',  -- open | resolved
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at  timestamptz NOT NULL DEFAULT now(),
    last_seen_job bigint      NOT NULL,
    resolved_at   timestamptz,
    UNIQUE (provider, fingerprint)
);
