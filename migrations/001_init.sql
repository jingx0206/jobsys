-- 001_init.sql: jobs, job_runs, job_logs.
--
-- Postgres is the source of truth for all state. Kafka only carries
-- notifications that a run is ready; workers always re-read the run here
-- and claim it with a conditional UPDATE.

-- A job definition: what to run and when.
CREATE TABLE IF NOT EXISTS jobs (
    id           uuid        PRIMARY KEY,
    name         text        NOT NULL,
    type         text        NOT NULL,
    payload      jsonb       NOT NULL DEFAULT '{}',
    -- NULL cron_expr means the job only runs when triggered manually.
    cron_expr    text,
    timezone     text        NOT NULL DEFAULT 'UTC',
    next_run_at  timestamptz,
    enabled      boolean     NOT NULL DEFAULT true,
    max_retries  integer     NOT NULL DEFAULT 3  CHECK (max_retries >= 0),
    timeout_sec  integer     NOT NULL DEFAULT 300 CHECK (timeout_sec > 0),
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- The scheduler's hot query: enabled jobs whose next_run_at has passed.
CREATE INDEX IF NOT EXISTS jobs_due_idx
    ON jobs (next_run_at)
    WHERE enabled AND next_run_at IS NOT NULL;

-- One execution of a job. A retry re-queues the same row with attempt + 1,
-- so a scheduled occurrence never produces a second row.
CREATE TABLE IF NOT EXISTS job_runs (
    id                uuid        PRIMARY KEY,
    job_id            uuid        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    -- The cron occurrence this run fulfils; now() for manual triggers.
    scheduled_time    timestamptz NOT NULL,
    trigger           text        NOT NULL CHECK (trigger IN ('schedule', 'manual')),
    status            text        NOT NULL DEFAULT 'QUEUED'
                      CHECK (status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')),
    attempt           integer     NOT NULL DEFAULT 1 CHECK (attempt >= 1),
    worker_id         text,
    queued_at         timestamptz NOT NULL DEFAULT now(),
    started_at        timestamptz,
    finished_at       timestamptz,
    last_heartbeat_at timestamptz,
    -- Set when a failed run is waiting out its retry backoff.
    next_attempt_at   timestamptz,
    error             text,
    output            text,

    -- Makes cron firing idempotent: if two schedulers race to fire the same
    -- occurrence, the second INSERT ... ON CONFLICT DO NOTHING is a no-op.
    CONSTRAINT job_runs_occurrence_uniq UNIQUE (job_id, scheduled_time)
);

-- The reaper's query: running runs whose heartbeat has gone stale.
CREATE INDEX IF NOT EXISTS job_runs_running_heartbeat_idx
    ON job_runs (last_heartbeat_at)
    WHERE status = 'RUNNING';

-- Queued runs waiting for their retry backoff to elapse.
CREATE INDEX IF NOT EXISTS job_runs_retry_due_idx
    ON job_runs (next_attempt_at)
    WHERE status = 'QUEUED' AND next_attempt_at IS NOT NULL;

-- Log lines for a run. seq is assigned by the worker and restarts for each
-- attempt, so a retry on another worker cannot collide with earlier lines.
-- The primary key doubles as the from_seq paging index.
CREATE TABLE IF NOT EXISTS job_logs (
    run_id   uuid        NOT NULL REFERENCES job_runs (id) ON DELETE CASCADE,
    attempt  integer     NOT NULL,
    seq      bigint      NOT NULL,
    ts       timestamptz NOT NULL DEFAULT now(),
    level    text        NOT NULL DEFAULT 'info' CHECK (level IN ('debug', 'info', 'warn', 'error')),
    line     text        NOT NULL,
    PRIMARY KEY (run_id, attempt, seq)
);
