-- 002_queued_sweep_index.sql: supports the worker's sweep for runs stuck in
-- QUEUED because their dispatch message was lost.
CREATE INDEX IF NOT EXISTS job_runs_queued_idx
    ON job_runs (queued_at)
    WHERE status = 'QUEUED';
