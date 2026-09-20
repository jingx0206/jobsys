-- Checks for migrations/001_init.sql. Run against a scratch database after
-- applying the migration. Each check prints NOTICE 'PASS ...'; any failure
-- aborts with an error.
--
--   createdb -h localhost -U postgres jobsys_check
--   psql -h localhost -U postgres -d jobsys_check -v ON_ERROR_STOP=1 \
--        -f migrations/001_init.sql -f migrations/checks/001_init_check.sql
--   dropdb -h localhost -U postgres jobsys_check

DO $$
DECLARE
    j uuid := gen_random_uuid();
    r uuid := gen_random_uuid();
    n int;
BEGIN
    INSERT INTO jobs (id, name, type, payload, cron_expr, next_run_at)
    VALUES (j, 'gen-report', 'generate_text', '{"lines": 5}', '0 */6 * * *', now());

    -- 1. First fire of a cron occurrence creates the run.
    INSERT INTO job_runs (id, job_id, scheduled_time, trigger)
    VALUES (r, j, '2026-09-17 06:00+00', 'schedule');

    -- 2. A racing scheduler firing the same occurrence is a no-op.
    INSERT INTO job_runs (id, job_id, scheduled_time, trigger)
    VALUES (gen_random_uuid(), j, '2026-09-17 06:00+00', 'schedule')
    ON CONFLICT ON CONSTRAINT job_runs_occurrence_uniq DO NOTHING;
    GET DIAGNOSTICS n = ROW_COUNT;
    ASSERT n = 0, 'duplicate occurrence should be ignored';
    RAISE NOTICE 'PASS 1-2  duplicate cron occurrence ignored';

    -- 3. Atomic claim: the first worker wins, the second updates nothing.
    UPDATE job_runs
    SET status = 'RUNNING', worker_id = 'w1', started_at = now(), last_heartbeat_at = now()
    WHERE id = r AND status = 'QUEUED';
    GET DIAGNOSTICS n = ROW_COUNT;
    ASSERT n = 1, 'first claim should succeed';
    UPDATE job_runs SET status = 'RUNNING', worker_id = 'w2'
    WHERE id = r AND status = 'QUEUED';
    GET DIAGNOSTICS n = ROW_COUNT;
    ASSERT n = 0, 'second claim should lose';
    RAISE NOTICE 'PASS 3    second worker loses the claim';

    -- 4. Unknown statuses are rejected.
    BEGIN
        UPDATE job_runs SET status = 'DONE' WHERE id = r;
        RAISE EXCEPTION 'invalid status accepted';
    EXCEPTION WHEN check_violation THEN NULL;
    END;
    RAISE NOTICE 'PASS 4    invalid status rejected';

    -- 5. Log seq may repeat across attempts but not within one.
    INSERT INTO job_logs (run_id, attempt, seq, line)
    VALUES (r, 1, 1, 'line a'), (r, 1, 2, 'line b'), (r, 2, 1, 'retry line a');
    BEGIN
        INSERT INTO job_logs (run_id, attempt, seq, line) VALUES (r, 1, 1, 'dup');
        RAISE EXCEPTION 'duplicate log seq accepted';
    EXCEPTION WHEN unique_violation THEN NULL;
    END;
    RAISE NOTICE 'PASS 5    log seq unique per attempt';

    -- 6. Deleting a job cascades to its runs and their logs.
    DELETE FROM jobs WHERE id = j;
    SELECT count(*) INTO n FROM job_runs WHERE job_id = j;
    ASSERT n = 0, 'runs should cascade';
    SELECT count(*) INTO n FROM job_logs WHERE run_id = r;
    ASSERT n = 0, 'logs should cascade';
    RAISE NOTICE 'PASS 6    delete cascades to runs and logs';
END $$;

-- The hot queries can use their partial indexes.
SET enable_seqscan = off;
EXPLAIN (COSTS OFF)
SELECT id FROM jobs
WHERE enabled AND next_run_at <= now()
ORDER BY next_run_at LIMIT 100
FOR UPDATE SKIP LOCKED;

EXPLAIN (COSTS OFF)
SELECT id FROM job_runs
WHERE status = 'RUNNING' AND last_heartbeat_at < now() - interval '60 seconds';
