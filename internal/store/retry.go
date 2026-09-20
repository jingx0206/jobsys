package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// FailOrRetry records a failed attempt. If the job has retries left, the run
// is requeued as the next attempt, not due until backoff has passed;
// otherwise it fails. Like FinishRun it only applies while workerID owns the
// attempt. It returns the run's new status, or "" if the worker no longer
// owned the attempt.
func (db *DB) FailOrRetry(ctx context.Context, runID, workerID string, attempt int, errMsg string, backoff time.Duration) (string, error) {
	var status string
	err := db.Pool.QueryRow(ctx, `
		WITH cur AS (
			SELECT r.id, r.attempt > j.max_retries AS exhausted
			FROM job_runs r JOIN jobs j ON j.id = r.job_id
			WHERE r.id = $1 AND r.worker_id = $2 AND r.attempt = $3 AND r.status = 'RUNNING'
			FOR UPDATE OF r
		)
		UPDATE job_runs r SET
			status            = CASE WHEN c.exhausted THEN 'FAILED' ELSE 'QUEUED' END,
			attempt           = CASE WHEN c.exhausted THEN r.attempt ELSE r.attempt + 1 END,
			error             = $4,
			finished_at       = CASE WHEN c.exhausted THEN now() END,
			next_attempt_at   = CASE WHEN c.exhausted THEN NULL ELSE now() + make_interval(secs => $5) END,
			queued_at         = CASE WHEN c.exhausted THEN r.queued_at ELSE now() END,
			worker_id         = CASE WHEN c.exhausted THEN r.worker_id END,
			started_at        = CASE WHEN c.exhausted THEN r.started_at END,
			last_heartbeat_at = CASE WHEN c.exhausted THEN r.last_heartbeat_at END
		FROM cur c
		WHERE r.id = c.id
		RETURNING r.status`,
		runID, workerID, attempt, errMsg, backoff.Seconds()).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return status, err
}

// DueRetries returns queued retries whose backoff has elapsed and clears their
// next_attempt_at, so each is dispatched once. SKIP LOCKED lets every worker
// run this concurrently without dispatching the same retry twice. If the
// dispatch is then lost, the sweeper finds the run still QUEUED.
func (db *DB) DueRetries(ctx context.Context, limit int) ([]Redispatch, error) {
	return collectRedispatches(db.Pool.Query(ctx, `
		WITH due AS (
			SELECT r.id, j.type
			FROM job_runs r JOIN jobs j ON j.id = r.job_id
			WHERE r.status = 'QUEUED' AND r.next_attempt_at <= now()
			ORDER BY r.next_attempt_at
			LIMIT $1
			FOR UPDATE OF r SKIP LOCKED
		)
		UPDATE job_runs r SET next_attempt_at = NULL, queued_at = now()
		FROM due d
		WHERE r.id = d.id
		RETURNING r.id::text, r.job_id::text, d.type, r.status, r.attempt`, limit))
}
