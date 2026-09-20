package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// ClaimRun atomically moves a QUEUED run to RUNNING for workerID. It returns
// false when the run is not claimable: another worker claimed it first, it
// already finished, or it is a retry whose backoff has not elapsed. This
// conditional UPDATE is what guarantees a run executes on one worker at a time
// even if its dispatch message is delivered twice.
func (db *DB) ClaimRun(ctx context.Context, runID, workerID string) (Run, bool, error) {
	row := db.Pool.QueryRow(ctx, `
		UPDATE job_runs
		SET status = 'RUNNING', worker_id = $2, started_at = now(),
		    last_heartbeat_at = now(), finished_at = NULL, next_attempt_at = NULL
		WHERE id = $1 AND status = 'QUEUED'
		  AND (next_attempt_at IS NULL OR next_attempt_at <= now())
		RETURNING `+runColumns, runID, workerID)
	r, err := scanRun(row)
	if errors.Is(err, ErrNotFound) {
		return Run{}, false, nil
	}
	if err != nil {
		return Run{}, false, err
	}
	return r, true, nil
}

// Heartbeat records that workerID is still executing this attempt. It returns
// false once the worker no longer owns the attempt, for example because the
// reaper requeued it after missed heartbeats; the worker must then stop.
func (db *DB) Heartbeat(ctx context.Context, runID, workerID string, attempt int) (bool, error) {
	tag, err := db.Pool.Exec(ctx, `
		UPDATE job_runs SET last_heartbeat_at = now()
		WHERE id = $1 AND worker_id = $2 AND attempt = $3 AND status = 'RUNNING'`,
		runID, workerID, attempt)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// FinishRun records how an attempt ended. Like Heartbeat it only applies while
// workerID still owns the attempt, so a worker the reaper gave up on cannot
// overwrite the attempt that replaced it.
func (db *DB) FinishRun(ctx context.Context, runID, workerID string, attempt int, status string, output, errMsg *string) (bool, error) {
	tag, err := db.Pool.Exec(ctx, `
		UPDATE job_runs SET status = $4, output = $5, error = $6, finished_at = now()
		WHERE id = $1 AND worker_id = $2 AND attempt = $3 AND status = 'RUNNING'`,
		runID, workerID, attempt, status, output, errMsg)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// ReleaseRun hands an unfinished attempt back to the queue, for a worker
// shutting down mid-job. The next attempt starts over, with its own log.
func (db *DB) ReleaseRun(ctx context.Context, runID, workerID string, attempt int, reason string) (bool, error) {
	tag, err := db.Pool.Exec(ctx, `
		UPDATE job_runs
		SET status = 'QUEUED', attempt = attempt + 1, worker_id = NULL,
		    started_at = NULL, last_heartbeat_at = NULL, queued_at = now(), error = $4
		WHERE id = $1 AND worker_id = $2 AND attempt = $3 AND status = 'RUNNING'`,
		runID, workerID, attempt, reason)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// AppendLogs stores a batch of log lines for one attempt in a single round
// trip. Re-sending lines is harmless: (run_id, attempt, seq) is the primary
// key and duplicates are ignored.
func (db *DB) AppendLogs(ctx context.Context, runID string, attempt int, lines []LogLine) error {
	if len(lines) == 0 {
		return nil
	}
	seqs := make([]int64, len(lines))
	times := make([]time.Time, len(lines))
	levels := make([]string, len(lines))
	texts := make([]string, len(lines))
	for i, l := range lines {
		seqs[i], times[i], levels[i], texts[i] = l.Seq, l.TS, l.Level, l.Line
	}
	_, err := db.Pool.Exec(ctx, `
		INSERT INTO job_logs (run_id, attempt, seq, ts, level, line)
		SELECT $1, $2, u.seq, u.ts, u.level, u.line
		FROM unnest($3::bigint[], $4::timestamptz[], $5::text[], $6::text[])
		     AS u(seq, ts, level, line)
		ON CONFLICT DO NOTHING`,
		runID, attempt, seqs, times, levels, texts)
	return err
}

// ReapStale finds runs whose worker stopped heartbeating and requeues them for
// another attempt or, once max_retries is used up, fails them. It returns what
// it changed so the caller can dispatch the requeued runs. SKIP LOCKED lets
// every worker run the reaper concurrently without handling a run twice.
func (db *DB) ReapStale(ctx context.Context, staleAfter time.Duration, limit int) ([]Redispatch, error) {
	return collectRedispatches(db.Pool.Query(ctx, `
		WITH stale AS (
			SELECT r.id, j.type, r.attempt > j.max_retries AS exhausted
			FROM job_runs r JOIN jobs j ON j.id = r.job_id
			WHERE r.status = 'RUNNING'
			  AND r.last_heartbeat_at < now() - make_interval(secs => $1)
			ORDER BY r.last_heartbeat_at
			LIMIT $2
			FOR UPDATE OF r SKIP LOCKED
		)
		UPDATE job_runs r SET
			status            = CASE WHEN s.exhausted THEN 'FAILED' ELSE 'QUEUED' END,
			attempt           = CASE WHEN s.exhausted THEN r.attempt ELSE r.attempt + 1 END,
			error             = format('attempt %s lost worker %s', r.attempt, r.worker_id),
			finished_at       = CASE WHEN s.exhausted THEN now() END,
			queued_at         = CASE WHEN s.exhausted THEN r.queued_at ELSE now() END,
			worker_id         = CASE WHEN s.exhausted THEN r.worker_id END,
			started_at        = CASE WHEN s.exhausted THEN r.started_at END,
			last_heartbeat_at = CASE WHEN s.exhausted THEN r.last_heartbeat_at END
		FROM stale s
		WHERE r.id = s.id
		RETURNING r.id::text, r.job_id::text, s.type, r.status, r.attempt`,
		staleAfter.Seconds(), limit))
}

// SweepQueued returns runs that have sat QUEUED longer than olderThan, which
// means their dispatch message was lost: the API crashed between inserting the
// run and publishing it, or a worker hit a database error while claiming it.
// It refreshes queued_at so each run is re-dispatched at most once per
// olderThan. A duplicate message is harmless because claiming is atomic.
func (db *DB) SweepQueued(ctx context.Context, olderThan time.Duration, limit int) ([]Redispatch, error) {
	return collectRedispatches(db.Pool.Query(ctx, `
		WITH due AS (
			SELECT r.id, j.type
			FROM job_runs r JOIN jobs j ON j.id = r.job_id
			WHERE r.status = 'QUEUED'
			  AND r.queued_at < now() - make_interval(secs => $1)
			  AND (r.next_attempt_at IS NULL OR r.next_attempt_at <= now())
			ORDER BY r.queued_at
			LIMIT $2
			FOR UPDATE OF r SKIP LOCKED
		)
		UPDATE job_runs r SET queued_at = now()
		FROM due d
		WHERE r.id = d.id
		RETURNING r.id::text, r.job_id::text, d.type, r.status, r.attempt`,
		olderThan.Seconds(), limit))
}

func collectRedispatches(rows pgx.Rows, err error) ([]Redispatch, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Redispatch
	for rows.Next() {
		var r Redispatch
		if err := rows.Scan(&r.RunID, &r.JobID, &r.JobType, &r.Status, &r.Attempt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
