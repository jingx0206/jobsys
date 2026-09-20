package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const runColumns = `id::text, job_id::text, scheduled_time, trigger, status, attempt,
	worker_id, queued_at, started_at, finished_at, last_heartbeat_at, next_attempt_at,
	error, output`

// runListColumns is runColumns with the potentially large output left out.
const runListColumns = `id::text, job_id::text, scheduled_time, trigger, status, attempt,
	worker_id, queued_at, started_at, finished_at, last_heartbeat_at, next_attempt_at,
	error, NULL::text`

// ListRuns returns the most recent runs, newest occurrence first, optionally
// only those of one job. Output is left out; GetRun returns it.
func (db *DB) ListRuns(ctx context.Context, jobID *string, limit int) ([]Run, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT `+runListColumns+` FROM job_runs
		WHERE $1::uuid IS NULL OR job_id = $1::uuid
		ORDER BY scheduled_time DESC, id
		LIMIT $2`, jobID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	runs := []Run{}
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

// pgForeignKeyViolation is the SQLSTATE for a missing referenced row.
const pgForeignKeyViolation = "23503"

func scanRun(s scanner) (Run, error) {
	var r Run
	err := s.Scan(&r.ID, &r.JobID, &r.ScheduledTime, &r.Trigger, &r.Status, &r.Attempt,
		&r.WorkerID, &r.QueuedAt, &r.StartedAt, &r.FinishedAt, &r.LastHeartbeatAt,
		&r.NextAttemptAt, &r.Error, &r.Output)
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, ErrNotFound
	}
	return r, err
}

// CreateManualRun queues a run triggered by a user rather than the schedule.
// It returns ErrNotFound if the job does not exist.
func (db *DB) CreateManualRun(ctx context.Context, jobID string) (Run, error) {
	row := db.Pool.QueryRow(ctx, `
		INSERT INTO job_runs (id, job_id, scheduled_time, trigger)
		VALUES ($1, $2, clock_timestamp(), 'manual')
		RETURNING `+runColumns, NewID(), jobID)
	r, err := scanRun(row)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgForeignKeyViolation {
		return Run{}, ErrNotFound
	}
	return r, err
}

// FailQueuedRun marks a run that never reached a worker as failed. A run that
// a worker has already claimed is left alone.
func (db *DB) FailQueuedRun(ctx context.Context, runID, reason string) error {
	_, err := db.Pool.Exec(ctx, `
		UPDATE job_runs SET status = 'FAILED', error = $2, finished_at = now()
		WHERE id = $1 AND status = 'QUEUED'`, runID, reason)
	return err
}

// GetRun returns a run by id.
func (db *DB) GetRun(ctx context.Context, id string) (Run, error) {
	row := db.Pool.QueryRow(ctx, `SELECT `+runColumns+` FROM job_runs WHERE id = $1`, id)
	return scanRun(row)
}

// ListLogs returns up to limit lines of one attempt of a run, in order,
// starting at seq fromSeq.
func (db *DB) ListLogs(ctx context.Context, runID string, attempt int, fromSeq int64, limit int) ([]LogLine, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT attempt, seq, ts, level, line FROM job_logs
		WHERE run_id = $1 AND attempt = $2 AND seq >= $3
		ORDER BY seq
		LIMIT $4`, runID, attempt, fromSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	lines := []LogLine{}
	for rows.Next() {
		var l LogLine
		if err := rows.Scan(&l.Attempt, &l.Seq, &l.TS, &l.Level, &l.Line); err != nil {
			return nil, err
		}
		lines = append(lines, l)
	}
	return lines, rows.Err()
}
