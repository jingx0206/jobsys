package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// DueJob is a scheduled job whose next_run_at has passed.
type DueJob struct {
	ID        string
	Type      string
	CronExpr  string
	Timezone  string
	NextRunAt time.Time
}

// FireDueJobs queues a run for each enabled scheduled job whose next_run_at
// has passed, and advances next_run_at to the time next returns. It handles
// up to limit jobs in one transaction and returns the queued runs, which the
// caller dispatches.
//
// Each occurrence is queued exactly once: SKIP LOCKED keeps concurrent
// schedulers off the same job, and the (job_id, scheduled_time) unique key
// makes a repeat fire of an occurrence a no-op. A scheduler that was down
// fires each overdue job once, for the occurrence stored in next_run_at, and
// moves on to next's answer rather than replaying every missed occurrence.
//
// If next fails for a job, its next_run_at is cleared so it stops firing, and
// the error is returned alongside the runs that were queued.
func (db *DB) FireDueJobs(ctx context.Context, limit int, next func(DueJob) (time.Time, error)) ([]Redispatch, error) {
	tx, err := db.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) // no-op once committed

	rows, err := tx.Query(ctx, `
		SELECT id::text, type, cron_expr, timezone, next_run_at
		FROM jobs
		WHERE enabled AND cron_expr IS NOT NULL AND next_run_at <= now()
		ORDER BY next_run_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, err
	}
	var due []DueJob
	for rows.Next() {
		var j DueJob
		if err := rows.Scan(&j.ID, &j.Type, &j.CronExpr, &j.Timezone, &j.NextRunAt); err != nil {
			rows.Close()
			return nil, err
		}
		due = append(due, j)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var fired []Redispatch
	var nextErrs []error
	for _, j := range due {
		r := Redispatch{JobID: j.ID, JobType: j.Type}
		err := tx.QueryRow(ctx, `
			INSERT INTO job_runs (id, job_id, scheduled_time, trigger)
			VALUES ($1, $2, $3, 'schedule')
			ON CONFLICT ON CONSTRAINT job_runs_occurrence_uniq DO NOTHING
			RETURNING id::text, status, attempt`,
			NewID(), j.ID, j.NextRunAt).Scan(&r.RunID, &r.Status, &r.Attempt)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			// Another scheduler already queued this occurrence.
		case err != nil:
			return nil, err
		default:
			fired = append(fired, r)
		}

		var nextAt *time.Time
		if t, err := next(j); err != nil {
			nextErrs = append(nextErrs, fmt.Errorf("job %s: %w", j.ID, err))
		} else {
			nextAt = &t
		}
		if _, err := tx.Exec(ctx, `UPDATE jobs SET next_run_at = $2, updated_at = now() WHERE id = $1`, j.ID, nextAt); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return fired, errors.Join(nextErrs...)
}
