package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

const jobColumns = `id::text, name, type, payload, cron_expr, timezone, next_run_at,
	enabled, max_retries, timeout_sec, created_at, updated_at`

func scanJob(s scanner) (Job, error) {
	var j Job
	err := s.Scan(&j.ID, &j.Name, &j.Type, &j.Payload, &j.CronExpr, &j.Timezone,
		&j.NextRunAt, &j.Enabled, &j.MaxRetries, &j.TimeoutSec, &j.CreatedAt, &j.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	return j, err
}

// CreateJob inserts a job definition.
func (db *DB) CreateJob(ctx context.Context, n NewJob) (Job, error) {
	row := db.Pool.QueryRow(ctx, `
		INSERT INTO jobs (id, name, type, payload, cron_expr, timezone, next_run_at,
		                  max_retries, timeout_sec)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING `+jobColumns,
		NewID(), n.Name, n.Type, n.Payload, n.CronExpr, n.Timezone, n.NextRunAt,
		n.MaxRetries, n.TimeoutSec)
	return scanJob(row)
}

// SetJobEnabled pauses or resumes a job and sets its next_run_at, which is nil
// for a paused or manual-only job.
func (db *DB) SetJobEnabled(ctx context.Context, id string, enabled bool, nextRunAt *time.Time) (Job, error) {
	row := db.Pool.QueryRow(ctx, `
		UPDATE jobs SET enabled = $2, next_run_at = $3, updated_at = now()
		WHERE id = $1
		RETURNING `+jobColumns, id, enabled, nextRunAt)
	return scanJob(row)
}

// GetJob returns a job by id.
func (db *DB) GetJob(ctx context.Context, id string) (Job, error) {
	row := db.Pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id)
	return scanJob(row)
}

// ListJobs returns the most recently created jobs first.
func (db *DB) ListJobs(ctx context.Context, limit int) ([]Job, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT `+jobColumns+` FROM jobs
		ORDER BY created_at DESC, id
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := []Job{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}
