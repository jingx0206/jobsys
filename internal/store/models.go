package store

import (
	"encoding/json"
	"errors"
	"time"
)

// ErrNotFound is returned when a job or run does not exist.
var ErrNotFound = errors.New("not found")

// Run statuses, matching the CHECK constraint on job_runs.status.
const (
	StatusQueued    = "QUEUED"
	StatusRunning   = "RUNNING"
	StatusSucceeded = "SUCCEEDED"
	StatusFailed    = "FAILED"
	StatusCancelled = "CANCELLED"
)

// Job is a job definition: what to run and when.
type Job struct {
	ID         string          `json:"id"`
	Name       string          `json:"name"`
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	CronExpr   *string         `json:"cron_expr,omitempty"`
	Timezone   string          `json:"timezone"`
	NextRunAt  *time.Time      `json:"next_run_at,omitempty"`
	Enabled    bool            `json:"enabled"`
	MaxRetries int             `json:"max_retries"`
	TimeoutSec int             `json:"timeout_sec"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

// NewJob holds the fields needed to create a job.
type NewJob struct {
	Name       string
	Type       string
	Payload    json.RawMessage
	CronExpr   *string
	Timezone   string
	NextRunAt  *time.Time
	MaxRetries int
	TimeoutSec int
}

// Run is one execution of a job.
type Run struct {
	ID              string     `json:"id"`
	JobID           string     `json:"job_id"`
	ScheduledTime   time.Time  `json:"scheduled_time"`
	Trigger         string     `json:"trigger"`
	Status          string     `json:"status"`
	Attempt         int        `json:"attempt"`
	WorkerID        *string    `json:"worker_id,omitempty"`
	QueuedAt        time.Time  `json:"queued_at"`
	StartedAt       *time.Time `json:"started_at,omitempty"`
	FinishedAt      *time.Time `json:"finished_at,omitempty"`
	LastHeartbeatAt *time.Time `json:"last_heartbeat_at,omitempty"`
	NextAttemptAt   *time.Time `json:"next_attempt_at,omitempty"`
	Error           *string    `json:"error,omitempty"`
	Output          *string    `json:"output,omitempty"`
}

// Redispatch is a run the worker's maintenance loop has requeued or found
// stuck, which may need publishing to Kafka again.
type Redispatch struct {
	RunID   string
	JobID   string
	JobType string
	Status  string
	Attempt int
}

// LogLine is one line of a run's log.
type LogLine struct {
	Attempt int       `json:"attempt"`
	Seq     int64     `json:"seq"`
	TS      time.Time `json:"ts"`
	Level   string    `json:"level"`
	Line    string    `json:"line"`
}
