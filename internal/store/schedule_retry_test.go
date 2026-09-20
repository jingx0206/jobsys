package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// newScheduledJob creates a job whose next_run_at is `due`, deleted after the
// test.
func newScheduledJob(t *testing.T, db *DB, due time.Time) Job {
	t.Helper()
	cronExpr := "@every 10s"
	j, err := db.CreateJob(context.Background(), NewJob{
		Name: "schedule-test", Type: "generate_text", Payload: json.RawMessage(`{"lines": 1}`),
		CronExpr: &cronExpr, Timezone: "UTC", NextRunAt: &due, MaxRetries: 3, TimeoutSec: 60,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	t.Cleanup(func() {
		db.Pool.Exec(context.Background(), `DELETE FROM jobs WHERE id = $1`, j.ID)
	})
	return j
}

func findJob(rs []Redispatch, jobID string) (Redispatch, bool) {
	for _, r := range rs {
		if r.JobID == jobID {
			return r, true
		}
	}
	return Redispatch{}, false
}

func TestFireDueJobsQueuesEachOccurrenceOnce(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	due := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	j := newScheduledJob(t, db, due)
	nextAt := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	var asked DueJob
	next := func(d DueJob) (time.Time, error) {
		if d.ID == j.ID {
			asked = d
		}
		return nextAt, nil
	}

	fired, err := db.FireDueJobs(ctx, 100, next)
	if err != nil {
		t.Fatalf("fire: %v", err)
	}
	r, ok := findJob(fired, j.ID)
	if !ok || r.Status != StatusQueued || r.JobType != "generate_text" {
		t.Fatalf("fired = %+v (found %v), want a QUEUED run of the job", r, ok)
	}
	if asked.CronExpr != "@every 10s" || !asked.NextRunAt.Equal(due) {
		t.Errorf("next was asked about %+v, want the job's cron and due time", asked)
	}
	run := mustGetRun(t, db, r.RunID)
	if run.Trigger != "schedule" || !run.ScheduledTime.Equal(due) {
		t.Errorf("run trigger=%s scheduled_time=%v, want schedule at %v", run.Trigger, run.ScheduledTime, due)
	}
	got, err := db.GetJob(ctx, j.ID)
	if err != nil || got.NextRunAt == nil || !got.NextRunAt.Equal(nextAt) {
		t.Errorf("next_run_at = %v (err %v), want %v", got.NextRunAt, err, nextAt)
	}

	fired, err = db.FireDueJobs(ctx, 100, next)
	if err != nil {
		t.Fatalf("second fire: %v", err)
	}
	if _, ok := findJob(fired, j.ID); ok {
		t.Error("fired the job again before its next occurrence")
	}
}

func TestFireDueJobsSkipsAnOccurrenceAlreadyQueued(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	due := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	j := newScheduledJob(t, db, due)
	// A racing scheduler already queued this occurrence.
	exec(t, db, `INSERT INTO job_runs (id, job_id, scheduled_time, trigger) VALUES ($1, $2, $3, 'schedule')`,
		NewID(), j.ID, due)
	nextAt := time.Now().Add(time.Hour).UTC().Truncate(time.Second)

	fired, err := db.FireDueJobs(ctx, 100, func(DueJob) (time.Time, error) { return nextAt, nil })
	if err != nil {
		t.Fatalf("fire: %v", err)
	}
	if _, ok := findJob(fired, j.ID); ok {
		t.Error("queued a second run for an occurrence that already had one")
	}
	if got, _ := db.GetJob(ctx, j.ID); got.NextRunAt == nil || !got.NextRunAt.Equal(nextAt) {
		t.Errorf("next_run_at = %v, want it advanced to %v anyway", got.NextRunAt, nextAt)
	}
}

func TestFailOrRetryBacksOffThenFails(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	run := newTestRun(t, db, 1) // two attempts in total
	mustClaim(t, db, run.ID, "w1")

	status, err := db.FailOrRetry(ctx, run.ID, "w1", 1, "boom", time.Hour)
	if err != nil || status != StatusQueued {
		t.Fatalf("first failure: status=%q err=%v, want QUEUED", status, err)
	}
	got := mustGetRun(t, db, run.ID)
	if got.Attempt != 2 || got.WorkerID != nil || got.Error == nil || *got.Error != "boom" ||
		got.NextAttemptAt == nil || time.Until(*got.NextAttemptAt) < 50*time.Minute {
		t.Fatalf("requeued run = attempt %d worker %v error %v next %v, want attempt 2 due in about an hour",
			got.Attempt, got.WorkerID, got.Error, got.NextAttemptAt)
	}

	// Not due yet: it cannot be claimed, and DueRetries leaves it alone.
	if _, ok, _ := db.ClaimRun(ctx, run.ID, "w2"); ok {
		t.Error("claimed a retry before its backoff elapsed")
	}
	due, err := db.DueRetries(ctx, 100)
	if err != nil {
		t.Fatalf("due retries: %v", err)
	}
	if _, ok := find(due, run.ID); ok {
		t.Error("DueRetries returned a retry that is not due")
	}

	exec(t, db, `UPDATE job_runs SET next_attempt_at = now() - interval '1 second' WHERE id = $1`, run.ID)
	due, err = db.DueRetries(ctx, 100)
	if err != nil {
		t.Fatalf("due retries: %v", err)
	}
	if r, ok := find(due, run.ID); !ok || r.Attempt != 2 {
		t.Fatalf("DueRetries = %+v (found %v), want attempt 2", r, ok)
	}
	if again, _ := db.DueRetries(ctx, 100); len(again) > 0 {
		if _, ok := find(again, run.ID); ok {
			t.Error("DueRetries returned the same retry twice")
		}
	}

	mustClaim(t, db, run.ID, "w2")
	status, err = db.FailOrRetry(ctx, run.ID, "w2", 2, "boom again", time.Hour)
	if err != nil || status != StatusFailed {
		t.Fatalf("second failure: status=%q err=%v, want FAILED", status, err)
	}
	if final := mustGetRun(t, db, run.ID); final.FinishedAt == nil || final.NextAttemptAt != nil {
		t.Errorf("failed run finished_at=%v next_attempt_at=%v, want finished and no retry", final.FinishedAt, final.NextAttemptAt)
	}
}

func TestFailOrRetryRequiresOwnership(t *testing.T) {
	db := openTestDB(t)
	run := mustClaim(t, db, newTestRun(t, db, 3).ID, "w1")

	status, err := db.FailOrRetry(context.Background(), run.ID, "w2", 1, "not mine", time.Second)
	if err != nil || status != "" {
		t.Errorf("status=%q err=%v, want no change from a worker that does not own the run", status, err)
	}
}
