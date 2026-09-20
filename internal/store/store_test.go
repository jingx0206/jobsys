package store

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

// These tests run the worker SQL against a real Postgres, since claiming,
// reaping and sweeping are only correct if the queries are. Point
// JOBSYS_TEST_DATABASE_URL at a scratch database with the migrations applied.

func openTestDB(t *testing.T) *DB {
	t.Helper()
	dsn := os.Getenv("JOBSYS_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set JOBSYS_TEST_DATABASE_URL to run store integration tests")
	}
	db, err := Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(db.Close)
	return db
}

// newTestRun creates a job and a QUEUED run of it, deleted after the test.
func newTestRun(t *testing.T, db *DB, maxRetries int) Run {
	t.Helper()
	ctx := context.Background()
	j, err := db.CreateJob(ctx, NewJob{
		Name: "store-test", Type: "generate_text", Payload: json.RawMessage(`{"lines": 1}`),
		Timezone: "UTC", MaxRetries: maxRetries, TimeoutSec: 60,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	t.Cleanup(func() {
		db.Pool.Exec(context.Background(), `DELETE FROM jobs WHERE id = $1`, j.ID)
	})
	r, err := db.CreateManualRun(ctx, j.ID)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	return r
}

func mustClaim(t *testing.T, db *DB, runID, workerID string) Run {
	t.Helper()
	r, ok, err := db.ClaimRun(context.Background(), runID, workerID)
	if err != nil || !ok {
		t.Fatalf("claim %s as %s: ok=%v err=%v", runID, workerID, ok, err)
	}
	return r
}

func mustGetRun(t *testing.T, db *DB, runID string) Run {
	t.Helper()
	r, err := db.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	return r
}

func exec(t *testing.T, db *DB, sql string, args ...any) {
	t.Helper()
	if _, err := db.Pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func find(rs []Redispatch, runID string) (Redispatch, bool) {
	for _, r := range rs {
		if r.RunID == runID {
			return r, true
		}
	}
	return Redispatch{}, false
}

func ptr(s string) *string { return &s }

func TestClaimRunIsExclusive(t *testing.T) {
	db := openTestDB(t)
	run := newTestRun(t, db, 3)

	got := mustClaim(t, db, run.ID, "w1")
	if got.Status != StatusRunning || got.WorkerID == nil || *got.WorkerID != "w1" {
		t.Errorf("claimed run = status %s worker %v, want RUNNING on w1", got.Status, got.WorkerID)
	}

	if _, ok, err := db.ClaimRun(context.Background(), run.ID, "w2"); err != nil || ok {
		t.Errorf("second claim: ok=%v err=%v, want ok=false", ok, err)
	}
}

func TestHeartbeatAndFinishRequireOwnership(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	run := mustClaim(t, db, newTestRun(t, db, 3).ID, "w1")

	if ok, _ := db.Heartbeat(ctx, run.ID, "w2", 1); ok {
		t.Error("a worker that does not own the run could heartbeat it")
	}
	if ok, _ := db.Heartbeat(ctx, run.ID, "w1", 1); !ok {
		t.Error("the owning worker could not heartbeat")
	}
	if ok, _ := db.FinishRun(ctx, run.ID, "w2", 1, StatusSucceeded, ptr("stolen"), nil); ok {
		t.Error("a worker that does not own the run could finish it")
	}
	if ok, err := db.FinishRun(ctx, run.ID, "w1", 1, StatusSucceeded, ptr("done"), nil); !ok || err != nil {
		t.Fatalf("owner finish: ok=%v err=%v", ok, err)
	}

	got := mustGetRun(t, db, run.ID)
	if got.Status != StatusSucceeded || got.Output == nil || *got.Output != "done" || got.FinishedAt == nil {
		t.Errorf("finished run = %+v, want SUCCEEDED with output and finished_at", got)
	}
	if ok, _ := db.Heartbeat(ctx, run.ID, "w1", 1); ok {
		t.Error("heartbeat succeeded on a finished run")
	}
}

func TestReapStaleRequeuesThenFailsWhenRetriesRunOut(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	run := newTestRun(t, db, 1) // two attempts in total

	mustClaim(t, db, run.ID, "w1")
	exec(t, db, `UPDATE job_runs SET last_heartbeat_at = now() - interval '1 hour' WHERE id = $1`, run.ID)

	reaped, err := db.ReapStale(ctx, time.Minute, 100)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	got, ok := find(reaped, run.ID)
	if !ok || got.Status != StatusQueued || got.Attempt != 2 || got.JobType != "generate_text" {
		t.Fatalf("first reap = %+v (found %v), want QUEUED attempt 2", got, ok)
	}
	requeued := mustGetRun(t, db, run.ID)
	if requeued.WorkerID != nil || requeued.Error == nil || !strings.Contains(*requeued.Error, "attempt 1 lost worker w1") {
		t.Errorf("requeued run = worker %v error %v, want no worker and a lost-worker error", requeued.WorkerID, requeued.Error)
	}
	if ok, _ := db.Heartbeat(ctx, run.ID, "w1", 1); ok {
		t.Error("the reaped worker could still heartbeat")
	}

	mustClaim(t, db, run.ID, "w2")
	exec(t, db, `UPDATE job_runs SET last_heartbeat_at = now() - interval '1 hour' WHERE id = $1`, run.ID)
	reaped, err = db.ReapStale(ctx, time.Minute, 100)
	if err != nil {
		t.Fatalf("second reap: %v", err)
	}
	if got, ok := find(reaped, run.ID); !ok || got.Status != StatusFailed || got.Attempt != 2 {
		t.Fatalf("second reap = %+v (found %v), want FAILED attempt 2", got, ok)
	}
	if final := mustGetRun(t, db, run.ID); final.FinishedAt == nil {
		t.Error("run failed by the reaper has no finished_at")
	}
}

func TestReapStaleLeavesHealthyRuns(t *testing.T) {
	db := openTestDB(t)
	run := mustClaim(t, db, newTestRun(t, db, 3).ID, "w1")

	reaped, err := db.ReapStale(context.Background(), time.Minute, 100)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if _, ok := find(reaped, run.ID); ok {
		t.Error("reaped a run with a fresh heartbeat")
	}
}

func TestSweepQueuedRedispatchesStuckRunsOnce(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	run := newTestRun(t, db, 3)
	exec(t, db, `UPDATE job_runs SET queued_at = now() - interval '1 hour' WHERE id = $1`, run.ID)

	swept, err := db.SweepQueued(ctx, time.Minute, 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if got, ok := find(swept, run.ID); !ok || got.Status != StatusQueued {
		t.Fatalf("sweep = %+v (found %v), want the stuck run", got, ok)
	}

	swept, err = db.SweepQueued(ctx, time.Minute, 100)
	if err != nil {
		t.Fatalf("second sweep: %v", err)
	}
	if _, ok := find(swept, run.ID); ok {
		t.Error("second sweep re-dispatched the run again straight away")
	}
}

func TestReleaseRunRequeuesNextAttempt(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	run := mustClaim(t, db, newTestRun(t, db, 3).ID, "w1")

	if ok, err := db.ReleaseRun(ctx, run.ID, "w1", 1, "worker shutting down"); !ok || err != nil {
		t.Fatalf("release: ok=%v err=%v", ok, err)
	}
	got := mustGetRun(t, db, run.ID)
	if got.Status != StatusQueued || got.Attempt != 2 || got.WorkerID != nil {
		t.Errorf("released run = status %s attempt %d worker %v, want QUEUED attempt 2 unowned",
			got.Status, got.Attempt, got.WorkerID)
	}
	if ok, _ := db.ReleaseRun(ctx, run.ID, "w1", 1, "again"); ok {
		t.Error("released the same attempt twice")
	}
}

func TestAppendLogsIgnoresDuplicates(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	run := newTestRun(t, db, 3)
	now := time.Now().UTC()
	line := func(seq int64, text string) LogLine {
		return LogLine{Seq: seq, TS: now, Level: "info", Line: text}
	}

	for _, batch := range [][]LogLine{
		{line(1, "a"), line(2, "b")},
		{line(1, "a"), line(2, "b")}, // a retried flush
		{line(3, "c")},
	} {
		if err := db.AppendLogs(ctx, run.ID, 1, batch); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	lines, err := db.ListLogs(ctx, run.ID, 1, 1, 100)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	var got []string
	for _, l := range lines {
		got = append(got, l.Line)
	}
	if strings.Join(got, ",") != "a,b,c" {
		t.Errorf("lines = %v, want [a b c]", got)
	}
}
