package store

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// These cover the recovery paths that keep a run executing exactly once when
// its worker dies, its dispatch is lost, or it is waiting out a retry backoff.
// They need the same scratch database as store_test.go.

func TestFailOrRetryFailsImmediatelyWithoutRetries(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	run := newTestRun(t, db, 0) // one attempt and no retries
	mustClaim(t, db, run.ID, "w1")

	status, err := db.FailOrRetry(ctx, run.ID, "w1", 1, "boom", time.Hour)

	if err != nil || status != StatusFailed {
		t.Fatalf("status=%q err=%v, want FAILED on the first failure of a job with no retries", status, err)
	}
	got := mustGetRun(t, db, run.ID)
	if got.Attempt != 1 || got.NextAttemptAt != nil || got.FinishedAt == nil {
		t.Errorf("failed run = attempt %d next_attempt_at %v finished_at %v, want attempt 1, no retry, finished",
			got.Attempt, got.NextAttemptAt, got.FinishedAt)
	}
	// A terminal failure keeps worker_id, so the run still shows where it ran.
	if got.WorkerID == nil || *got.WorkerID != "w1" {
		t.Errorf("worker_id = %v, want it kept as w1", got.WorkerID)
	}
}

func TestReapStaleFailsImmediatelyWithoutRetries(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	run := newTestRun(t, db, 0)
	mustClaim(t, db, run.ID, "w1")
	exec(t, db, `UPDATE job_runs SET last_heartbeat_at = now() - interval '1 hour' WHERE id = $1`, run.ID)

	reaped, err := db.ReapStale(ctx, time.Minute, 100)

	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	got, ok := find(reaped, run.ID)
	if !ok || got.Status != StatusFailed || got.Attempt != 1 {
		t.Fatalf("reap = %+v (found %v), want FAILED attempt 1 rather than a second attempt", got, ok)
	}
	if final := mustGetRun(t, db, run.ID); final.Status != StatusFailed || final.FinishedAt == nil {
		t.Errorf("run = status %s finished_at %v, want FAILED and finished", final.Status, final.FinishedAt)
	}
}

func TestSweepQueuedLeavesBackedOffRetries(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	run := newTestRun(t, db, 3)
	mustClaim(t, db, run.ID, "w1")
	// Fail it, so it is QUEUED again with an hour of backoff left to wait...
	if _, err := db.FailOrRetry(ctx, run.ID, "w1", 1, "boom", time.Hour); err != nil {
		t.Fatalf("fail: %v", err)
	}
	// ...and old enough that the sweeper would otherwise re-dispatch it.
	exec(t, db, `UPDATE job_runs SET queued_at = now() - interval '1 hour' WHERE id = $1`, run.ID)

	swept, err := db.SweepQueued(ctx, time.Minute, 100)

	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if _, ok := find(swept, run.ID); ok {
		t.Error("swept a retry whose backoff has not elapsed; a worker would then run it early")
	}

	// Once the backoff has elapsed the run is the sweeper's business again.
	exec(t, db, `UPDATE job_runs SET next_attempt_at = now() - interval '1 second' WHERE id = $1`, run.ID)
	swept, err = db.SweepQueued(ctx, time.Minute, 100)
	if err != nil {
		t.Fatalf("second sweep: %v", err)
	}
	if got, ok := find(swept, run.ID); !ok || got.Attempt != 2 {
		t.Errorf("sweep after the backoff = %+v (found %v), want attempt 2", got, ok)
	}
}

func TestClaimRunAcceptsADueRetry(t *testing.T) {
	// A retry is normally claimed after DueRetries has cleared next_attempt_at,
	// but a re-delivered dispatch can reach a worker with it still set.
	db := openTestDB(t)
	ctx := context.Background()
	run := newTestRun(t, db, 3)
	mustClaim(t, db, run.ID, "w1")
	if _, err := db.FailOrRetry(ctx, run.ID, "w1", 1, "boom", time.Hour); err != nil {
		t.Fatalf("fail: %v", err)
	}
	if _, ok, _ := db.ClaimRun(ctx, run.ID, "w2"); ok {
		t.Fatal("claimed a retry before its backoff elapsed")
	}

	exec(t, db, `UPDATE job_runs SET next_attempt_at = now() - interval '1 second' WHERE id = $1`, run.ID)

	got := mustClaim(t, db, run.ID, "w2")
	if got.Status != StatusRunning || got.Attempt != 2 {
		t.Errorf("claimed run = status %s attempt %d, want RUNNING attempt 2", got.Status, got.Attempt)
	}
	if got.NextAttemptAt != nil {
		t.Errorf("next_attempt_at = %v, want the claim to have cleared it", got.NextAttemptAt)
	}
}

func TestReapedRunIsHandedOverAndTheOldWorkerLockedOut(t *testing.T) {
	// The core of at-most-one-execution: a worker the reaper gave up on is
	// still executing, and every write it makes afterwards must be discarded
	// rather than overwrite the attempt that replaced it.
	db := openTestDB(t)
	ctx := context.Background()
	run := newTestRun(t, db, 3)
	mustClaim(t, db, run.ID, "w1")
	exec(t, db, `UPDATE job_runs SET last_heartbeat_at = now() - interval '1 hour' WHERE id = $1`, run.ID)
	if _, err := db.ReapStale(ctx, time.Minute, 100); err != nil {
		t.Fatalf("reap: %v", err)
	}

	second := mustClaim(t, db, run.ID, "w2")
	if second.Attempt != 2 {
		t.Fatalf("re-claim = attempt %d, want attempt 2", second.Attempt)
	}

	// w1 only learns it lost the run when it tries to write.
	if ok, err := db.FinishRun(ctx, run.ID, "w1", 1, StatusSucceeded, ptr("stale output"), nil); ok || err != nil {
		t.Errorf("stale FinishRun: ok=%v err=%v, want ok=false", ok, err)
	}
	if status, err := db.FailOrRetry(ctx, run.ID, "w1", 1, "stale failure", time.Second); status != "" || err != nil {
		t.Errorf("stale FailOrRetry: status=%q err=%v, want no change", status, err)
	}
	if ok, err := db.ReleaseRun(ctx, run.ID, "w1", 1, "stale release"); ok || err != nil {
		t.Errorf("stale ReleaseRun: ok=%v err=%v, want ok=false", ok, err)
	}
	if ok, err := db.Heartbeat(ctx, run.ID, "w1", 1); ok || err != nil {
		t.Errorf("stale Heartbeat: ok=%v err=%v, want ok=false", ok, err)
	}

	// None of it touched the attempt w2 owns.
	got := mustGetRun(t, db, run.ID)
	if got.Status != StatusRunning || got.Attempt != 2 || got.Output != nil ||
		got.WorkerID == nil || *got.WorkerID != "w2" {
		t.Errorf("run = status %s attempt %d worker %v output %v, want RUNNING attempt 2 on w2 with no output",
			got.Status, got.Attempt, got.WorkerID, got.Output)
	}
	if ok, err := db.FinishRun(ctx, run.ID, "w2", 2, StatusSucceeded, ptr("real output"), nil); !ok || err != nil {
		t.Fatalf("the owning worker could not finish the run: ok=%v err=%v", ok, err)
	}
	if final := mustGetRun(t, db, run.ID); final.Output == nil || *final.Output != "real output" {
		t.Errorf("output = %v, want the surviving attempt's output", final.Output)
	}
}

func TestClaimRunHasOneWinnerUnderConcurrency(t *testing.T) {
	// The conditional UPDATE, not the dispatch path, is what stops two workers
	// executing the same run, so hit it from several connections at once.
	db := openTestDB(t)
	run := newTestRun(t, db, 3)

	const workers = 8
	var ready, done sync.WaitGroup
	start := make(chan struct{})
	claimed := make([]bool, workers)
	errs := make([]error, workers)
	ready.Add(workers)
	done.Add(workers)
	for i := range workers {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			_, ok, err := db.ClaimRun(context.Background(), run.ID, fmt.Sprintf("w%d", i))
			claimed[i], errs[i] = ok, err
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()

	winners := 0
	for i, ok := range claimed {
		if errs[i] != nil {
			t.Errorf("worker %d: %v", i, errs[i])
		}
		if ok {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("%d of %d concurrent workers claimed the run, want exactly 1", winners, workers)
	}
}

func TestHeartbeatKeepsARunFromBeingReaped(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	run := newTestRun(t, db, 3)
	mustClaim(t, db, run.ID, "w1")
	exec(t, db, `UPDATE job_runs SET last_heartbeat_at = now() - interval '1 hour' WHERE id = $1`, run.ID)

	// A slow job that is still alive checks in just before the reaper looks.
	if ok, err := db.Heartbeat(ctx, run.ID, "w1", 1); !ok || err != nil {
		t.Fatalf("heartbeat: ok=%v err=%v", ok, err)
	}

	reaped, err := db.ReapStale(ctx, time.Minute, 100)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if _, ok := find(reaped, run.ID); ok {
		t.Error("reaped a run that had just heartbeated")
	}
}

func TestReapStaleTakesTheStalestFirstUpToTheLimit(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	// Three runs, silent for three, two and one hours.
	ids := make([]string, 3)
	for i := range ids {
		r := newTestRun(t, db, 3)
		ids[i] = r.ID
		mustClaim(t, db, r.ID, "w1")
		exec(t, db, `UPDATE job_runs SET last_heartbeat_at = now() - make_interval(hours => $2) WHERE id = $1`,
			r.ID, len(ids)-i)
	}

	// One pass handles at most `limit` runs, so a backlog cannot produce one
	// enormous transaction; the rest wait for the next pass.
	var order []string
	for pass := 0; pass < 10 && len(order) < len(ids); pass++ {
		reaped, err := db.ReapStale(ctx, time.Minute, 1)
		if err != nil {
			t.Fatalf("reap: %v", err)
		}
		if len(reaped) > 1 {
			t.Fatalf("reaped %d runs with limit 1", len(reaped))
		}
		for _, r := range reaped {
			if slices.Contains(ids, r.RunID) {
				order = append(order, r.RunID)
			}
		}
	}

	if strings.Join(order, ",") != strings.Join(ids, ",") {
		t.Errorf("reaped in order %v, want the stalest run first: %v", order, ids)
	}
}

func TestReapStaleSkipsRunsLockedByAnotherWorker(t *testing.T) {
	// Every worker runs the reaper. SKIP LOCKED is what lets them overlap
	// without blocking on each other or reaping the same run twice.
	db := openTestDB(t)
	ctx := context.Background()
	run := newTestRun(t, db, 3)
	mustClaim(t, db, run.ID, "w1")
	exec(t, db, `UPDATE job_runs SET last_heartbeat_at = now() - interval '1 hour' WHERE id = $1`, run.ID)

	tx, err := db.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT 1 FROM job_runs WHERE id = $1 FOR UPDATE`, run.ID); err != nil {
		t.Fatalf("lock the run: %v", err)
	}

	// Without SKIP LOCKED this waits for the other transaction; the timeout
	// turns that into a failure rather than a hung test.
	reapCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	reaped, err := db.ReapStale(reapCtx, time.Minute, 100)
	if err != nil {
		t.Fatalf("reap while the run was locked: %v", err)
	}
	if _, ok := find(reaped, run.ID); ok {
		t.Error("reaped a run another transaction had locked")
	}

	// Once that transaction ends, the run is reapable again.
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	reaped, err = db.ReapStale(ctx, time.Minute, 100)
	if err != nil {
		t.Fatalf("second reap: %v", err)
	}
	if _, ok := find(reaped, run.ID); !ok {
		t.Error("the run stayed unreaped after the lock was released")
	}
}
