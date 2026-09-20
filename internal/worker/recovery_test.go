package worker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jingxu/jobsys/internal/store"
)

// syncBuffer collects log output written from the worker's goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureLog points the worker's log at a buffer. The paths that discard a
// result the worker no longer owns have no effect other than what they log,
// so the log is what a test can assert on.
func captureLog(w *Worker) *syncBuffer {
	b := &syncBuffer{}
	w.log = slog.New(slog.NewTextHandler(b, nil))
	return b
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// A worker whose run the reaper requeued keeps executing until its next
// heartbeat. Whatever it writes when it finishes must be dropped, not
// overwrite the attempt that replaced it.

func TestSucceedDiscardsAResultTheWorkerNoLongerOwns(t *testing.T) {
	w, st, _ := newTestWorker(t, `{"lines": 2}`, 60)
	logs := captureLog(w)
	st.ownerLost = true

	w.execute(context.Background(), claimedRun("run-1"))

	if got := st.results(); len(got) != 1 || got[0].status != store.StatusSucceeded {
		t.Fatalf("results = %+v, want the worker to have attempted the write", got)
	}
	if !strings.Contains(logs.String(), "no longer owns the run") {
		t.Errorf("log = %q, want it to report the result was discarded", logs.String())
	}
}

func TestFailDiscardsAFailureTheWorkerNoLongerOwns(t *testing.T) {
	w, st, _ := newTestWorker(t, `{"lines": 5, "fail_at": 2}`, 60)
	logs := captureLog(w)
	st.ownerLost = true

	w.execute(context.Background(), claimedRun("run-1"))

	if got := st.failures(); len(got) != 1 {
		t.Fatalf("failures = %+v, want the worker to have attempted the write", got)
	}
	if !strings.Contains(logs.String(), "no longer owns the run") {
		t.Errorf("log = %q, want it to report the failure was discarded", logs.String())
	}
}

func TestFailReportsARunThatIsOutOfRetries(t *testing.T) {
	w, st, _ := newTestWorker(t, `{"lines": 5, "fail_at": 2}`, 60)
	logs := captureLog(w)
	st.failStatus = store.StatusFailed // the store used up the job's retries

	w.execute(context.Background(), claimedRun("run-1"))

	out := logs.String()
	if !strings.Contains(out, "status="+store.StatusFailed) {
		t.Errorf("log = %q, want the terminal FAILED status reported", out)
	}
	if strings.Contains(out, "will retry") {
		t.Errorf("log = %q, want no retry announced for a run that is out of retries", out)
	}
}

func TestReleaseLeavesTheRunToTheReaperWhenItCannotBeReleased(t *testing.T) {
	w, st, d := newTestWorker(t, `{"lines": 100, "delay_ms": 20}`, 60)
	logs := captureLog(w)
	st.ownerLost = true
	jobsCtx, stopJobs := context.WithCancelCause(context.Background())
	time.AfterFunc(30*time.Millisecond, func() { stopJobs(errShutdown) })

	w.execute(jobsCtx, claimedRun("run-1"))

	if len(d.dispatched()) != 0 {
		t.Errorf("dispatched %+v, want nothing re-dispatched for a run this worker had already lost", d.dispatched())
	}
	if !strings.Contains(logs.String(), "could not release run") {
		t.Errorf("log = %q, want it to hand the run to the reaper", logs.String())
	}
}

func TestReleaseLeavesTheRunToTheSweeperWhenItCannotBeDispatched(t *testing.T) {
	w, st, d := newTestWorker(t, `{"lines": 100, "delay_ms": 20}`, 60)
	logs := captureLog(w)
	d.setErr(errors.New("kafka down"))
	jobsCtx, stopJobs := context.WithCancelCause(context.Background())
	time.AfterFunc(30*time.Millisecond, func() { stopJobs(errShutdown) })

	w.execute(jobsCtx, claimedRun("run-1"))

	// The release still happened, so the run is QUEUED for another worker;
	// only the shortcut of waking one immediately was lost.
	if len(st.released) != 1 {
		t.Errorf("released %v, want the run handed back to the queue", st.released)
	}
	if !strings.Contains(logs.String(), "could not re-dispatch") {
		t.Errorf("log = %q, want it to leave the run to the sweeper", logs.String())
	}
}

// The maintenance loop turns what the store recovered into dispatches. Which
// runs it wakes a worker for, and which it leaves alone, is the decision
// under test here.

func TestMaintainDispatchesRecoveredRunsButNotFailedOnes(t *testing.T) {
	w, st, d := newTestWorker(t, `{"lines": 1}`, 60)
	w.cfg.MaintenanceInterval = time.Millisecond
	redispatch := func(id string, status string, attempt int) store.Redispatch {
		return store.Redispatch{RunID: id, JobID: "job-1", JobType: "generate_text", Status: status, Attempt: attempt}
	}
	st.reaped = []store.Redispatch{
		redispatch("reaped-queued", store.StatusQueued, 2),
		// Its worker died with no retries left: there is nothing to run.
		redispatch("reaped-failed", store.StatusFailed, 3),
	}
	st.dueRetries = []store.Redispatch{redispatch("retry-due", store.StatusQueued, 2)}
	st.swept = []store.Redispatch{redispatch("swept", store.StatusQueued, 1)}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.maintain(ctx) }()
	waitFor(t, "the recovered runs to be dispatched", func() bool { return len(d.dispatched()) >= 3 })
	cancel()
	<-done

	var ids []string
	for _, r := range d.dispatched() {
		ids = append(ids, r.ID)
	}
	if want := "reaped-queued,retry-due,swept"; strings.Join(ids, ",") != want {
		t.Errorf("dispatched %v, want %s and no run that already FAILED", ids, want)
	}
	// The dispatch has to carry the new attempt number, or the worker that
	// picks it up claims the wrong attempt.
	if got := d.dispatched()[0]; got.Attempt != 2 || got.JobID != "job-1" {
		t.Errorf("first dispatch = %+v, want job-1 attempt 2", got)
	}
}

func TestMaintainKeepsGoingAfterStoreAndDispatchErrors(t *testing.T) {
	// A maintenance pass that gives up for good would strand every run whose
	// worker dies later, so both failures have to be survivable.
	w, st, d := newTestWorker(t, `{"lines": 1}`, 60)
	w.cfg.MaintenanceInterval = time.Millisecond
	logs := captureLog(w)
	st.maintenanceErr = errors.New("database down")
	st.swept = []store.Redispatch{{RunID: "swept-1", JobID: "job-1", JobType: "generate_text", Status: store.StatusQueued, Attempt: 1}}
	d.setErr(errors.New("kafka down"))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.maintain(ctx) }()
	waitFor(t, "the first dispatch attempt", func() bool { return len(d.dispatched()) >= 1 })

	// Both are back: a later pass must still pick up what the store returns.
	st.maintenanceErr = nil
	d.setErr(nil)
	st.setPending(&st.swept, []store.Redispatch{
		{RunID: "swept-2", JobID: "job-1", JobType: "generate_text", Status: store.StatusQueued, Attempt: 1},
	})
	waitFor(t, "a later pass to recover", func() bool { return len(d.dispatched()) >= 2 })
	cancel()
	<-done

	if got := d.dispatched()[1]; got.ID != "swept-2" {
		t.Errorf("second dispatch = %s, want swept-2", got.ID)
	}
	if out := logs.String(); !strings.Contains(out, "sweeper failed") || !strings.Contains(out, "re-dispatch failed") {
		t.Errorf("log = %q, want both the store and the dispatch failure reported", out)
	}
}

func TestMaintainStopsWhenTheWorkerShutsDown(t *testing.T) {
	w, _, _ := newTestWorker(t, `{"lines": 1}`, 60)
	w.cfg.MaintenanceInterval = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.maintain(ctx) }()
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the maintenance loop kept running after the worker was told to stop")
	}
}
