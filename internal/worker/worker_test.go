package worker

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/jingxu/jobsys/internal/store"
)

type finished struct {
	runID, status   string
	output, errText string
}

type retried struct {
	runID, errText string
	backoff        time.Duration
}

// fakeStore is an in-memory Store. Runs listed in claimable can be claimed
// once; every claimed run belongs to job.
type fakeStore struct {
	mu        sync.Mutex
	job       store.Job
	claimable map[string]bool
	lost      bool // Heartbeat reports the worker no longer owns the run
	finished  []finished
	retries   []retried
	released  []string
	appends   [][]store.LogLine

	// ownerLost makes the writes that end an attempt report that the attempt
	// was taken away, as the store does once the reaper has requeued it.
	ownerLost bool
	// failStatus is what FailOrRetry reports; QUEUED, a retry, by default.
	failStatus string

	// What the maintenance queries hand to the next pass that asks, and the
	// error they all report.
	reaped         []store.Redispatch
	dueRetries     []store.Redispatch
	swept          []store.Redispatch
	maintenanceErr error
}

func (f *fakeStore) ClaimRun(_ context.Context, runID, workerID string) (store.Run, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.claimable[runID] {
		return store.Run{}, false, nil
	}
	delete(f.claimable, runID)
	return store.Run{ID: runID, JobID: f.job.ID, Status: store.StatusRunning, Attempt: 1, WorkerID: &workerID}, true, nil
}

func (f *fakeStore) GetJob(context.Context, string) (store.Job, error) {
	return f.job, nil
}

func (f *fakeStore) Heartbeat(context.Context, string, string, int) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.lost, nil
}

func (f *fakeStore) FinishRun(_ context.Context, runID, _ string, _ int, status string, output, errMsg *string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fin := finished{runID: runID, status: status}
	if output != nil {
		fin.output = *output
	}
	if errMsg != nil {
		fin.errText = *errMsg
	}
	f.finished = append(f.finished, fin)
	return !f.ownerLost, nil
}

func (f *fakeStore) FailOrRetry(_ context.Context, runID, _ string, _ int, errMsg string, backoff time.Duration) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.retries = append(f.retries, retried{runID: runID, errText: errMsg, backoff: backoff})
	switch {
	case f.ownerLost:
		return "", nil
	case f.failStatus != "":
		return f.failStatus, nil
	default:
		return store.StatusQueued, nil
	}
}

func (f *fakeStore) ReleaseRun(_ context.Context, runID, _ string, _ int, _ string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.released = append(f.released, runID)
	return !f.ownerLost, nil
}

func (f *fakeStore) AppendLogs(_ context.Context, _ string, _ int, lines []store.LogLine) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.appends = append(f.appends, lines)
	return nil
}

func (f *fakeStore) ReapStale(context.Context, time.Duration, int) ([]store.Redispatch, error) {
	return f.takePending(&f.reaped)
}

func (f *fakeStore) SweepQueued(context.Context, time.Duration, int) ([]store.Redispatch, error) {
	return f.takePending(&f.swept)
}

func (f *fakeStore) DueRetries(context.Context, int) ([]store.Redispatch, error) {
	return f.takePending(&f.dueRetries)
}

// takePending hands the waiting runs to the first maintenance pass that asks
// and clears them, the way the real queries only return each run once.
func (f *fakeStore) takePending(pending *[]store.Redispatch) ([]store.Redispatch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rs := *pending
	*pending = nil
	return rs, f.maintenanceErr
}

// setPending queues runs for a later maintenance pass, for tests that check
// the loop is still running after an error.
func (f *fakeStore) setPending(pending *[]store.Redispatch, rs []store.Redispatch) {
	f.mu.Lock()
	defer f.mu.Unlock()
	*pending = rs
}

func (f *fakeStore) results() []finished {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]finished(nil), f.finished...)
}

func (f *fakeStore) failures() []retried {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]retried(nil), f.retries...)
}

type fakeDispatcher struct {
	mu   sync.Mutex
	err  error
	runs []store.Run
}

func (d *fakeDispatcher) Dispatch(_ context.Context, run store.Run, _ string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.runs = append(d.runs, run)
	return d.err
}

func (d *fakeDispatcher) dispatched() []store.Run {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]store.Run(nil), d.runs...)
}

func (d *fakeDispatcher) setErr(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.err = err
}

type fakeConsumer struct {
	msgs chan kafka.Message

	mu        sync.Mutex
	committed []int64
}

func (c *fakeConsumer) FetchMessage(ctx context.Context) (kafka.Message, error) {
	select {
	case m := <-c.msgs:
		return m, nil
	case <-ctx.Done():
		return kafka.Message{}, ctx.Err()
	}
}

func (c *fakeConsumer) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range msgs {
		c.committed = append(c.committed, m.Offset)
	}
	return nil
}

func newTestWorker(t *testing.T, payload string, timeoutSec int) (*Worker, *fakeStore, *fakeDispatcher) {
	t.Helper()
	st := &fakeStore{
		job: store.Job{
			ID: "job-1", Type: "generate_text", Payload: json.RawMessage(payload), TimeoutSec: timeoutSec,
		},
		claimable: map[string]bool{},
	}
	d := &fakeDispatcher{}
	w := New(Config{
		ID:                  "w-test",
		Concurrency:         2,
		HeartbeatInterval:   10 * time.Millisecond,
		StaleAfter:          time.Second,
		MaintenanceInterval: time.Hour,
		SweepAfter:          time.Hour,
		ShutdownGrace:       50 * time.Millisecond,
		RetryBaseDelay:      time.Second,
		RetryMaxDelay:       time.Minute,
		OutputDir:           t.TempDir(),
	}, st, d, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return w, st, d
}

func claimedRun(id string) store.Run {
	return store.Run{ID: id, JobID: "job-1", Status: store.StatusRunning, Attempt: 1}
}

func TestExecuteSucceeds(t *testing.T) {
	w, st, _ := newTestWorker(t, `{"lines": 3}`, 60)

	w.execute(context.Background(), claimedRun("run-1"))

	got := st.results()
	if len(got) != 1 || got[0].status != store.StatusSucceeded || got[0].output != "line 1\nline 2\nline 3\n" {
		t.Fatalf("results = %+v, want one SUCCEEDED run with three lines", got)
	}
	file, err := os.ReadFile(filepath.Join(w.cfg.OutputDir, "run-1.txt"))
	if err != nil || string(file) != got[0].output {
		t.Errorf("output file = %q (err %v), want the run output", file, err)
	}
	var seqs []int64
	for _, batch := range st.appends {
		for _, l := range batch {
			seqs = append(seqs, l.Seq)
		}
	}
	for i, s := range seqs {
		if s != int64(i+1) {
			t.Fatalf("log seqs = %v, want 1..%d in order", seqs, len(seqs))
		}
	}
	if len(seqs) == 0 {
		t.Error("no log lines were written")
	}
}

func TestExecuteFailureSchedulesRetry(t *testing.T) {
	w, st, _ := newTestWorker(t, `{"lines": 5, "fail_at": 2}`, 60)

	w.execute(context.Background(), claimedRun("run-1"))

	got := st.failures()
	if len(got) != 1 || !strings.Contains(got[0].errText, "after 2 lines") || got[0].backoff != time.Second {
		t.Fatalf("failures = %+v, want the job's error with the base 1s backoff", got)
	}
	if len(st.results()) != 0 {
		t.Error("a failed attempt was recorded as finished")
	}
}

func TestExecuteTimesOut(t *testing.T) {
	w, st, _ := newTestWorker(t, `{"lines": 50, "delay_ms": 100}`, 1)
	start := time.Now()

	w.execute(context.Background(), claimedRun("run-1"))

	got := st.failures()
	if len(got) != 1 || got[0].errText != "timed out after 1s" {
		t.Fatalf("failures = %+v, want timed out after 1s", got)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("took %v, want the job stopped at its 1s timeout", elapsed)
	}
}

func TestRetryDelayDoublesUpToCap(t *testing.T) {
	w := &Worker{cfg: Config{RetryBaseDelay: time.Second, RetryMaxDelay: 5 * time.Second}}

	for attempt, want := range map[int]time.Duration{1: 1 * time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 4: 5 * time.Second, 30: 5 * time.Second} {
		if got := w.retryDelay(attempt); got != want {
			t.Errorf("retryDelay(%d) = %v, want %v", attempt, got, want)
		}
	}
}

func TestExecuteStopsWhenRunIsLost(t *testing.T) {
	w, st, _ := newTestWorker(t, `{"lines": 100, "delay_ms": 20}`, 60)
	st.lost = true
	start := time.Now()

	w.execute(context.Background(), claimedRun("run-1"))

	if got, fails := st.results(), st.failures(); len(got) != 0 || len(fails) != 0 {
		t.Errorf("results = %+v, failures = %+v; want nothing recorded by a worker that lost the run", got, fails)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("took %v, want the job stopped at the first failed heartbeat", elapsed)
	}
}

func TestExecuteReleasesRunOnShutdown(t *testing.T) {
	w, st, d := newTestWorker(t, `{"lines": 100, "delay_ms": 20}`, 60)
	jobsCtx, stopJobs := context.WithCancelCause(context.Background())
	time.AfterFunc(50*time.Millisecond, func() { stopJobs(errShutdown) })

	w.execute(jobsCtx, claimedRun("run-1"))

	if len(st.released) != 1 || len(st.results()) != 0 || len(st.failures()) != 0 {
		t.Errorf("released %v, finished %+v, failed %+v; want only a release",
			st.released, st.results(), st.failures())
	}
	if len(d.runs) != 1 || d.runs[0].ID != "run-1" || d.runs[0].Attempt != 2 {
		t.Errorf("dispatched %+v, want run-1 attempt 2 re-dispatched", d.runs)
	}
}

func TestRunClaimsCommitsAndExecutes(t *testing.T) {
	w, st, _ := newTestWorker(t, `{"lines": 2}`, 60)
	st.claimable["run-a"] = true // run-b was already claimed by another worker
	c := &fakeConsumer{msgs: make(chan kafka.Message, 3)}
	c.msgs <- kafka.Message{Offset: 0, Value: []byte(`{"run_id": "run-a"}`)}
	c.msgs <- kafka.Message{Offset: 1, Value: []byte(`{"run_id": "run-b"}`)}
	c.msgs <- kafka.Message{Offset: 2, Value: []byte(`not json`)}

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx, c) }()

	deadline := time.After(5 * time.Second)
	for len(st.results()) == 0 {
		select {
		case <-deadline:
			t.Fatal("run-a was never executed")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	if err := <-errc; err != nil {
		t.Fatalf("Run returned %v", err)
	}

	if got := st.results(); len(got) != 1 || got[0].runID != "run-a" || got[0].status != store.StatusSucceeded {
		t.Errorf("results = %+v, want only run-a SUCCEEDED", got)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.committed) != 3 {
		t.Errorf("committed offsets %v, want all 3 messages committed, claimed or not", c.committed)
	}
}

func TestRunLoggerBatches(t *testing.T) {
	st := &fakeStore{}
	l := startRunLogger(st, "run-1", 1, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Hour, 3)

	for i := 0; i < 7; i++ {
		l.Log("info", "line")
	}
	l.Close()

	var sizes []int
	var seqs []int64
	for _, batch := range st.appends {
		sizes = append(sizes, len(batch))
		for _, line := range batch {
			seqs = append(seqs, line.Seq)
		}
	}
	if len(sizes) != 3 || sizes[0] != 3 || sizes[1] != 3 || sizes[2] != 1 {
		t.Errorf("batch sizes = %v, want [3 3 1]", sizes)
	}
	for i, s := range seqs {
		if s != int64(i+1) {
			t.Fatalf("seqs = %v, want 1..7", seqs)
		}
	}
}
