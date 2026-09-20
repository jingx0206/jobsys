package scheduler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jingxu/jobsys/internal/store"
)

// fakeStore returns due as fired runs on the first call, recording what next
// computed for each, and nothing afterwards. Set batches instead to hand Tick
// one prepared batch per call, or err to fail every call.
type fakeStore struct {
	due     []store.DueJob
	batches [][]store.Redispatch
	err     error
	calls   int
	nexts   map[string]time.Time
}

func (f *fakeStore) FireDueJobs(_ context.Context, _ int, next func(store.DueJob) (time.Time, error)) ([]store.Redispatch, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	if f.batches != nil {
		if f.calls > len(f.batches) {
			return nil, nil
		}
		return f.batches[f.calls-1], nil
	}
	if f.calls > 1 {
		return nil, nil
	}
	f.nexts = map[string]time.Time{}
	var fired []store.Redispatch
	for _, d := range f.due {
		t, err := next(d)
		if err != nil {
			return nil, err
		}
		f.nexts[d.ID] = t
		fired = append(fired, store.Redispatch{RunID: "run-" + d.ID, JobID: d.ID, JobType: d.Type, Status: store.StatusQueued, Attempt: 1})
	}
	return fired, nil
}

type fakeDispatcher struct {
	err  error
	runs []store.Run
}

func (d *fakeDispatcher) Dispatch(_ context.Context, run store.Run, _ string) error {
	d.runs = append(d.runs, run)
	return d.err
}

func TestTickFiresAndDispatchesDueJobs(t *testing.T) {
	st := &fakeStore{due: []store.DueJob{
		{ID: "a", Type: "generate_text", CronExpr: "*/15 * * * *", Timezone: "UTC"},
		{ID: "b", Type: "generate_text", CronExpr: "0 9 * * *", Timezone: "America/New_York"},
	}}
	d := &fakeDispatcher{}
	s := New(st, d, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Second)
	s.now = func() time.Time { return time.Date(2026, 9, 17, 10, 7, 0, 0, time.UTC) }

	s.Tick(context.Background())

	if len(d.runs) != 2 || d.runs[0].ID != "run-a" || d.runs[1].ID != "run-b" {
		t.Errorf("dispatched %+v, want run-a and run-b", d.runs)
	}
	// The next occurrence is computed from now, in each job's own zone.
	if want := time.Date(2026, 9, 17, 10, 15, 0, 0, time.UTC); !st.nexts["a"].Equal(want) {
		t.Errorf("next for a = %v, want %v", st.nexts["a"], want)
	}
	if want := time.Date(2026, 9, 17, 13, 0, 0, 0, time.UTC); !st.nexts["b"].Equal(want) {
		t.Errorf("next for b = %v, want 9am New York = %v", st.nexts["b"], want)
	}
}

func TestTickSurvivesDispatchFailure(t *testing.T) {
	st := &fakeStore{due: []store.DueJob{{ID: "a", Type: "generate_text", CronExpr: "@every 10s", Timezone: "UTC"}}}
	d := &fakeDispatcher{err: errors.New("kafka down")}
	s := New(st, d, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Second)

	s.Tick(context.Background()) // must not panic or loop

	if len(d.runs) != 1 {
		t.Errorf("attempted %d dispatches, want 1; the sweeper handles the rest", len(d.runs))
	}
}

func TestTickDrainsABacklogOfDueJobs(t *testing.T) {
	// A full batch means there may be more jobs already due. Waiting for the
	// next interval would let the backlog grow faster than it is worked off,
	// so Tick goes back for another batch until one comes back short.
	full := make([]store.Redispatch, batch)
	for i := range full {
		full[i] = store.Redispatch{
			RunID: fmt.Sprintf("run-%d", i), JobID: "a", JobType: "generate_text",
			Status: store.StatusQueued, Attempt: 1,
		}
	}
	last := []store.Redispatch{{RunID: "run-last", JobID: "a", JobType: "generate_text", Status: store.StatusQueued, Attempt: 1}}
	st := &fakeStore{batches: [][]store.Redispatch{full, last}}
	d := &fakeDispatcher{}
	s := New(st, d, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Second)

	s.Tick(context.Background())

	if st.calls != 2 {
		t.Errorf("FireDueJobs called %d times, want 2: a full batch, then a short one that ends the pass", st.calls)
	}
	if len(d.runs) != batch+1 {
		t.Errorf("dispatched %d runs, want all %d of them", len(d.runs), batch+1)
	}
	if len(d.runs) > 0 && d.runs[len(d.runs)-1].ID != "run-last" {
		t.Errorf("last dispatch = %s, want run-last from the second batch", d.runs[len(d.runs)-1].ID)
	}
}

func TestTickGivesUpOnAStoreError(t *testing.T) {
	// The API keeps serving when the scheduler cannot read the database; the
	// next tick tries again. Retrying inside one tick would spin.
	st := &fakeStore{err: errors.New("database down")}
	d := &fakeDispatcher{}
	s := New(st, d, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Second)

	s.Tick(context.Background())

	if st.calls != 1 {
		t.Errorf("FireDueJobs called %d times, want 1; a failing tick must not loop", st.calls)
	}
	if len(d.runs) != 0 {
		t.Errorf("dispatched %+v, want nothing when nothing was fired", d.runs)
	}
}

func TestRunTicksUntilCancelled(t *testing.T) {
	st := &fakeStore{due: []store.DueJob{{ID: "a", Type: "generate_text", CronExpr: "@every 10s", Timezone: "UTC"}}}
	s := New(st, &fakeDispatcher{}, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.Run(ctx) }()
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run kept going after its context was cancelled")
	}
	if st.calls == 0 {
		t.Error("Run returned without firing due jobs even once")
	}
}
