package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jingxu/jobsys/internal/store"
)

// fakeStore is an in-memory Store.
type fakeStore struct {
	jobs map[string]store.Job
	runs map[string]store.Run
	logs map[string][]store.LogLine
	// pingErr makes the store report Postgres as unreachable.
	pingErr error
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		jobs: map[string]store.Job{},
		runs: map[string]store.Run{},
		logs: map[string][]store.LogLine{},
	}
}

func (f *fakeStore) Ping(context.Context) error { return f.pingErr }

func (f *fakeStore) CreateJob(_ context.Context, n store.NewJob) (store.Job, error) {
	j := store.Job{
		ID: store.NewID(), Name: n.Name, Type: n.Type, Payload: n.Payload,
		CronExpr: n.CronExpr, Timezone: n.Timezone, NextRunAt: n.NextRunAt,
		Enabled: true, MaxRetries: n.MaxRetries, TimeoutSec: n.TimeoutSec,
	}
	f.jobs[j.ID] = j
	return j, nil
}

func (f *fakeStore) ListJobs(_ context.Context, limit int) ([]store.Job, error) {
	jobs := []store.Job{}
	for _, j := range f.jobs {
		jobs = append(jobs, j)
	}
	if len(jobs) > limit {
		jobs = jobs[:limit]
	}
	return jobs, nil
}

func (f *fakeStore) GetJob(_ context.Context, id string) (store.Job, error) {
	j, ok := f.jobs[id]
	if !ok {
		return store.Job{}, store.ErrNotFound
	}
	return j, nil
}

func (f *fakeStore) SetJobEnabled(_ context.Context, id string, enabled bool, nextRunAt *time.Time) (store.Job, error) {
	j, ok := f.jobs[id]
	if !ok {
		return store.Job{}, store.ErrNotFound
	}
	j.Enabled, j.NextRunAt = enabled, nextRunAt
	f.jobs[id] = j
	return j, nil
}

func (f *fakeStore) CreateManualRun(_ context.Context, jobID string) (store.Run, error) {
	if _, ok := f.jobs[jobID]; !ok {
		return store.Run{}, store.ErrNotFound
	}
	now := time.Now()
	r := store.Run{
		ID: store.NewID(), JobID: jobID, ScheduledTime: now, Trigger: "manual",
		Status: store.StatusQueued, Attempt: 1, QueuedAt: now,
	}
	f.runs[r.ID] = r
	return r, nil
}

func (f *fakeStore) FailQueuedRun(_ context.Context, runID, reason string) error {
	r, ok := f.runs[runID]
	if ok && r.Status == store.StatusQueued {
		r.Status = store.StatusFailed
		r.Error = &reason
		f.runs[runID] = r
	}
	return nil
}

func (f *fakeStore) GetRun(_ context.Context, id string) (store.Run, error) {
	r, ok := f.runs[id]
	if !ok {
		return store.Run{}, store.ErrNotFound
	}
	return r, nil
}

func (f *fakeStore) ListRuns(_ context.Context, jobID *string, limit int) ([]store.Run, error) {
	runs := []store.Run{}
	for _, r := range f.runs {
		if jobID == nil || r.JobID == *jobID {
			runs = append(runs, r)
		}
	}
	sort.Slice(runs, func(i, j int) bool { return runs[i].ScheduledTime.After(runs[j].ScheduledTime) })
	if len(runs) > limit {
		runs = runs[:limit]
	}
	return runs, nil
}

func (f *fakeStore) ListLogs(_ context.Context, runID string, attempt int, fromSeq int64, limit int) ([]store.LogLine, error) {
	lines := []store.LogLine{}
	for _, l := range f.logs[runID] {
		if l.Attempt == attempt && l.Seq >= fromSeq {
			lines = append(lines, l)
		}
	}
	sort.Slice(lines, func(i, j int) bool { return lines[i].Seq < lines[j].Seq })
	if len(lines) > limit {
		lines = lines[:limit]
	}
	return lines, nil
}

type fakeDispatcher struct {
	err  error
	runs []string
}

func (d *fakeDispatcher) Dispatch(_ context.Context, run store.Run, _ string) error {
	d.runs = append(d.runs, run.ID)
	return d.err
}

// fixedNow is 10:30 UTC, which is 06:30 in New York (EDT).
var fixedNow = time.Date(2026, 9, 17, 10, 30, 0, 0, time.UTC)

func newTestServer() (http.Handler, *fakeStore, *fakeDispatcher) {
	st := newFakeStore()
	d := &fakeDispatcher{}
	s := NewServer(st, d, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.now = func() time.Time { return fixedNow }
	return s.Routes(), st, d
}

func do(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("response is not valid JSON: %v; body: %s", err, rec.Body)
	}
	return v
}

func TestCreateJobSchedulesNextRun(t *testing.T) {
	tests := []struct {
		name     string
		cron, tz string
		want     time.Time
	}{
		{"every six hours UTC", "0 */6 * * *", "", time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)},
		{"9am New York", "0 9 * * *", "America/New_York", time.Date(2026, 9, 17, 13, 0, 0, 0, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _, _ := newTestServer()
			body := `{"name": "report", "type": "generate_text", "payload": {"lines": 5},
				"cron_expr": "` + tt.cron + `", "timezone": "` + tt.tz + `"}`

			rec := do(h, http.MethodPost, "/jobs", body)

			if rec.Code != http.StatusCreated {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusCreated, rec.Body)
			}
			j := decode[store.Job](t, rec)
			if j.NextRunAt == nil || !j.NextRunAt.Equal(tt.want) {
				t.Errorf("next_run_at = %v, want %v", j.NextRunAt, tt.want)
			}
			if j.MaxRetries != defaultMaxRetries || j.TimeoutSec != defaultTimeoutSec {
				t.Errorf("defaults = (%d, %d), want (%d, %d)",
					j.MaxRetries, j.TimeoutSec, defaultMaxRetries, defaultTimeoutSec)
			}
		})
	}
}

func TestCreateManualOnlyJobHasNoNextRun(t *testing.T) {
	h, _, _ := newTestServer()

	rec := do(h, http.MethodPost, "/jobs", `{"name": "adhoc", "type": "generate_text", "payload": {"lines": 3}}`)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d; body: %s", rec.Code, rec.Body)
	}
	if j := decode[store.Job](t, rec); j.CronExpr != nil || j.NextRunAt != nil {
		t.Errorf("manual-only job got a schedule: cron=%v next=%v", j.CronExpr, j.NextRunAt)
	}
}

func TestCreateJobRejectsInvalidInput(t *testing.T) {
	tests := []struct{ name, body string }{
		{"malformed json", `{"name":`},
		{"unknown field", `{"name": "x", "type": "generate_text", "payload": {"lines": 1}, "color": "red"}`},
		{"trailing data", `{"name": "x", "type": "generate_text", "payload": {"lines": 1}} {}`},
		{"missing name", `{"type": "generate_text", "payload": {"lines": 1}}`},
		{"missing type", `{"name": "x", "payload": {"lines": 1}}`},
		{"unknown type", `{"name": "x", "type": "mine_bitcoin", "payload": {}}`},
		{"zero lines", `{"name": "x", "type": "generate_text", "payload": {"lines": 0}}`},
		{"unknown payload field", `{"name": "x", "type": "generate_text", "payload": {"lines": 1, "font": "mono"}}`},
		{"delay too long", `{"name": "x", "type": "generate_text", "payload": {"lines": 1, "delay_ms": 60000}}`},
		{"bad cron", `{"name": "x", "type": "generate_text", "payload": {"lines": 1}, "cron_expr": "every tuesday"}`},
		{"bad timezone", `{"name": "x", "type": "generate_text", "payload": {"lines": 1}, "timezone": "Mars/Olympus"}`},
		{"local timezone", `{"name": "x", "type": "generate_text", "payload": {"lines": 1}, "timezone": "Local"}`},
		{"negative retries", `{"name": "x", "type": "generate_text", "payload": {"lines": 1}, "max_retries": -1}`},
		{"too many retries", `{"name": "x", "type": "generate_text", "payload": {"lines": 1}, "max_retries": 11}`},
		{"zero timeout", `{"name": "x", "type": "generate_text", "payload": {"lines": 1}, "timeout_sec": 0}`},
		{"negative timeout", `{"name": "x", "type": "generate_text", "payload": {"lines": 1}, "timeout_sec": -60}`},
		{"timeout past a day", `{"name": "x", "type": "generate_text", "payload": {"lines": 1}, "timeout_sec": 86401}`},
		{"blank name", `{"name": "   ", "type": "generate_text", "payload": {"lines": 1}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, st, _ := newTestServer()

			rec := do(h, http.MethodPost, "/jobs", tt.body)

			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want %d; body: %s", rec.Code, http.StatusBadRequest, rec.Body)
			}
			if len(st.jobs) != 0 {
				t.Errorf("invalid job was stored")
			}
		})
	}
}

func TestGetJob(t *testing.T) {
	h, _, _ := newTestServer()
	created := decode[store.Job](t, do(h, http.MethodPost, "/jobs",
		`{"name": "x", "type": "generate_text", "payload": {"lines": 1}}`))

	tests := []struct {
		name string
		path string
		want int
	}{
		{"existing", "/jobs/" + created.ID, http.StatusOK},
		{"missing", "/jobs/" + store.NewID(), http.StatusNotFound},
		{"not a uuid", "/jobs/nope", http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if rec := do(h, http.MethodGet, tt.path, ""); rec.Code != tt.want {
				t.Errorf("status = %d, want %d; body: %s", rec.Code, tt.want, rec.Body)
			}
		})
	}
}

func TestListJobsRejectsBadLimit(t *testing.T) {
	h, _, _ := newTestServer()

	if rec := do(h, http.MethodGet, "/jobs?limit=0", ""); rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestRunJobQueuesAndDispatches(t *testing.T) {
	h, st, d := newTestServer()
	j := decode[store.Job](t, do(h, http.MethodPost, "/jobs",
		`{"name": "x", "type": "generate_text", "payload": {"lines": 1}}`))

	rec := do(h, http.MethodPost, "/jobs/"+j.ID+"/run", "")

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusAccepted, rec.Body)
	}
	run := decode[store.Run](t, rec)
	if run.Status != store.StatusQueued || run.JobID != j.ID || run.Trigger != "manual" {
		t.Errorf("run = %+v, want a QUEUED manual run of job %s", run, j.ID)
	}
	if len(d.runs) != 1 || d.runs[0] != run.ID {
		t.Errorf("dispatched %v, want [%s]", d.runs, run.ID)
	}
	if _, ok := st.runs[run.ID]; !ok {
		t.Errorf("run was not stored")
	}
}

func TestRunJobDispatchFailureFailsRun(t *testing.T) {
	h, st, d := newTestServer()
	d.err = errors.New("broker unreachable")
	j := decode[store.Job](t, do(h, http.MethodPost, "/jobs",
		`{"name": "x", "type": "generate_text", "payload": {"lines": 1}}`))

	rec := do(h, http.MethodPost, "/jobs/"+j.ID+"/run", "")

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadGateway)
	}
	if len(d.runs) != 1 {
		t.Fatalf("dispatched %d runs, want 1", len(d.runs))
	}
	if run := st.runs[d.runs[0]]; run.Status != store.StatusFailed {
		t.Errorf("run status = %s, want %s so it is not stranded as QUEUED", run.Status, store.StatusFailed)
	}
}

func TestRunMissingJob(t *testing.T) {
	h, _, d := newTestServer()

	rec := do(h, http.MethodPost, "/jobs/"+store.NewID()+"/run", "")

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	if len(d.runs) != 0 {
		t.Errorf("dispatched a run for a missing job")
	}
}

func TestListLogs(t *testing.T) {
	h, st, _ := newTestServer()
	runID := store.NewID()
	st.runs[runID] = store.Run{ID: runID, Status: store.StatusRunning, Attempt: 2}
	for _, l := range []store.LogLine{
		{Attempt: 1, Seq: 1, Line: "a1"}, {Attempt: 1, Seq: 2, Line: "a2"},
		{Attempt: 2, Seq: 1, Line: "b1"}, {Attempt: 2, Seq: 2, Line: "b2"}, {Attempt: 2, Seq: 3, Line: "b3"},
	} {
		st.logs[runID] = append(st.logs[runID], l)
	}

	tests := []struct {
		name        string
		query       string
		wantAttempt int
		wantLines   []string
		wantNext    int64
	}{
		{"defaults to current attempt", "", 2, []string{"b1", "b2", "b3"}, 4},
		{"from_seq resumes", "?from_seq=3", 2, []string{"b3"}, 4},
		{"limit pages", "?limit=2", 2, []string{"b1", "b2"}, 3},
		{"earlier attempt", "?attempt=1", 1, []string{"a1", "a2"}, 3},
		{"past the end", "?from_seq=9", 2, []string{}, 9},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(h, http.MethodGet, "/runs/"+runID+"/logs"+tt.query, "")
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d; body: %s", rec.Code, rec.Body)
			}
			resp := decode[logsResponse](t, rec)
			got := []string{}
			for _, l := range resp.Lines {
				got = append(got, l.Line)
			}
			if resp.Attempt != tt.wantAttempt || resp.NextSeq != tt.wantNext ||
				strings.Join(got, ",") != strings.Join(tt.wantLines, ",") {
				t.Errorf("got attempt=%d lines=%v next=%d, want attempt=%d lines=%v next=%d",
					resp.Attempt, got, resp.NextSeq, tt.wantAttempt, tt.wantLines, tt.wantNext)
			}
			if resp.Status != store.StatusRunning {
				t.Errorf("status = %s, want %s", resp.Status, store.StatusRunning)
			}
		})
	}

	if rec := do(h, http.MethodGet, "/runs/"+runID+"/logs?attempt=3", ""); rec.Code != http.StatusBadRequest {
		t.Errorf("future attempt: status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}
