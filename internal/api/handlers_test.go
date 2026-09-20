package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jingxu/jobsys/internal/store"
)

// errStore fails every read with a database error rather than ErrNotFound,
// which is the only way to reach the handlers' 500 path.
type errStore struct {
	*fakeStore
	err error
}

func (e *errStore) CreateJob(context.Context, store.NewJob) (store.Job, error) {
	return store.Job{}, e.err
}
func (e *errStore) ListJobs(context.Context, int) ([]store.Job, error)          { return nil, e.err }
func (e *errStore) GetJob(context.Context, string) (store.Job, error)           { return store.Job{}, e.err }
func (e *errStore) GetRun(context.Context, string) (store.Run, error)           { return store.Run{}, e.err }
func (e *errStore) ListRuns(context.Context, *string, int) ([]store.Run, error) { return nil, e.err }

func TestReadyzReportsWhetherPostgresIsReachable(t *testing.T) {
	h, st, _ := newTestServer()

	if rec := do(h, http.MethodGet, "/readyz", ""); rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d while the database is up", rec.Code, http.StatusOK)
	}

	st.pingErr = errors.New("connection refused")

	rec := do(h, http.MethodGet, "/readyz", "")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d so Kubernetes stops routing to this pod",
			rec.Code, http.StatusServiceUnavailable)
	}
	if got := decode[map[string]string](t, rec); got["status"] != "unavailable" {
		t.Errorf("body = %v, want status unavailable", got)
	}
}

func TestHealthzStaysUpWhenPostgresIsDown(t *testing.T) {
	// Liveness must not depend on the database: a pod waiting for Postgres to
	// come back is not a pod Kubernetes should keep restarting.
	h, st, _ := newTestServer()
	st.pingErr = errors.New("connection refused")

	if rec := do(h, http.MethodGet, "/healthz", ""); rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d even with the database down", rec.Code, http.StatusOK)
	}
}

func TestStoreFailuresBecomeInternalErrors(t *testing.T) {
	st := &errStore{fakeStore: newFakeStore(), err: errors.New("connection reset by peer")}
	s := NewServer(st, &fakeDispatcher{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.now = func() time.Time { return fixedNow }
	h := s.Routes()
	id := store.NewID()

	tests := []struct{ name, method, path, body string }{
		{"create job", http.MethodPost, "/jobs", `{"name": "x", "type": "generate_text", "payload": {"lines": 1}}`},
		{"list jobs", http.MethodGet, "/jobs", ""},
		{"get job", http.MethodGet, "/jobs/" + id, ""},
		{"run job", http.MethodPost, "/jobs/" + id + "/run", ""},
		{"pause job", http.MethodPost, "/jobs/" + id + "/pause", ""},
		{"resume job", http.MethodPost, "/jobs/" + id + "/resume", ""},
		{"list runs", http.MethodGet, "/runs", ""},
		{"get run", http.MethodGet, "/runs/" + id, ""},
		{"list logs", http.MethodGet, "/runs/" + id + "/logs", ""},
		{"stream logs", http.MethodGet, "/runs/" + id + "/logs/stream", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(h, tt.method, tt.path, tt.body)

			if rec.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusInternalServerError, rec.Body)
			}
			// The database error itself stays in the log, not the response.
			if body := rec.Body.String(); strings.Contains(body, "connection reset") {
				t.Errorf("body = %s, want the store error kept out of the response", body)
			}
		})
	}
}

func TestCreateJobAcceptsTheLimitsOfItsRanges(t *testing.T) {
	tests := []struct {
		name             string
		field, value     string
		retries, timeout int
	}{
		{"no retries", "max_retries", "0", 0, defaultTimeoutSec},
		{"the most retries", "max_retries", "10", maxMaxRetries, defaultTimeoutSec},
		{"a one second timeout", "timeout_sec", "1", defaultMaxRetries, 1},
		{"a full day timeout", "timeout_sec", "86400", defaultMaxRetries, maxTimeoutSec},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _, _ := newTestServer()
			body := `{"name": "x", "type": "generate_text", "payload": {"lines": 1}, "` +
				tt.field + `": ` + tt.value + `}`

			rec := do(h, http.MethodPost, "/jobs", body)

			if rec.Code != http.StatusCreated {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusCreated, rec.Body)
			}
			if j := decode[store.Job](t, rec); j.MaxRetries != tt.retries || j.TimeoutSec != tt.timeout {
				t.Errorf("job = (retries %d, timeout %d), want (%d, %d)",
					j.MaxRetries, j.TimeoutSec, tt.retries, tt.timeout)
			}
		})
	}
}

func TestCreateJobRejectsAnOversizedBody(t *testing.T) {
	h, st, _ := newTestServer()
	// Two megabytes of prefix, past the one megabyte cap on request bodies.
	body := `{"name": "x", "type": "generate_text", "payload": {"lines": 1, "prefix": "` +
		strings.Repeat("a", 2<<20) + `"}}`

	rec := do(h, http.MethodPost, "/jobs", body)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d; body: %s", rec.Code, http.StatusBadRequest, rec.Body)
	}
	if len(st.jobs) != 0 {
		t.Error("an oversized request still created a job")
	}
}

func TestListLogsRejectsBadRequests(t *testing.T) {
	h, st, _ := newTestServer()
	runID := store.NewID()
	st.runs[runID] = store.Run{ID: runID, Status: store.StatusRunning, Attempt: 2}
	logs := "/runs/" + runID + "/logs"

	tests := []struct {
		name, path string
		want       int
	}{
		{"unknown run", "/runs/" + store.NewID() + "/logs", http.StatusNotFound},
		{"not a uuid", "/runs/nope/logs", http.StatusBadRequest},
		{"attempt zero", logs + "?attempt=0", http.StatusBadRequest},
		{"negative attempt", logs + "?attempt=-1", http.StatusBadRequest},
		{"attempt not a number", logs + "?attempt=two", http.StatusBadRequest},
		{"from_seq zero", logs + "?from_seq=0", http.StatusBadRequest},
		{"from_seq not a number", logs + "?from_seq=start", http.StatusBadRequest},
		{"limit zero", logs + "?limit=0", http.StatusBadRequest},
		{"limit past the cap", logs + "?limit=1001", http.StatusBadRequest},
		{"limit not a number", logs + "?limit=all", http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if rec := do(h, http.MethodGet, tt.path, ""); rec.Code != tt.want {
				t.Errorf("status = %d, want %d; body: %s", rec.Code, tt.want, rec.Body)
			}
		})
	}
}

func TestListRunsRejectsBadFilters(t *testing.T) {
	h, _, _ := newTestServer()

	tests := []struct {
		name, path string
		want       int
	}{
		{"job_id not a uuid", "/runs?job_id=nope", http.StatusBadRequest},
		{"limit zero", "/runs?limit=0", http.StatusBadRequest},
		{"limit past the cap", "/runs?limit=201", http.StatusBadRequest},
		{"limit not a number", "/runs?limit=many", http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if rec := do(h, http.MethodGet, tt.path, ""); rec.Code != tt.want {
				t.Errorf("status = %d, want %d; body: %s", rec.Code, tt.want, rec.Body)
			}
		})
	}
}
