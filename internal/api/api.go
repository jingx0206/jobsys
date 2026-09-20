// Package api holds the HTTP handlers and routing for the job API.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/jingxu/jobsys/internal/store"
	"github.com/jingxu/jobsys/web"
)

// Store is the persistence the API needs. *store.DB implements it; tests
// substitute an in-memory fake.
type Store interface {
	Ping(ctx context.Context) error
	CreateJob(ctx context.Context, n store.NewJob) (store.Job, error)
	ListJobs(ctx context.Context, limit int) ([]store.Job, error)
	GetJob(ctx context.Context, id string) (store.Job, error)
	SetJobEnabled(ctx context.Context, id string, enabled bool, nextRunAt *time.Time) (store.Job, error)
	CreateManualRun(ctx context.Context, jobID string) (store.Run, error)
	FailQueuedRun(ctx context.Context, runID, reason string) error
	GetRun(ctx context.Context, id string) (store.Run, error)
	ListRuns(ctx context.Context, jobID *string, limit int) ([]store.Run, error)
	ListLogs(ctx context.Context, runID string, attempt int, fromSeq int64, limit int) ([]store.LogLine, error)
}

// Dispatcher hands a queued run to the worker pool.
type Dispatcher interface {
	Dispatch(ctx context.Context, run store.Run, jobType string) error
}

// Server serves the job API.
type Server struct {
	store      Store
	dispatcher Dispatcher
	log        *slog.Logger
	now        func() time.Time
	// streamPoll is how often a log stream checks Postgres for new lines.
	streamPoll time.Duration
}

func NewServer(st Store, d Dispatcher, log *slog.Logger) *Server {
	return &Server{store: st, dispatcher: d, log: log, now: time.Now, streamPoll: time.Second}
}

// Routes returns the API's HTTP handler.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.HandleFunc("POST /jobs", s.handleCreateJob)
	mux.HandleFunc("GET /jobs", s.handleListJobs)
	mux.HandleFunc("GET /jobs/{id}", s.handleGetJob)
	mux.HandleFunc("POST /jobs/{id}/run", s.handleRunJob)
	mux.HandleFunc("POST /jobs/{id}/pause", s.handleSetEnabled(false))
	mux.HandleFunc("POST /jobs/{id}/resume", s.handleSetEnabled(true))
	mux.HandleFunc("GET /runs", s.handleListRuns)
	mux.HandleFunc("GET /runs/{id}", s.handleGetRun)
	mux.HandleFunc("GET /runs/{id}/logs", s.handleListLogs)
	mux.HandleFunc("GET /runs/{id}/logs/stream", s.handleStreamLogs)
	// The web UI, at / and anything the API routes above don't claim.
	mux.Handle("GET /", http.FileServerFS(web.FS))
	return s.logRequests(mux)
}

const maxBodyBytes = 1 << 20

// decodeJSON reads a single JSON object from the request body, rejecting
// unknown fields and trailing data.
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	if dec.More() {
		return errors.New("invalid JSON body: unexpected data after object")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// storeError writes 404 for store.ErrNotFound and 500 for anything else.
func (s *Server) storeError(w http.ResponseWriter, r *http.Request, what string, err error) {
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, what+" not found")
		return
	}
	s.log.Error("store error", "method", r.Method, "path", r.URL.Path, "err", err)
	writeError(w, http.StatusInternalServerError, "internal error")
}

// pathID returns the {id} path segment in canonical UUID form, writing 400
// if it is not a UUID.
func pathID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return "", false
	}
	return id.String(), true
}

// queryInt reads an integer query parameter, returning def when it is absent
// and an error when it is not an integer in [lo, hi].
func queryInt(r *http.Request, name string, def, lo, hi int) (int, error) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < lo || n > hi {
		return 0, fmt.Errorf("%s must be an integer between %d and %d", name, lo, hi)
	}
	return n, nil
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (rec *statusRecorder) WriteHeader(status int) {
	rec.status = status
	rec.ResponseWriter.WriteHeader(status)
}

// Unwrap lets http.ResponseController reach the underlying writer, so the log
// stream can flush through this middleware.
func (rec *statusRecorder) Unwrap() http.ResponseWriter {
	return rec.ResponseWriter
}

// logRequests logs every request except health probes, which Kubernetes
// sends every few seconds.
func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
			return
		}
		s.log.Info("request", "method", r.Method, "path", r.URL.Path,
			"status", rec.status, "duration", time.Since(start))
	})
}
