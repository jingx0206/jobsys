package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/jingxu/jobsys/internal/job"
	"github.com/jingxu/jobsys/internal/store"
)

const (
	defaultMaxRetries = 3
	maxMaxRetries     = 10
	defaultTimeoutSec = 300
	maxTimeoutSec     = 24 * 60 * 60
)

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReadyz reports whether the API can reach Postgres, so Kubernetes only
// routes traffic to pods that can serve it.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		s.log.Warn("readiness check failed", "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type createJobRequest struct {
	Name       string          `json:"name"`
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	CronExpr   string          `json:"cron_expr"`
	Timezone   string          `json:"timezone"`
	MaxRetries *int            `json:"max_retries"`
	TimeoutSec *int            `json:"timeout_sec"`
}

func (s *Server) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	n, err := s.newJob(req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	j, err := s.store.CreateJob(r.Context(), n)
	if err != nil {
		s.storeError(w, r, "job", err)
		return
	}
	writeJSON(w, http.StatusCreated, j)
}

// newJob validates a create request and fills in defaults. For scheduled jobs
// it computes the first run time in the job's time zone.
func (s *Server) newJob(req createJobRequest) (store.NewJob, error) {
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return store.NewJob{}, errors.New("name is required")
	}
	if len(req.Payload) == 0 {
		req.Payload = json.RawMessage(`{}`)
	}
	if err := job.Validate(req.Type, req.Payload); err != nil {
		return store.NewJob{}, err
	}

	n := store.NewJob{
		Name:       name,
		Type:       req.Type,
		Payload:    req.Payload,
		Timezone:   "UTC",
		MaxRetries: defaultMaxRetries,
		TimeoutSec: defaultTimeoutSec,
	}
	if req.Timezone != "" {
		n.Timezone = req.Timezone
	}
	if _, err := time.LoadLocation(n.Timezone); err != nil || n.Timezone == "Local" {
		return store.NewJob{}, fmt.Errorf("unknown timezone %q", n.Timezone)
	}

	if expr := strings.TrimSpace(req.CronExpr); expr != "" {
		next, err := job.NextRun(expr, n.Timezone, s.now())
		if err != nil {
			return store.NewJob{}, err
		}
		n.CronExpr = &expr
		n.NextRunAt = &next
	}

	if req.MaxRetries != nil {
		if *req.MaxRetries < 0 || *req.MaxRetries > maxMaxRetries {
			return store.NewJob{}, fmt.Errorf("max_retries must be between 0 and %d", maxMaxRetries)
		}
		n.MaxRetries = *req.MaxRetries
	}
	if req.TimeoutSec != nil {
		if *req.TimeoutSec < 1 || *req.TimeoutSec > maxTimeoutSec {
			return store.NewJob{}, fmt.Errorf("timeout_sec must be between 1 and %d", maxTimeoutSec)
		}
		n.TimeoutSec = *req.TimeoutSec
	}
	return n, nil
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	limit, err := queryInt(r, "limit", 50, 1, 200)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	jobs, err := s.store.ListJobs(r.Context(), limit)
	if err != nil {
		s.storeError(w, r, "jobs", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	j, err := s.store.GetJob(r.Context(), id)
	if err != nil {
		s.storeError(w, r, "job", err)
		return
	}
	writeJSON(w, http.StatusOK, j)
}

// handleSetEnabled pauses or resumes a job's schedule. Resuming schedules the
// next occurrence after now, so a job paused for a week doesn't fire once for
// every occurrence it missed. Manual runs work either way.
func (s *Server) handleSetEnabled(enabled bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathID(w, r)
		if !ok {
			return
		}
		j, err := s.store.GetJob(r.Context(), id)
		if err != nil {
			s.storeError(w, r, "job", err)
			return
		}

		var next *time.Time
		if enabled && j.CronExpr != nil {
			t, err := job.NextRun(*j.CronExpr, j.Timezone, s.now())
			if err != nil {
				s.storeError(w, r, "job", fmt.Errorf("compute next run: %w", err))
				return
			}
			next = &t
		}
		if j, err = s.store.SetJobEnabled(r.Context(), id, enabled, next); err != nil {
			s.storeError(w, r, "job", err)
			return
		}
		writeJSON(w, http.StatusOK, j)
	}
}

// handleRunJob queues a manual run and dispatches it to the worker pool.
func (s *Server) handleRunJob(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	ctx := r.Context()

	j, err := s.store.GetJob(ctx, id)
	if err != nil {
		s.storeError(w, r, "job", err)
		return
	}
	run, err := s.store.CreateManualRun(ctx, j.ID)
	if err != nil {
		s.storeError(w, r, "job", err)
		return
	}

	if err := s.dispatcher.Dispatch(ctx, run, j.Type); err != nil {
		s.log.Error("dispatch failed", "run_id", run.ID, "err", err)
		// Fail the run so it isn't left QUEUED with no message to wake a
		// worker. Use a context that survives the client disconnecting.
		failCtx := context.WithoutCancel(ctx)
		if ferr := s.store.FailQueuedRun(failCtx, run.ID, "dispatch failed: "+err.Error()); ferr != nil {
			s.log.Error("mark undispatched run failed", "run_id", run.ID, "err", ferr)
		}
		writeError(w, http.StatusBadGateway, "could not dispatch run")
		return
	}
	writeJSON(w, http.StatusAccepted, run)
}

// handleListRuns lists recent runs, newest first, optionally for one job.
func (s *Server) handleListRuns(w http.ResponseWriter, r *http.Request) {
	limit, err := queryInt(r, "limit", 50, 1, 200)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var jobID *string
	if raw := r.URL.Query().Get("job_id"); raw != "" {
		u, err := uuid.Parse(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid job_id")
			return
		}
		id := u.String()
		jobID = &id
	}
	runs, err := s.store.ListRuns(r.Context(), jobID, limit)
	if err != nil {
		s.storeError(w, r, "runs", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs})
}

func (s *Server) handleGetRun(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	run, err := s.store.GetRun(r.Context(), id)
	if err != nil {
		s.storeError(w, r, "run", err)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

type logsResponse struct {
	RunID   string `json:"run_id"`
	Attempt int    `json:"attempt"`
	// Status lets a tailing client stop polling once the run has finished
	// and it has read every line.
	Status  string          `json:"status"`
	Lines   []store.LogLine `json:"lines"`
	NextSeq int64           `json:"next_seq"`
}

// handleListLogs pages through the log lines of one attempt of a run,
// defaulting to the run's current attempt. Pass next_seq back as from_seq to
// fetch the following page.
func (s *Server) handleListLogs(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	run, err := s.store.GetRun(r.Context(), id)
	if err != nil {
		s.storeError(w, r, "run", err)
		return
	}

	attempt, err := queryInt(r, "attempt", run.Attempt, 1, run.Attempt)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	fromSeq, err := queryInt(r, "from_seq", 1, 1, math.MaxInt)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	limit, err := queryInt(r, "limit", 500, 1, 1000)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	lines, err := s.store.ListLogs(r.Context(), run.ID, attempt, int64(fromSeq), limit)
	if err != nil {
		s.storeError(w, r, "run", err)
		return
	}
	next := int64(fromSeq)
	if len(lines) > 0 {
		next = lines[len(lines)-1].Seq + 1
	}
	writeJSON(w, http.StatusOK, logsResponse{
		RunID:   run.ID,
		Attempt: attempt,
		Status:  run.Status,
		Lines:   lines,
		NextSeq: next,
	})
}
