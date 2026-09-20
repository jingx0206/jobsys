// Package worker claims dispatched runs from Kafka and executes them.
//
// Kafka only says "this run is ready". Postgres decides who runs it: a worker
// executes a run only after its conditional claim succeeds, heartbeats while
// it runs, and stops the moment it learns it no longer owns the run.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/jingxu/jobsys/internal/job"
	"github.com/jingxu/jobsys/internal/queue"
	"github.com/jingxu/jobsys/internal/store"
)

// Store is the persistence the worker needs; *store.DB implements it.
type Store interface {
	ClaimRun(ctx context.Context, runID, workerID string) (store.Run, bool, error)
	GetJob(ctx context.Context, id string) (store.Job, error)
	Heartbeat(ctx context.Context, runID, workerID string, attempt int) (bool, error)
	FinishRun(ctx context.Context, runID, workerID string, attempt int, status string, output, errMsg *string) (bool, error)
	FailOrRetry(ctx context.Context, runID, workerID string, attempt int, errMsg string, backoff time.Duration) (string, error)
	ReleaseRun(ctx context.Context, runID, workerID string, attempt int, reason string) (bool, error)
	AppendLogs(ctx context.Context, runID string, attempt int, lines []store.LogLine) error
	ReapStale(ctx context.Context, staleAfter time.Duration, limit int) ([]store.Redispatch, error)
	SweepQueued(ctx context.Context, olderThan time.Duration, limit int) ([]store.Redispatch, error)
	DueRetries(ctx context.Context, limit int) ([]store.Redispatch, error)
}

// Dispatcher publishes a run to Kafka; *queue.Publisher implements it.
type Dispatcher interface {
	Dispatch(ctx context.Context, run store.Run, jobType string) error
}

// Consumer is the part of *kafka.Reader the worker uses.
type Consumer interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

// Config controls a worker.
type Config struct {
	// ID identifies this worker in job_runs.worker_id. It must be unique.
	ID string
	// Concurrency is how many runs the worker executes at once.
	Concurrency int
	// HeartbeatInterval is how often a running job reports it is alive.
	HeartbeatInterval time.Duration
	// StaleAfter is how long without a heartbeat before the reaper decides a
	// worker died and requeues its run. Keep it several heartbeats long.
	StaleAfter time.Duration
	// MaintenanceInterval is how often the reaper, sweeper and retry
	// dispatcher run.
	MaintenanceInterval time.Duration
	// SweepAfter is how long a run may sit QUEUED before it is re-dispatched.
	SweepAfter time.Duration
	// ShutdownGrace is how long running jobs get to finish on shutdown
	// before they are handed back to the queue.
	ShutdownGrace time.Duration
	// RetryBaseDelay is the backoff after a first failed attempt. It doubles
	// with each further attempt, up to RetryMaxDelay.
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
	// OutputDir, if set, receives a text file of each successful run's output.
	OutputDir string
}

// Worker executes runs dispatched over Kafka.
type Worker struct {
	cfg        Config
	store      Store
	dispatcher Dispatcher
	log        *slog.Logger
}

func New(cfg Config, st Store, d Dispatcher, log *slog.Logger) *Worker {
	return &Worker{cfg: cfg, store: st, dispatcher: d, log: log}
}

var (
	errLostRun  = errors.New("worker no longer owns the run")
	errShutdown = errors.New("worker shutting down")
	errTimeout  = errors.New("job timed out")
)

// writeTimeout bounds database writes made after a job ends. They use their
// own context because they must happen even when the job was cancelled.
const writeTimeout = 10 * time.Second

// maintenanceBatch caps how many runs one maintenance pass handles.
const maintenanceBatch = 100

// Run consumes dispatch messages until ctx is cancelled. It then gives running
// jobs ShutdownGrace to finish and hands any still running back to the queue.
func (w *Worker) Run(ctx context.Context, c Consumer) error {
	jobsCtx, stopJobs := context.WithCancelCause(context.Background())
	defer stopJobs(nil)

	var maint sync.WaitGroup
	maint.Add(1)
	go func() {
		defer maint.Done()
		w.maintain(ctx)
	}()

	var jobs sync.WaitGroup
	err := w.consume(ctx, c, jobsCtx, &jobs)

	done := make(chan struct{})
	go func() {
		jobs.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(w.cfg.ShutdownGrace):
		w.log.Info("shutdown grace period over, handing running jobs back")
		stopJobs(errShutdown)
		<-done
	}
	maint.Wait()
	return err
}

func (w *Worker) consume(ctx context.Context, c Consumer, jobsCtx context.Context, jobs *sync.WaitGroup) error {
	slots := make(chan struct{}, w.cfg.Concurrency)
	for {
		// Take a slot before fetching, so the worker never claims more runs
		// than it can execute right now.
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return nil
		}

		msg, err := c.FetchMessage(ctx)
		if err != nil {
			<-slots
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch dispatch message: %w", err)
		}

		run, ok := w.claim(ctx, msg)
		// Commit whether or not the claim succeeded. Postgres, not the
		// offset, decides whether a run still needs doing, and the sweeper
		// re-dispatches anything left QUEUED.
		if err := c.CommitMessages(ctx, msg); err != nil && ctx.Err() == nil {
			w.log.Warn("commit offset failed", "partition", msg.Partition, "offset", msg.Offset, "err", err)
		}
		if !ok {
			<-slots
			continue
		}

		jobs.Add(1)
		go func() {
			defer jobs.Done()
			defer func() { <-slots }()
			w.execute(jobsCtx, run)
		}()
	}
}

func (w *Worker) claim(ctx context.Context, msg kafka.Message) (store.Run, bool) {
	d, err := queue.DecodeDispatch(msg.Value)
	if err != nil {
		w.log.Warn("skipping malformed dispatch message", "partition", msg.Partition, "offset", msg.Offset, "err", err)
		return store.Run{}, false
	}
	run, ok, err := w.store.ClaimRun(ctx, d.RunID, w.cfg.ID)
	if err != nil {
		w.log.Error("claim failed; the sweeper will re-dispatch the run", "run_id", d.RunID, "err", err)
		return store.Run{}, false
	}
	if !ok {
		w.log.Info("run not claimable (claimed, finished, or retry not yet due), skipping", "run_id", d.RunID)
		return store.Run{}, false
	}
	w.log.Info("claimed run", "run_id", run.ID, "attempt", run.Attempt, "partition", msg.Partition)
	return run, true
}

// execute runs one claimed attempt and records how it ended.
func (w *Worker) execute(jobsCtx context.Context, run store.Run) {
	log := w.log.With("run_id", run.ID, "attempt", run.Attempt)

	loadCtx, cancelLoad := context.WithTimeout(context.Background(), writeTimeout)
	j, err := w.store.GetJob(loadCtx, run.JobID)
	cancelLoad()
	if err != nil {
		w.fail(run, "load job: "+err.Error(), log)
		return
	}

	ctx, cancel := context.WithCancelCause(jobsCtx)
	defer cancel(nil)
	ctx, cancelTimeout := context.WithTimeoutCause(ctx, time.Duration(j.TimeoutSec)*time.Second, errTimeout)
	defer cancelTimeout()

	heartbeats := make(chan struct{})
	go func() {
		defer close(heartbeats)
		w.heartbeat(ctx, run, cancel, log)
	}()

	rl := newRunLogger(w.store, run.ID, run.Attempt, log)
	output, err := job.Run(ctx, j.Type, j.Payload, run.Attempt, rl)
	cause := context.Cause(ctx)
	cancel(nil)
	<-heartbeats
	rl.Close() // flush the log before the status changes

	switch {
	case err == nil:
		w.writeOutputFile(run, output, log)
		w.succeed(run, output, log)
	case errors.Is(cause, errLostRun):
		log.Warn("stopped: another attempt owns this run now")
	case errors.Is(cause, errShutdown):
		w.release(run, j.Type, log)
	case errors.Is(cause, errTimeout):
		w.fail(run, fmt.Sprintf("timed out after %ds", j.TimeoutSec), log)
	default:
		w.fail(run, err.Error(), log)
	}
}

// heartbeat reports the attempt alive until ctx ends. If the store says this
// worker no longer owns the attempt, it cancels the job with errLostRun.
func (w *Worker) heartbeat(ctx context.Context, run store.Run, cancel context.CancelCauseFunc, log *slog.Logger) {
	t := time.NewTicker(w.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		ok, err := w.store.Heartbeat(ctx, run.ID, w.cfg.ID, run.Attempt)
		switch {
		case err != nil && ctx.Err() == nil:
			// Transient: the reaper allows several missed heartbeats.
			log.Warn("heartbeat failed", "err", err)
		case err == nil && !ok:
			cancel(errLostRun)
			return
		}
	}
}

func (w *Worker) succeed(run store.Run, output string, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()

	ok, err := w.store.FinishRun(ctx, run.ID, w.cfg.ID, run.Attempt, store.StatusSucceeded, &output, nil)
	switch {
	case err != nil:
		log.Error("record result failed", "err", err)
	case !ok:
		log.Warn("result discarded: this worker no longer owns the run")
	default:
		log.Info("run finished", "status", store.StatusSucceeded)
	}
}

// fail records a failed attempt. The run is retried after a backoff if the
// job has retries left, and fails otherwise.
func (w *Worker) fail(run store.Run, errMsg string, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()

	backoff := w.retryDelay(run.Attempt)
	status, err := w.store.FailOrRetry(ctx, run.ID, w.cfg.ID, run.Attempt, errMsg, backoff)
	switch {
	case err != nil:
		log.Error("record failure failed", "error", errMsg, "err", err)
	case status == "":
		log.Warn("failure discarded: this worker no longer owns the run", "error", errMsg)
	case status == store.StatusQueued:
		log.Warn("attempt failed, will retry", "error", errMsg, "retry_in", backoff)
	default:
		log.Warn("run finished", "status", status, "error", errMsg)
	}
}

// retryDelay is the backoff before retrying after the given failed attempt:
// RetryBaseDelay, doubled for each earlier attempt, capped at RetryMaxDelay.
func (w *Worker) retryDelay(attempt int) time.Duration {
	d := w.cfg.RetryBaseDelay
	for i := 1; i < attempt && d < w.cfg.RetryMaxDelay; i++ {
		d *= 2
	}
	return min(d, w.cfg.RetryMaxDelay)
}

// release hands an unfinished attempt back to the queue and re-dispatches it,
// so a restarting worker's job moves to another worker without waiting for the
// reaper.
func (w *Worker) release(run store.Run, jobType string, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()

	ok, err := w.store.ReleaseRun(ctx, run.ID, w.cfg.ID, run.Attempt, "released by "+w.cfg.ID+" shutting down")
	if err != nil || !ok {
		log.Warn("could not release run; the reaper will requeue it", "ok", ok, "err", err)
		return
	}
	next := store.Run{ID: run.ID, JobID: run.JobID, Attempt: run.Attempt + 1}
	if err := w.dispatcher.Dispatch(ctx, next, jobType); err != nil {
		log.Warn("released run but could not re-dispatch it; the sweeper will", "err", err)
		return
	}
	log.Info("released run to another worker")
}

func (w *Worker) writeOutputFile(run store.Run, output string, log *slog.Logger) {
	if w.cfg.OutputDir == "" {
		return
	}
	if err := os.MkdirAll(w.cfg.OutputDir, 0o755); err != nil {
		log.Warn("create output dir failed", "err", err)
		return
	}
	path := filepath.Join(w.cfg.OutputDir, run.ID+".txt")
	if err := os.WriteFile(path, []byte(output), 0o644); err != nil {
		log.Warn("write output file failed", "err", err)
		return
	}
	log.Info("wrote output file", "path", path)
}

// maintain periodically requeues runs whose worker died, dispatches retries
// whose backoff has elapsed, and re-dispatches runs whose dispatch message was
// lost. Every worker runs it; the store queries use SKIP LOCKED, so two
// workers never handle the same run.
func (w *Worker) maintain(ctx context.Context) {
	t := time.NewTicker(w.cfg.MaintenanceInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		reaped, err := w.store.ReapStale(ctx, w.cfg.StaleAfter, maintenanceBatch)
		w.logMaintenanceError("reaper", err, ctx)
		for _, r := range reaped {
			if r.Status == store.StatusFailed {
				w.log.Warn("run failed: its worker died and retries are used up", "run_id", r.RunID, "attempt", r.Attempt)
				continue
			}
			w.log.Warn("requeued run from a dead worker", "run_id", r.RunID, "attempt", r.Attempt)
			w.redispatch(ctx, r)
		}

		retries, err := w.store.DueRetries(ctx, maintenanceBatch)
		w.logMaintenanceError("retry dispatcher", err, ctx)
		for _, r := range retries {
			w.log.Info("dispatching retry", "run_id", r.RunID, "attempt", r.Attempt)
			w.redispatch(ctx, r)
		}

		swept, err := w.store.SweepQueued(ctx, w.cfg.SweepAfter, maintenanceBatch)
		w.logMaintenanceError("sweeper", err, ctx)
		for _, r := range swept {
			w.log.Info("re-dispatching run stuck in QUEUED", "run_id", r.RunID, "attempt", r.Attempt)
			w.redispatch(ctx, r)
		}
	}
}

func (w *Worker) logMaintenanceError(what string, err error, ctx context.Context) {
	if err != nil && ctx.Err() == nil {
		w.log.Warn(what+" failed", "err", err)
	}
}

func (w *Worker) redispatch(ctx context.Context, r store.Redispatch) {
	run := store.Run{ID: r.RunID, JobID: r.JobID, Attempt: r.Attempt}
	if err := w.dispatcher.Dispatch(ctx, run, r.JobType); err != nil && ctx.Err() == nil {
		// The run stays QUEUED, so the next sweep tries again.
		w.log.Warn("re-dispatch failed", "run_id", r.RunID, "err", err)
	}
}
