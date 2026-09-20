// Package scheduler fires cron jobs: it queues a run for every job whose
// next_run_at has passed and dispatches it to the worker pool.
package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/jingxu/jobsys/internal/job"
	"github.com/jingxu/jobsys/internal/store"
)

// Store is the persistence the scheduler needs; *store.DB implements it.
type Store interface {
	FireDueJobs(ctx context.Context, limit int, next func(store.DueJob) (time.Time, error)) ([]store.Redispatch, error)
}

// Dispatcher publishes a run to Kafka; *queue.Publisher implements it.
type Dispatcher interface {
	Dispatch(ctx context.Context, run store.Run, jobType string) error
}

// batch caps how many due jobs one transaction fires.
const batch = 100

// Scheduler fires due cron jobs. Several can run at once, one per API
// replica: the store query uses SKIP LOCKED and a per-occurrence unique key.
type Scheduler struct {
	store      Store
	dispatcher Dispatcher
	log        *slog.Logger
	interval   time.Duration
	now        func() time.Time
}

func New(st Store, d Dispatcher, log *slog.Logger, interval time.Duration) *Scheduler {
	return &Scheduler{store: st, dispatcher: d, log: log, interval: interval, now: time.Now}
}

// Run fires due jobs every interval until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		s.Tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Tick fires every job that is due now.
func (s *Scheduler) Tick(ctx context.Context) {
	for {
		fired, err := s.store.FireDueJobs(ctx, batch, s.next)
		if err != nil && ctx.Err() == nil {
			s.log.Error("firing scheduled jobs", "err", err)
		}
		for _, r := range fired {
			run := store.Run{ID: r.RunID, JobID: r.JobID, Attempt: r.Attempt}
			if err := s.dispatcher.Dispatch(ctx, run, r.JobType); err != nil {
				// The run stays QUEUED; the workers' sweeper re-dispatches it.
				s.log.Warn("dispatch scheduled run failed; the sweeper will retry", "run_id", r.RunID, "err", err)
				continue
			}
			s.log.Info("fired scheduled run", "job_id", r.JobID, "run_id", r.RunID)
		}
		if len(fired) < batch {
			return
		}
	}
}

// next computes a job's following occurrence, after now rather than after the
// occurrence just fired, so a scheduler that was down doesn't replay every
// missed one.
func (s *Scheduler) next(j store.DueJob) (time.Time, error) {
	return job.NextRun(j.CronExpr, j.Timezone, s.now())
}
