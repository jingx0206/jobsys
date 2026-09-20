package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jingxu/jobsys/internal/store"
)

const (
	logFlushInterval = time.Second
	logMaxBatch      = 200
)

// runLogger buffers a run's log lines and writes them in batches, so a job
// printing thousands of lines costs a handful of inserts while a tailing
// client still sees new lines within about a second. It flushes every
// interval, whenever maxBatch lines are waiting, and on Close.
type runLogger struct {
	store    Store
	runID    string
	attempt  int
	log      *slog.Logger
	maxBatch int

	mu  sync.Mutex
	seq int64
	buf []store.LogLine

	stop chan struct{}
	done chan struct{}
}

func newRunLogger(st Store, runID string, attempt int, log *slog.Logger) *runLogger {
	return startRunLogger(st, runID, attempt, log, logFlushInterval, logMaxBatch)
}

func startRunLogger(st Store, runID string, attempt int, log *slog.Logger, interval time.Duration, maxBatch int) *runLogger {
	l := &runLogger{
		store: st, runID: runID, attempt: attempt, log: log, maxBatch: maxBatch,
		stop: make(chan struct{}), done: make(chan struct{}),
	}
	go l.loop(interval)
	return l
}

// Log implements job.Logger.
func (l *runLogger) Log(level, line string) {
	l.mu.Lock()
	l.seq++
	l.buf = append(l.buf, store.LogLine{
		Attempt: l.attempt, Seq: l.seq, TS: time.Now().UTC(), Level: level, Line: line,
	})
	full := len(l.buf) >= l.maxBatch
	l.mu.Unlock()

	if full {
		l.flush()
	}
}

// Close stops the background flusher and writes any remaining lines.
func (l *runLogger) Close() {
	close(l.stop)
	<-l.done
	l.flush()
}

func (l *runLogger) loop(interval time.Duration) {
	defer close(l.done)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			l.flush()
		}
	}
}

func (l *runLogger) flush() {
	l.mu.Lock()
	batch := l.buf
	l.buf = nil
	l.mu.Unlock()
	if len(batch) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()
	if err := l.store.AppendLogs(ctx, l.runID, l.attempt, batch); err != nil {
		// Logs are best effort; the run's status and output are what count.
		l.log.Warn("dropped log lines", "count", len(batch), "err", err)
	}
}
