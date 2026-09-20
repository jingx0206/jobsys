package api

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/jingxu/jobsys/internal/store"
)

const (
	// streamBatch caps how many lines one query reads.
	streamBatch = 500
	// streamPing keeps an idle stream alive through proxies and port-forwards.
	streamPing = 15 * time.Second
)

func isFinal(status string) bool {
	switch status {
	case store.StatusSucceeded, store.StatusFailed, store.StatusCancelled:
		return true
	}
	return false
}

// handleStreamLogs follows a run's log as server-sent events until the run
// finishes:
//
//	event: attempt   data: {"attempt": 2}           lines of this attempt follow
//	event: line      data: a store.LogLine          one log line
//	event: end       data: {"status": "SUCCEEDED"}  the run finished; stream closes
//
// It starts at attempt 1 unless ?attempt= says otherwise, so a client sees
// every attempt of a retried run. It polls Postgres, where workers write
// their logs, so any API replica can serve any stream.
func (s *Server) handleStreamLogs(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	ctx := r.Context()
	run, err := s.store.GetRun(ctx, id)
	if err != nil {
		s.storeError(w, r, "run", err)
		return
	}
	attempt, err := queryInt(r, "attempt", 1, 1, run.Attempt)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	fromSeq, err := queryInt(r, "from_seq", 1, 1, math.MaxInt)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)

	lastWrite := time.Now()
	send := func(event string, v any) bool {
		data, err := json.Marshal(v)
		if err != nil {
			return false
		}
		lastWrite = time.Now()
		_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
		return err == nil
	}

	next := int64(fromSeq)
	if !send("attempt", map[string]int{"attempt": attempt}) {
		return
	}
	poll := time.NewTicker(s.streamPoll)
	defer poll.Stop()

	for {
		// Read the run before its lines. Workers write an attempt's lines
		// before changing its status, so once this snapshot shows the attempt
		// is over, the lines read next are all of them.
		if run, err = s.store.GetRun(ctx, id); err != nil {
			s.streamError(ctx, id, err)
			return
		}
		for {
			lines, err := s.store.ListLogs(ctx, id, attempt, next, streamBatch)
			if err != nil {
				s.streamError(ctx, id, err)
				return
			}
			for _, l := range lines {
				if !send("line", l) {
					return
				}
				next = l.Seq + 1
			}
			if len(lines) < streamBatch {
				break
			}
		}

		if attempt < run.Attempt {
			attempt++
			next = 1
			if !send("attempt", map[string]int{"attempt": attempt}) {
				return
			}
			continue
		}
		if isFinal(run.Status) {
			send("end", map[string]any{"status": run.Status, "attempt": run.Attempt, "error": run.Error})
			rc.Flush()
			return
		}

		if time.Since(lastWrite) >= streamPing {
			fmt.Fprint(w, ": ping\n\n")
			lastWrite = time.Now()
		}
		if err := rc.Flush(); err != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-poll.C:
		}
	}
}

func (s *Server) streamError(ctx context.Context, runID string, err error) {
	if ctx.Err() == nil {
		s.log.Error("log stream failed", "run_id", runID, "err", err)
	}
}
