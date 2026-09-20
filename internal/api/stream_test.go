package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jingxu/jobsys/internal/store"
)

// lockedStore makes fakeStore's run and log reads safe while a test changes
// them from another goroutine.
type lockedStore struct {
	*fakeStore
	mu sync.Mutex
}

func (l *lockedStore) GetRun(ctx context.Context, id string) (store.Run, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.fakeStore.GetRun(ctx, id)
}

func (l *lockedStore) ListLogs(ctx context.Context, runID string, attempt int, fromSeq int64, limit int) ([]store.LogLine, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.fakeStore.ListLogs(ctx, runID, attempt, fromSeq, limit)
}

func (l *lockedStore) update(f func(*fakeStore)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	f(l.fakeStore)
}

func newStreamServer() (http.Handler, *lockedStore) {
	st := &lockedStore{fakeStore: newFakeStore()}
	s := NewServer(st, &fakeDispatcher{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.streamPoll = 5 * time.Millisecond
	return s.Routes(), st
}

// events parses a server-sent event stream into "event data" strings, using
// only the line text for line events.
func events(t *testing.T, body string) []string {
	t.Helper()
	var out []string
	for _, block := range strings.Split(strings.TrimSpace(body), "\n\n") {
		var event, data string
		for _, field := range strings.Split(block, "\n") {
			switch {
			case strings.HasPrefix(field, "event: "):
				event = strings.TrimPrefix(field, "event: ")
			case strings.HasPrefix(field, "data: "):
				data = strings.TrimPrefix(field, "data: ")
			}
		}
		if event == "" {
			continue // a comment such as ": ping"
		}
		switch event {
		case "line":
			var l store.LogLine
			if err := json.Unmarshal([]byte(data), &l); err != nil {
				t.Fatalf("bad line event %q: %v", data, err)
			}
			data = l.Line
		case "end":
			var e struct{ Status string }
			json.Unmarshal([]byte(data), &e)
			data = e.Status
		}
		out = append(out, event+" "+data)
	}
	return out
}

func TestStreamLogsFollowsEveryAttempt(t *testing.T) {
	h, st := newStreamServer()
	runID := store.NewID()
	st.update(func(f *fakeStore) {
		f.runs[runID] = store.Run{ID: runID, Status: store.StatusSucceeded, Attempt: 2}
		f.logs[runID] = []store.LogLine{
			{Attempt: 1, Seq: 1, Line: "a1"}, {Attempt: 1, Seq: 2, Line: "a2"},
			{Attempt: 2, Seq: 1, Line: "b1"},
		}
	})

	rec := do(h, http.MethodGet, "/runs/"+runID+"/logs/stream", "")

	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}
	want := []string{`attempt {"attempt":1}`, "line a1", "line a2", `attempt {"attempt":2}`, "line b1", "end SUCCEEDED"}
	if got := events(t, rec.Body.String()); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("events =\n%v\nwant\n%v", got, want)
	}
}

func TestStreamLogsWaitsForALiveRunToFinish(t *testing.T) {
	h, st := newStreamServer()
	runID := store.NewID()
	st.update(func(f *fakeStore) {
		f.runs[runID] = store.Run{ID: runID, Status: store.StatusRunning, Attempt: 1}
		f.logs[runID] = []store.LogLine{{Attempt: 1, Seq: 1, Line: "first"}}
	})
	// The worker writes its last line, then marks the run finished.
	time.AfterFunc(30*time.Millisecond, func() {
		st.update(func(f *fakeStore) {
			f.logs[runID] = append(f.logs[runID], store.LogLine{Attempt: 1, Seq: 2, Line: "last"})
			f.runs[runID] = store.Run{ID: runID, Status: store.StatusFailed, Attempt: 1}
		})
	})

	rec := do(h, http.MethodGet, "/runs/"+runID+"/logs/stream", "")

	want := []string{`attempt {"attempt":1}`, "line first", "line last", "end FAILED"}
	if got := events(t, rec.Body.String()); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("events =\n%v\nwant\n%v", got, want)
	}
}

func TestStreamLogsUnknownRun(t *testing.T) {
	h, _ := newStreamServer()

	if rec := do(h, http.MethodGet, "/runs/"+store.NewID()+"/logs/stream", ""); rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestStreamLogsRejectsBadRequests(t *testing.T) {
	// A bad query has to be refused before the response becomes a stream,
	// because after that there is no status code left to send.
	h, st := newStreamServer()
	runID := store.NewID()
	st.update(func(f *fakeStore) {
		f.runs[runID] = store.Run{ID: runID, Status: store.StatusRunning, Attempt: 1}
	})
	stream := "/runs/" + runID + "/logs/stream"

	tests := []struct {
		name, path string
		want       int
	}{
		{"not a uuid", "/runs/nope/logs/stream", http.StatusBadRequest},
		{"attempt zero", stream + "?attempt=0", http.StatusBadRequest},
		{"an attempt the run has not reached", stream + "?attempt=2", http.StatusBadRequest},
		{"attempt not a number", stream + "?attempt=last", http.StatusBadRequest},
		{"from_seq zero", stream + "?from_seq=0", http.StatusBadRequest},
		{"from_seq not a number", stream + "?from_seq=start", http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(h, http.MethodGet, tt.path, "")
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, tt.want, rec.Body)
			}
			if ct := rec.Header().Get("Content-Type"); strings.HasPrefix(ct, "text/event-stream") {
				t.Errorf("Content-Type = %q, want the request refused rather than streamed", ct)
			}
		})
	}
}

func TestStreamLogsStopsWhenTheClientDisconnects(t *testing.T) {
	// The run never finishes, so only the client going away ends this stream.
	// A stream that outlived its client would hold a connection and keep
	// polling Postgres forever.
	h, st := newStreamServer()
	runID := store.NewID()
	st.update(func(f *fakeStore) {
		f.runs[runID] = store.Run{ID: runID, Status: store.StatusRunning, Attempt: 1}
		f.logs[runID] = []store.LogLine{{Attempt: 1, Seq: 1, Line: "working"}}
	})

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/runs/"+runID+"/logs/stream", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(rec, req)
	}()

	// Let it send the line it has, then hang up.
	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the stream kept running after the client disconnected")
	}
	if got := events(t, rec.Body.String()); len(got) == 0 {
		t.Error("the stream sent nothing before the client disconnected")
	}
}
