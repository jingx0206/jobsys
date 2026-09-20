package api

import (
	"net/http"
	"strings"
	"testing"

	"github.com/jingxu/jobsys/internal/store"
)

func TestListRuns(t *testing.T) {
	h, _, _ := newTestServer()
	create := func(name string) store.Job {
		return decode[store.Job](t, do(h, http.MethodPost, "/jobs",
			`{"name": "`+name+`", "type": "generate_text", "payload": {"lines": 1}}`))
	}
	a, b := create("a"), create("b")
	for _, id := range []string{a.ID, a.ID, b.ID} {
		do(h, http.MethodPost, "/jobs/"+id+"/run", "")
	}

	tests := []struct {
		name  string
		query string
		want  int
	}{
		{"all runs", "", 3},
		{"one job", "?job_id=" + a.ID, 2},
		{"limit", "?limit=1", 1},
		{"job with no runs", "?job_id=" + store.NewID(), 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(h, http.MethodGet, "/runs"+tt.query, "")
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d; body: %s", rec.Code, rec.Body)
			}
			if got := decode[struct{ Runs []store.Run }](t, rec).Runs; len(got) != tt.want {
				t.Errorf("got %d runs, want %d", len(got), tt.want)
			}
		})
	}

	if rec := do(h, http.MethodGet, "/runs?job_id=nope", ""); rec.Code != http.StatusBadRequest {
		t.Errorf("bad job_id: status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestServesWebUI(t *testing.T) {
	h, _, _ := newTestServer()

	tests := []struct {
		path, contains string
	}{
		{"/", "<title>jobsys</title>"},
		{"/app.js", "EventSource"},
		{"/style.css", "--succeeded"},
	}
	for _, tt := range tests {
		rec := do(h, http.MethodGet, tt.path, "")
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), tt.contains) {
			t.Errorf("GET %s: status %d, want 200 with %q", tt.path, rec.Code, tt.contains)
		}
	}

	// API routes still win over the file server.
	if rec := do(h, http.MethodGet, "/jobs", ""); !strings.Contains(rec.Body.String(), `"jobs"`) {
		t.Errorf("GET /jobs no longer reaches the API: %s", rec.Body)
	}
}
