package api

import (
	"net/http"
	"testing"
	"time"

	"github.com/jingxu/jobsys/internal/store"
)

func TestPauseAndResumeSchedule(t *testing.T) {
	h, _, _ := newTestServer()
	j := decode[store.Job](t, do(h, http.MethodPost, "/jobs",
		`{"name": "tick", "type": "generate_text", "payload": {"lines": 1}, "cron_expr": "*/15 * * * *"}`))

	paused := decode[store.Job](t, do(h, http.MethodPost, "/jobs/"+j.ID+"/pause", ""))
	if paused.Enabled || paused.NextRunAt != nil {
		t.Errorf("paused job: enabled=%v next_run_at=%v, want disabled with no next run", paused.Enabled, paused.NextRunAt)
	}

	resumed := decode[store.Job](t, do(h, http.MethodPost, "/jobs/"+j.ID+"/resume", ""))
	// fixedNow is 10:30, so the next quarter hour is 10:45.
	want := time.Date(2026, 9, 17, 10, 45, 0, 0, time.UTC)
	if !resumed.Enabled || resumed.NextRunAt == nil || !resumed.NextRunAt.Equal(want) {
		t.Errorf("resumed job: enabled=%v next_run_at=%v, want enabled with next run %v", resumed.Enabled, resumed.NextRunAt, want)
	}

	if rec := do(h, http.MethodPost, "/jobs/"+store.NewID()+"/pause", ""); rec.Code != http.StatusNotFound {
		t.Errorf("pause missing job: status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}
