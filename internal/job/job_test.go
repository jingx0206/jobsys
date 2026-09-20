package job

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

type recorder struct {
	lines []string
}

func (r *recorder) Log(level, line string) {
	r.lines = append(r.lines, level+": "+line)
}

func TestGenerateTextOutput(t *testing.T) {
	log := &recorder{}

	out, err := Run(context.Background(), TypeGenerateText, json.RawMessage(`{"lines": 3, "prefix": "row"}`), 1, log)

	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if want := "row 1\nrow 2\nrow 3\n"; out != want {
		t.Errorf("output = %q, want %q", out, want)
	}
	if len(log.lines) == 0 || !strings.Contains(log.lines[len(log.lines)-1], "wrote 3/3 lines") {
		t.Errorf("log = %v, want it to end with progress 3/3", log.lines)
	}
}

func TestGenerateTextDefaultsToNumberedLines(t *testing.T) {
	out, err := Run(context.Background(), TypeGenerateText, json.RawMessage(`{"lines": 2}`), 1, &recorder{})

	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if want := "line 1\nline 2\n"; out != want {
		t.Errorf("output = %q, want %q", out, want)
	}
}

func TestGenerateTextKeepsAUnicodePrefix(t *testing.T) {
	out, err := Run(context.Background(), TypeGenerateText, json.RawMessage(`{"lines": 2, "prefix": "行"}`), 1, &recorder{})

	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if want := "行 1\n行 2\n"; out != want {
		t.Errorf("output = %q, want %q", out, want)
	}
}

func TestGenerateTextFailAtTheLastLine(t *testing.T) {
	// Writing every line and then failing must still be a failure, not a
	// success with complete output.
	out, err := Run(context.Background(), TypeGenerateText, json.RawMessage(`{"lines": 3, "fail_at": 3}`), 1, &recorder{})

	if err == nil || !strings.Contains(err.Error(), "after 3 lines") {
		t.Fatalf("err = %v, want a deliberate failure after 3 lines", err)
	}
	if out != "" {
		t.Errorf("output = %q, want nothing recorded for a failed attempt", out)
	}
}

func TestRunRejectsWhatItCannotExecute(t *testing.T) {
	tests := []struct{ name, jobType, payload string }{
		{"unknown type", "mine_bitcoin", `{}`},
		{"malformed payload", TypeGenerateText, `{"lines":`},
		{"payload out of range", TypeGenerateText, `{"lines": 0}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := Run(context.Background(), tt.jobType, json.RawMessage(tt.payload), 1, &recorder{})

			if err == nil {
				t.Fatalf("Run succeeded with output %q, want an error", out)
			}
			if out != "" {
				t.Errorf("output = %q, want nothing alongside the error", out)
			}
		})
	}
}

func TestGenerateTextLogsBoundedProgress(t *testing.T) {
	log := &recorder{}

	if _, err := Run(context.Background(), TypeGenerateText, json.RawMessage(`{"lines": 10000}`), 1, log); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// One "generating" line plus about 20 progress lines, not 10,000.
	if len(log.lines) > 25 {
		t.Errorf("logged %d lines for a 10,000-line job, want about 21", len(log.lines))
	}
}

func TestGenerateTextFailAt(t *testing.T) {
	log := &recorder{}

	_, err := Run(context.Background(), TypeGenerateText, json.RawMessage(`{"lines": 5, "fail_at": 2}`), 1, log)

	if err == nil || !strings.Contains(err.Error(), "after 2 lines") {
		t.Fatalf("err = %v, want a deliberate failure after 2 lines", err)
	}
	if !strings.HasPrefix(log.lines[len(log.lines)-1], "error:") {
		t.Errorf("last log line = %q, want an error line", log.lines[len(log.lines)-1])
	}
}

func TestGenerateTextFailAttemptsSucceedsOnRetry(t *testing.T) {
	payload := json.RawMessage(`{"lines": 2, "fail_attempts": 2}`)

	for attempt := 1; attempt <= 3; attempt++ {
		_, err := Run(context.Background(), TypeGenerateText, payload, attempt, &recorder{})
		if wantFail := attempt <= 2; (err != nil) != wantFail {
			t.Errorf("attempt %d: err = %v, want failure=%v", attempt, err, wantFail)
		}
	}
}

func TestGenerateTextStopsWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()

	_, err := Run(ctx, TypeGenerateText, json.RawMessage(`{"lines": 100, "delay_ms": 100}`), 1, &recorder{})

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("took %v to stop, want it to stop promptly", elapsed)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name, jobType, payload string
		ok                     bool
	}{
		{"valid", TypeGenerateText, `{"lines": 5}`, true},
		{"fail_at within lines", TypeGenerateText, `{"lines": 5, "fail_at": 5}`, true},
		{"fail_at beyond lines", TypeGenerateText, `{"lines": 5, "fail_at": 6}`, false},
		{"negative fail_at", TypeGenerateText, `{"lines": 5, "fail_at": -1}`, false},
		{"fail_attempts", TypeGenerateText, `{"lines": 5, "fail_attempts": 2}`, true},
		{"the most fail_attempts", TypeGenerateText, `{"lines": 5, "fail_attempts": 10}`, true},
		{"too many fail_attempts", TypeGenerateText, `{"lines": 5, "fail_attempts": 11}`, false},
		{"negative fail_attempts", TypeGenerateText, `{"lines": 5, "fail_attempts": -1}`, false},
		{"one line", TypeGenerateText, `{"lines": 1}`, true},
		{"the most lines", TypeGenerateText, `{"lines": 100000}`, true},
		{"past the most lines", TypeGenerateText, `{"lines": 100001}`, false},
		{"zero lines", TypeGenerateText, `{"lines": 0}`, false},
		{"negative lines", TypeGenerateText, `{"lines": -1}`, false},
		{"no delay", TypeGenerateText, `{"lines": 1, "delay_ms": 0}`, true},
		{"the longest delay", TypeGenerateText, `{"lines": 1, "delay_ms": 10000}`, true},
		{"past the longest delay", TypeGenerateText, `{"lines": 1, "delay_ms": 10001}`, false},
		{"negative delay", TypeGenerateText, `{"lines": 1, "delay_ms": -1}`, false},
		{"unknown field", TypeGenerateText, `{"lines": 1, "font": "mono"}`, false},
		{"malformed json", TypeGenerateText, `{"lines":`, false},
		{"lines as a string", TypeGenerateText, `{"lines": "three"}`, false},
		{"payload is not an object", TypeGenerateText, `[1, 2, 3]`, false},
		{"empty payload", TypeGenerateText, `{}`, false},
		{"unknown type", "mine_bitcoin", `{}`, false},
		{"missing type", "", `{}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(tt.jobType, json.RawMessage(tt.payload))
			if (err == nil) != tt.ok {
				t.Errorf("Validate = %v, want ok=%v", err, tt.ok)
			}
		})
	}
}

func TestNextRun(t *testing.T) {
	// 10:30 UTC is 06:30 in New York (EDT).
	at := time.Date(2026, 9, 17, 10, 30, 0, 500_000_000, time.UTC)
	tests := []struct {
		name, cron, tz string
		want           time.Time
	}{
		{"every 15 minutes", "*/15 * * * *", "UTC", time.Date(2026, 9, 17, 10, 45, 0, 0, time.UTC)},
		{"9am New York", "0 9 * * *", "America/New_York", time.Date(2026, 9, 17, 13, 0, 0, 0, time.UTC)},
		{"every 10 seconds", "@every 10s", "UTC", time.Date(2026, 9, 17, 10, 30, 10, 0, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NextRun(tt.cron, tt.tz, at)
			if err != nil || !got.Equal(tt.want) {
				t.Errorf("NextRun = %v, %v; want %v", got, err, tt.want)
			}
		})
	}

	for _, bad := range []struct{ cron, tz string }{{"every tuesday", "UTC"}, {"* * * * *", "Mars/Olympus"}, {"* * * * *", "Local"}} {
		if _, err := NextRun(bad.cron, bad.tz, at); err == nil {
			t.Errorf("NextRun(%q, %q) succeeded, want an error", bad.cron, bad.tz)
		}
	}
}
