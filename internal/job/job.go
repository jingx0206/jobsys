// Package job defines job types, their payloads and executors, and cron
// schedule evaluation.
package job

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

// TypeGenerateText produces a text file of numbered lines.
const TypeGenerateText = "generate_text"

// GenerateText is the payload of a generate_text job.
type GenerateText struct {
	// Lines is how many lines to write.
	Lines int `json:"lines"`
	// Prefix starts every line; defaults to "line".
	Prefix string `json:"prefix,omitempty"`
	// DelayMs pauses after each line, so a run stays RUNNING long enough to
	// watch it, or to kill its worker mid-run.
	DelayMs int `json:"delay_ms,omitempty"`
	// FailAt makes every attempt fail after writing this many lines, to
	// exercise failure handling. Zero means never.
	FailAt int `json:"fail_at,omitempty"`
	// FailAttempts makes the first this-many attempts fail after writing every
	// line, so the run only succeeds on a retry. Zero means never.
	FailAttempts int `json:"fail_attempts,omitempty"`
}

const (
	maxLines        = 100_000
	maxDelayMs      = 10_000
	maxFailAttempts = 10
)

func (p GenerateText) validate() error {
	if p.Lines < 1 || p.Lines > maxLines {
		return fmt.Errorf("payload.lines must be between 1 and %d", maxLines)
	}
	if p.DelayMs < 0 || p.DelayMs > maxDelayMs {
		return fmt.Errorf("payload.delay_ms must be between 0 and %d", maxDelayMs)
	}
	if p.FailAt < 0 || p.FailAt > p.Lines {
		return fmt.Errorf("payload.fail_at must be between 0 and lines (%d)", p.Lines)
	}
	if p.FailAttempts < 0 || p.FailAttempts > maxFailAttempts {
		return fmt.Errorf("payload.fail_attempts must be between 0 and %d", maxFailAttempts)
	}
	return nil
}

// Logger receives a run's log lines as the job produces them.
type Logger interface {
	Log(level, line string)
}

// Validate reports whether jobType is known and payload is valid for it.
func Validate(jobType string, payload json.RawMessage) error {
	switch jobType {
	case TypeGenerateText:
		_, err := parseGenerateText(payload)
		return err
	case "":
		return errors.New("type is required")
	default:
		return fmt.Errorf("unknown job type %q", jobType)
	}
}

// Run executes one attempt of a job and returns its output. attempt counts
// from 1. It stops early, returning ctx's error, if ctx is cancelled.
func Run(ctx context.Context, jobType string, payload json.RawMessage, attempt int, log Logger) (string, error) {
	switch jobType {
	case TypeGenerateText:
		p, err := parseGenerateText(payload)
		if err != nil {
			return "", err
		}
		return generateText(ctx, p, attempt, log)
	default:
		return "", fmt.Errorf("unknown job type %q", jobType)
	}
}

func parseGenerateText(payload json.RawMessage) (GenerateText, error) {
	var p GenerateText
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return GenerateText{}, fmt.Errorf("invalid payload: %w", err)
	}
	return p, p.validate()
}

func generateText(ctx context.Context, p GenerateText, attempt int, log Logger) (string, error) {
	prefix := p.Prefix
	if prefix == "" {
		prefix = "line"
	}
	// Report progress about 20 times however large the job is.
	every := max(1, p.Lines/20)

	log.Log("info", fmt.Sprintf("generating %d lines", p.Lines))
	var b strings.Builder
	for i := 1; i <= p.Lines; i++ {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "%s %d\n", prefix, i)

		if i == p.FailAt {
			log.Log("error", fmt.Sprintf("failing on purpose after %d lines", i))
			return "", fmt.Errorf("deliberate failure after %d lines (fail_at)", i)
		}
		if i%every == 0 || i == p.Lines {
			log.Log("info", fmt.Sprintf("wrote %d/%d lines", i, p.Lines))
		}
		if p.DelayMs > 0 && i < p.Lines {
			if err := sleep(ctx, time.Duration(p.DelayMs)*time.Millisecond); err != nil {
				return "", err
			}
		}
	}

	if attempt <= p.FailAttempts {
		log.Log("error", fmt.Sprintf("failing attempt %d on purpose (fail_attempts=%d)", attempt, p.FailAttempts))
		return "", fmt.Errorf("deliberate failure of attempt %d (fail_attempts=%d)", attempt, p.FailAttempts)
	}
	return b.String(), nil
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// NextRun returns when cronExpr next fires after `after`, evaluated in the
// time zone tz, in UTC. cronExpr is five standard fields or a descriptor such
// as @hourly or @every 10s.
func NextRun(cronExpr, tz string, after time.Time) (time.Time, error) {
	loc, err := time.LoadLocation(tz)
	if err != nil || tz == "Local" {
		return time.Time{}, fmt.Errorf("unknown timezone %q", tz)
	}
	sched, err := cron.ParseStandard(cronExpr)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid cron_expr: %w", err)
	}
	next := sched.Next(after.In(loc))
	if next.IsZero() {
		return time.Time{}, fmt.Errorf("cron_expr %q never fires", cronExpr)
	}
	return next.UTC(), nil
}
