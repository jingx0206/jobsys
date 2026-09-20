package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestWaitForRetriesUntilReady(t *testing.T) {
	initialBackoff = time.Millisecond
	calls := 0

	err := WaitFor(context.Background(), quiet, "thing", time.Second, func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("not yet")
		}
		return nil
	})

	if err != nil || calls != 3 {
		t.Errorf("err = %v after %d calls, want success on the 3rd call", err, calls)
	}
}

func TestWaitForGivesUpWithLastError(t *testing.T) {
	initialBackoff = time.Millisecond
	start := time.Now()

	err := WaitFor(context.Background(), quiet, "thing", 50*time.Millisecond, func(context.Context) error {
		return errors.New("connection refused")
	})

	if err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("err = %v, want it to wrap the last check error", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("gave up after %v, want about the 50ms timeout", elapsed)
	}
}

func TestSplitList(t *testing.T) {
	got := SplitList(" kafka-0:9092, ,kafka-1:9092 ")
	if strings.Join(got, "|") != "kafka-0:9092|kafka-1:9092" {
		t.Errorf("SplitList = %q, want the two brokers trimmed", got)
	}
}
