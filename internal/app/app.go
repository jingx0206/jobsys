// Package app holds startup plumbing shared by the api and worker commands:
// configuration from the environment, and waiting for Postgres and Kafka.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jingxu/jobsys/internal/queue"
	"github.com/jingxu/jobsys/internal/store"
)

// StartupTimeout bounds how long a command waits for Postgres and Kafka.
// In Kubernetes the API and workers can start before either is ready, so they
// wait and retry instead of exiting into a CrashLoopBackOff.
const StartupTimeout = 3 * time.Minute

const (
	attemptTimeout = 10 * time.Second
	maxBackoff     = 5 * time.Second
)

// initialBackoff is a variable so tests can shorten it.
var initialBackoff = 500 * time.Millisecond

// OpenDB connects to DATABASE_URL, waiting for Postgres to accept connections.
func OpenDB(ctx context.Context, log *slog.Logger) (*store.DB, error) {
	dsn := Env("DATABASE_URL", "postgres://postgres@localhost:5432/jobsys?sslmode=disable")
	var db *store.DB
	err := WaitFor(ctx, log, "postgres", StartupTimeout, func(ctx context.Context) error {
		var err error
		db, err = store.Open(ctx, dsn)
		return err
	})
	return db, err
}

// WaitForTopic waits until the dispatch topic exists and returns its
// partition count.
func WaitForTopic(ctx context.Context, log *slog.Logger, brokers []string) (int, error) {
	var partitions int
	err := WaitFor(ctx, log, "kafka topic "+queue.DispatchTopic, StartupTimeout, func(ctx context.Context) error {
		var err error
		partitions, err = queue.CheckTopic(ctx, brokers)
		return err
	})
	return partitions, err
}

// WaitFor calls check until it succeeds, backing off between attempts, and
// gives up after timeout with the last error.
func WaitFor(ctx context.Context, log *slog.Logger, what string, timeout time.Duration, check func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	backoff := initialBackoff
	for attempt := 1; ; attempt++ {
		attemptCtx, cancelAttempt := context.WithTimeout(ctx, attemptTimeout)
		err := check(attemptCtx)
		cancelAttempt()
		if err == nil {
			if attempt > 1 {
				log.Info(what+" is ready", "attempts", attempt)
			}
			return nil
		}

		log.Warn("waiting for "+what, "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s not ready after %v: %w", what, timeout, err)
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// Env returns the environment variable key, or def if it is unset.
func Env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// EnvInt parses an integer environment variable, or returns def if unset.
func EnvInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

// EnvDuration parses a duration environment variable such as "15s", or
// returns def if unset.
func EnvDuration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}

// SplitList parses a comma-separated list, dropping empty entries.
func SplitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
