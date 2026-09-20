// Command worker consumes dispatched runs from Kafka and executes them.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/jingxu/jobsys/internal/app"
	"github.com/jingxu/jobsys/internal/queue"
	"github.com/jingxu/jobsys/internal/worker"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("worker stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	brokers := app.SplitList(app.Env("KAFKA_BROKERS", "localhost:9092"))
	log = log.With("worker", cfg.ID)

	db, err := app.OpenDB(ctx, log)
	if err != nil {
		return err
	}
	defer db.Close()

	partitions, err := app.WaitForTopic(ctx, log, brokers)
	if err != nil {
		return err
	}

	pub := queue.NewPublisher(brokers)
	defer pub.Close()
	reader := queue.NewConsumer(brokers)
	defer reader.Close()

	log.Info("worker started", "pid", os.Getpid(), "group", queue.WorkerGroup,
		"partitions", partitions, "concurrency", cfg.Concurrency, "output_dir", cfg.OutputDir)
	err = worker.New(cfg, db, pub, log).Run(ctx, reader)
	log.Info("worker stopped")
	return err
}

func loadConfig() (worker.Config, error) {
	host, _ := os.Hostname()
	cfg := worker.Config{
		// Hostname alone is not unique when several workers run on one
		// machine; in Kubernetes WORKER_ID is set to the pod name.
		ID:        app.Env("WORKER_ID", fmt.Sprintf("%s-%d", host, os.Getpid())),
		OutputDir: app.Env("OUTPUT_DIR", filepath.Join(os.TempDir(), "jobsys-output")),
	}
	var err error
	if cfg.Concurrency, err = app.EnvInt("WORKER_CONCURRENCY", 2); err != nil {
		return cfg, err
	}
	durations := []struct {
		key  string
		def  time.Duration
		dest *time.Duration
	}{
		{"HEARTBEAT_INTERVAL", 3 * time.Second, &cfg.HeartbeatInterval},
		{"STALE_AFTER", 15 * time.Second, &cfg.StaleAfter},
		{"MAINTENANCE_INTERVAL", 5 * time.Second, &cfg.MaintenanceInterval},
		{"SWEEP_AFTER", 60 * time.Second, &cfg.SweepAfter},
		{"SHUTDOWN_GRACE", 10 * time.Second, &cfg.ShutdownGrace},
		{"RETRY_BASE_DELAY", 5 * time.Second, &cfg.RetryBaseDelay},
		{"RETRY_MAX_DELAY", 5 * time.Minute, &cfg.RetryMaxDelay},
	}
	for _, d := range durations {
		if *d.dest, err = app.EnvDuration(d.key, d.def); err != nil {
			return cfg, err
		}
	}
	if cfg.Concurrency < 1 {
		return cfg, fmt.Errorf("WORKER_CONCURRENCY must be at least 1")
	}
	if cfg.RetryBaseDelay <= 0 || cfg.RetryMaxDelay < cfg.RetryBaseDelay {
		return cfg, fmt.Errorf("need 0 < RETRY_BASE_DELAY (%v) <= RETRY_MAX_DELAY (%v)", cfg.RetryBaseDelay, cfg.RetryMaxDelay)
	}
	if cfg.StaleAfter < 3*cfg.HeartbeatInterval {
		return cfg, fmt.Errorf("STALE_AFTER (%v) must be at least 3x HEARTBEAT_INTERVAL (%v), "+
			"or a slow heartbeat gets a live worker's run taken away", cfg.StaleAfter, cfg.HeartbeatInterval)
	}
	return cfg, nil
}
