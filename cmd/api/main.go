// Command api serves the job management HTTP API and runs the cron scheduler.
//
// `api migrate` instead applies the embedded database migrations and exits;
// Compose and Kubernetes run it before the API and workers start.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	// Embed the time zone database: cron schedules name zones like
	// America/New_York, and minimal container images ship without one.
	_ "time/tzdata"

	"github.com/jingxu/jobsys/internal/api"
	"github.com/jingxu/jobsys/internal/app"
	"github.com/jingxu/jobsys/internal/queue"
	"github.com/jingxu/jobsys/internal/scheduler"
	"github.com/jingxu/jobsys/migrations"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		err = migrate(ctx, log)
	} else {
		err = serve(ctx, log)
	}
	if err != nil {
		log.Error("api stopped", "err", err)
		os.Exit(1)
	}
}

func migrate(ctx context.Context, log *slog.Logger) error {
	db, err := app.OpenDB(ctx, log)
	if err != nil {
		return err
	}
	defer db.Close()

	applied, err := db.Migrate(ctx, migrations.FS)
	if err != nil {
		return err
	}
	log.Info("database schema up to date", "applied", applied)
	return nil
}

func serve(ctx context.Context, log *slog.Logger) error {
	brokers := app.SplitList(app.Env("KAFKA_BROKERS", "localhost:9092"))
	addr := ":" + app.Env("PORT", "8080")
	schedEvery, err := app.EnvDuration("SCHEDULER_INTERVAL", 2*time.Second)
	if err != nil {
		return err
	}

	db, err := app.OpenDB(ctx, log)
	if err != nil {
		return err
	}
	defer db.Close()

	partitions, err := app.WaitForTopic(ctx, log, brokers)
	if err != nil {
		return err
	}
	log.Info("kafka ready", "topic", queue.DispatchTopic, "partitions", partitions)

	pub := queue.NewPublisher(brokers)
	defer pub.Close()

	// The scheduler runs in every API replica; its queries make that safe.
	schedDone := make(chan struct{})
	go func() {
		defer close(schedDone)
		scheduler.New(db, pub, log.With("component", "scheduler"), schedEvery).Run(ctx)
	}()
	log.Info("scheduler started", "interval", schedEvery)

	srv := &http.Server{
		Addr:              addr,
		Handler:           api.NewServer(db, pub, log).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		log.Info("api listening", "addr", addr)
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	log.Info("api shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = srv.Shutdown(shutdownCtx)
	<-schedDone
	return err
}
