// Package queue publishes and consumes run dispatch messages on Kafka.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/jingxu/jobsys/internal/store"
)

const (
	// DispatchTopic carries one message per queued run.
	DispatchTopic = "job-dispatch"
	// WorkerGroup is the consumer group shared by the worker pool, so each
	// run is delivered to exactly one worker.
	WorkerGroup = "job-workers"
)

// dispatchTimeout bounds how long triggering a run waits on the broker.
const dispatchTimeout = 10 * time.Second

// Dispatch is the message published for each queued run. It only identifies
// the run: workers read everything else from Postgres, which stays the source
// of truth, so a stale or duplicate message can never override it.
type Dispatch struct {
	RunID   string `json:"run_id"`
	JobID   string `json:"job_id"`
	JobType string `json:"job_type"`
	Attempt int    `json:"attempt"`
}

// DecodeDispatch parses a dispatch message.
func DecodeDispatch(b []byte) (Dispatch, error) {
	var d Dispatch
	if err := json.Unmarshal(b, &d); err != nil {
		return Dispatch{}, fmt.Errorf("decode dispatch: %w", err)
	}
	if d.RunID == "" {
		return Dispatch{}, errors.New("decode dispatch: missing run_id")
	}
	return d, nil
}

// Publisher writes dispatch messages.
//
// Messages are spread round-robin across partitions rather than hashed by run
// id. Order between a run's messages does not matter, since Postgres decides
// who owns a run, and hashing would pin a requeued run to the same partition
// as its lost attempt: the partition of the worker that just died, which no
// one reads until the consumer group rebalances. The run id is still set as
// the key so messages are easy to find with Kafka tooling.
type Publisher struct {
	w *kafka.Writer
}

func NewPublisher(brokers []string) *Publisher {
	return &Publisher{w: &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        DispatchTopic,
		Balancer:     &kafka.RoundRobin{},
		RequiredAcks: kafka.RequireAll,
		// Send each message promptly. The default batches for up to a second,
		// which would add that much latency to every run trigger.
		BatchTimeout: 10 * time.Millisecond,
		MaxAttempts:  3,
		WriteTimeout: 5 * time.Second,
	}}
}

// Dispatch publishes a queued run for the worker pool. It returns only once
// the broker has acknowledged the message. It implements api.Dispatcher.
func (p *Publisher) Dispatch(ctx context.Context, run store.Run, jobType string) error {
	msg, err := dispatchMessage(run, jobType)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, dispatchTimeout)
	defer cancel()
	return p.w.WriteMessages(ctx, msg)
}

func dispatchMessage(run store.Run, jobType string) (kafka.Message, error) {
	value, err := json.Marshal(Dispatch{
		RunID:   run.ID,
		JobID:   run.JobID,
		JobType: jobType,
		Attempt: run.Attempt,
	})
	if err != nil {
		return kafka.Message{}, fmt.Errorf("encode dispatch: %w", err)
	}
	return kafka.Message{Key: []byte(run.ID), Value: value}, nil
}

// Close flushes pending messages and closes the writer.
func (p *Publisher) Close() error {
	return p.w.Close()
}

// CheckTopic confirms the dispatch topic exists and returns its partition
// count. Topic auto-creation is disabled, so a missing topic means deployment
// skipped creating it.
func CheckTopic(ctx context.Context, brokers []string) (int, error) {
	if len(brokers) == 0 {
		return 0, errors.New("no kafka brokers configured")
	}
	var lastErr error
	for _, b := range brokers {
		conn, err := kafka.DialContext(ctx, "tcp", b)
		if err != nil {
			lastErr = err
			continue
		}
		parts, err := conn.ReadPartitions(DispatchTopic)
		conn.Close()
		if err != nil {
			return 0, fmt.Errorf("topic %s: %w", DispatchTopic, err)
		}
		return len(parts), nil
	}
	return 0, fmt.Errorf("reach kafka: %w", lastErr)
}

// NewConsumer joins the worker group and reads dispatched runs.
func NewConsumer(brokers []string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   DispatchTopic,
		GroupID: WorkerGroup,
		// How long before the group gives a silent worker's partitions to
		// the others. The 30s default leaves new runs on a crashed worker's
		// partitions waiting that long.
		SessionTimeout:    10 * time.Second,
		HeartbeatInterval: 3 * time.Second,
	})
}
