package queue

import (
	"context"
	"strings"
	"testing"

	"github.com/jingxu/jobsys/internal/store"
)

func TestDispatchMessageRoundTrip(t *testing.T) {
	run := store.Run{ID: "run-1", JobID: "job-1", Attempt: 2}

	msg, err := dispatchMessage(run, "generate_text")
	if err != nil {
		t.Fatalf("dispatchMessage: %v", err)
	}

	if string(msg.Key) != run.ID {
		t.Errorf("key = %q, want the run id %q", msg.Key, run.ID)
	}
	got, err := DecodeDispatch(msg.Value)
	if err != nil {
		t.Fatalf("DecodeDispatch: %v", err)
	}
	want := Dispatch{RunID: "run-1", JobID: "job-1", JobType: "generate_text", Attempt: 2}
	if got != want {
		t.Errorf("decoded %+v, want %+v", got, want)
	}
}

func TestDecodeDispatchRejectsBadMessages(t *testing.T) {
	for _, body := range []string{`not json`, `{}`, `{"job_id": "job-1"}`} {
		if _, err := DecodeDispatch([]byte(body)); err == nil {
			t.Errorf("DecodeDispatch(%s) succeeded, want an error", body)
		}
	}
}

func TestCheckTopicNeedsBrokers(t *testing.T) {
	// A worker with no broker list would otherwise start up and sit silent,
	// looking healthy while nothing is ever dispatched to it.
	for _, brokers := range [][]string{nil, {}} {
		_, err := CheckTopic(context.Background(), brokers)
		if err == nil {
			t.Fatalf("CheckTopic(%v) succeeded, want it to refuse an empty broker list", brokers)
		}
		if !strings.Contains(err.Error(), "no kafka brokers") {
			t.Errorf("err = %v, want it to name the missing broker list", err)
		}
	}
}

// Re-dispatching a run must not be pinned to the partition of its previous
// attempt, which may belong to the worker that just died. Consecutive
// messages for the same run should land on different partitions.
func TestPublisherBalancerDoesNotPinRuns(t *testing.T) {
	partitions := []int{0, 1, 2}
	balancer := NewPublisher([]string{"unused:9092"}).w.Balancer
	msg, err := dispatchMessage(store.Run{ID: store.NewID(), JobID: "job-1", Attempt: 1}, "generate_text")
	if err != nil {
		t.Fatalf("dispatchMessage: %v", err)
	}

	seen := map[int]bool{}
	for i := 0; i < len(partitions); i++ {
		seen[balancer.Balance(msg, partitions...)] = true
	}

	if len(seen) != len(partitions) {
		t.Errorf("3 dispatches of one run used partitions %v, want all of %v", seen, partitions)
	}
}
