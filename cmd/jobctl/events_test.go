package main

import (
	"errors"
	"strings"
	"testing"
)

func TestReadEvents(t *testing.T) {
	stream := "event: attempt\ndata: {\"attempt\":1}\n\n" +
		": ping\n\n" +
		"event: line\ndata: {\"line\":\"a\"}\n\n" +
		"event: end\ndata: {\"status\":\"SUCCEEDED\"}\n\n"

	var got []string
	err := readEvents(strings.NewReader(stream), func(event, data string) error {
		got = append(got, event+" "+data)
		return nil
	})

	want := []string{`attempt {"attempt":1}`, `line {"line":"a"}`, `end {"status":"SUCCEEDED"}`}
	if err != nil || strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("got %v (err %v), want %v", got, err, want)
	}
}

func TestReadEventsStopsOnCallbackError(t *testing.T) {
	stop := errors.New("stop")
	calls := 0

	err := readEvents(strings.NewReader("event: a\ndata: 1\n\nevent: b\ndata: 2\n\n"), func(string, string) error {
		calls++
		return stop
	})

	if !errors.Is(err, stop) || calls != 1 {
		t.Errorf("err = %v after %d calls, want the callback's error after 1 call", err, calls)
	}
}
