// Command jobctl is a command-line client for the jobsys API.
//
//	jobctl jobs                      list jobs
//	jobctl create -name N [flags]    create a generate_text job
//	jobctl run [-f] <job-id>         trigger a run; -f follows its log
//	jobctl status <run-id>           show a run
//	jobctl logs [-f] <run-id>        print a run's log; -f follows it to the end
//
// The API address comes from -api or JOBSYS_API (default http://localhost:8080).
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"
	"time"
)

type jobInfo struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	Type      string     `json:"type"`
	CronExpr  *string    `json:"cron_expr"`
	Timezone  string     `json:"timezone"`
	NextRunAt *time.Time `json:"next_run_at"`
}

type runInfo struct {
	ID            string     `json:"id"`
	JobID         string     `json:"job_id"`
	Trigger       string     `json:"trigger"`
	Status        string     `json:"status"`
	Attempt       int        `json:"attempt"`
	WorkerID      *string    `json:"worker_id"`
	ScheduledTime time.Time  `json:"scheduled_time"`
	StartedAt     *time.Time `json:"started_at"`
	FinishedAt    *time.Time `json:"finished_at"`
	NextAttemptAt *time.Time `json:"next_attempt_at"`
	Error         *string    `json:"error"`
	Output        *string    `json:"output"`
}

type logLine struct {
	Attempt int       `json:"attempt"`
	Seq     int64     `json:"seq"`
	TS      time.Time `json:"ts"`
	Level   string    `json:"level"`
	Line    string    `json:"line"`
}

func main() {
	def := os.Getenv("JOBSYS_API")
	if def == "" {
		def = "http://localhost:8080"
	}
	api := flag.String("api", def, "API base URL")
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() == 0 {
		usage()
		os.Exit(2)
	}

	c := &client{base: strings.TrimRight(*api, "/")}
	args := flag.Args()[1:]
	var err error
	switch flag.Arg(0) {
	case "jobs":
		err = c.jobs()
	case "create":
		err = c.create(args)
	case "run":
		err = c.run(args)
	case "status":
		err = c.status(args)
	case "logs":
		err = c.logs(args)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "jobctl:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: jobctl [-api URL] <command> [flags] [args]

commands:
  jobs                      list jobs
  create -name N [flags]    create a generate_text job (jobctl create -h for flags)
  run [-f] <job-id>         trigger a run; -f follows its log
  status <run-id>           show a run
  logs [-f] <run-id>        print a run's log; -f follows it until the run ends
`)
}

type client struct {
	base string
}

// requestClient bounds ordinary requests. Streams use http.DefaultClient,
// which has no timeout.
var requestClient = &http.Client{Timeout: 30 * time.Second}

// do sends a JSON request and decodes the JSON response into out, turning an
// API error response into a Go error.
func (c *client) do(method, path string, body, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.base+path, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := requestClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return apiError(resp)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func apiError(resp *http.Response) error {
	var e struct {
		Error string `json:"error"`
	}
	json.NewDecoder(resp.Body).Decode(&e)
	if e.Error == "" {
		e.Error = resp.Status
	}
	return fmt.Errorf("%s %s: %s", resp.Request.Method, resp.Request.URL.Path, e.Error)
}

func (c *client) jobs() error {
	var resp struct {
		Jobs []jobInfo `json:"jobs"`
	}
	if err := c.do(http.MethodGet, "/jobs?limit=200", nil, &resp); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tNAME\tSCHEDULE\tNEXT RUN")
	for _, j := range resp.Jobs {
		schedule, next := "manual", "-"
		if j.CronExpr != nil {
			schedule = fmt.Sprintf("%s (%s)", *j.CronExpr, j.Timezone)
		}
		if j.NextRunAt != nil {
			next = j.NextRunAt.Local().Format(time.DateTime)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", j.ID, j.Name, schedule, next)
	}
	return tw.Flush()
}

func (c *client) create(args []string) error {
	fs := flag.NewFlagSet("create", flag.ExitOnError)
	name := fs.String("name", "", "job name (required)")
	lines := fs.Int("lines", 10, "lines to generate")
	prefix := fs.String("prefix", "", "start of each line")
	delay := fs.Int("delay-ms", 0, "pause after each line, in milliseconds")
	failAt := fs.Int("fail-at", 0, "fail every attempt after this many lines")
	failAttempts := fs.Int("fail-attempts", 0, "fail the first N attempts, so the run succeeds on a retry")
	cronExpr := fs.String("cron", "", `schedule, e.g. "*/5 * * * *" or "@every 30s"; omit for manual runs only`)
	tz := fs.String("tz", "", "time zone for -cron, e.g. America/New_York (default UTC)")
	maxRetries := fs.Int("max-retries", -1, "retries after a failed attempt (default: server's)")
	timeout := fs.Int("timeout", 0, "seconds allowed per attempt (default: server's)")
	fs.Parse(args)
	if *name == "" {
		return errors.New("create: -name is required")
	}

	payload := map[string]any{"lines": *lines}
	for key, v := range map[string]int{"delay_ms": *delay, "fail_at": *failAt, "fail_attempts": *failAttempts} {
		if v != 0 {
			payload[key] = v
		}
	}
	if *prefix != "" {
		payload["prefix"] = *prefix
	}
	body := map[string]any{"name": *name, "type": "generate_text", "payload": payload}
	if *cronExpr != "" {
		body["cron_expr"] = *cronExpr
	}
	if *tz != "" {
		body["timezone"] = *tz
	}
	if *maxRetries >= 0 {
		body["max_retries"] = *maxRetries
	}
	if *timeout > 0 {
		body["timeout_sec"] = *timeout
	}

	var j jobInfo
	if err := c.do(http.MethodPost, "/jobs", body, &j); err != nil {
		return err
	}
	fmt.Println(j.ID)
	if j.NextRunAt != nil {
		fmt.Fprintf(os.Stderr, "scheduled %q; first run at %s\n", *j.CronExpr, j.NextRunAt.Local().Format(time.DateTime))
	}
	return nil
}

func (c *client) run(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	follow := fs.Bool("f", false, "follow the run's log until it finishes")
	fs.Parse(args)
	if fs.NArg() != 1 {
		return errors.New("usage: jobctl run [-f] <job-id>")
	}

	var r runInfo
	if err := c.do(http.MethodPost, "/jobs/"+url.PathEscape(fs.Arg(0))+"/run", nil, &r); err != nil {
		return err
	}
	fmt.Println(r.ID)
	if *follow {
		return c.follow(r.ID)
	}
	return nil
}

func (c *client) status(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: jobctl status <run-id>")
	}
	var r runInfo
	if err := c.do(http.MethodGet, "/runs/"+url.PathEscape(args[0]), nil, &r); err != nil {
		return err
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	row := func(k, v string) { fmt.Fprintf(tw, "%s\t%s\n", k, v) }
	row("run", r.ID)
	row("job", r.JobID)
	row("status", r.Status)
	row("attempt", fmt.Sprint(r.Attempt))
	row("trigger", r.Trigger)
	row("scheduled", r.ScheduledTime.Local().Format(time.DateTime))
	if r.WorkerID != nil {
		row("worker", *r.WorkerID)
	}
	if r.StartedAt != nil {
		row("started", r.StartedAt.Local().Format(time.DateTime))
	}
	if r.FinishedAt != nil {
		row("finished", r.FinishedAt.Local().Format(time.DateTime))
	}
	if r.NextAttemptAt != nil {
		row("retry at", r.NextAttemptAt.Local().Format(time.DateTime))
	}
	if r.Error != nil {
		row("error", *r.Error)
	}
	if r.Output != nil {
		row("output", fmt.Sprintf("%d lines", strings.Count(*r.Output, "\n")))
	}
	return tw.Flush()
}

func (c *client) logs(args []string) error {
	fs := flag.NewFlagSet("logs", flag.ExitOnError)
	follow := fs.Bool("f", false, "follow every attempt until the run finishes")
	fs.Parse(args)
	if fs.NArg() != 1 {
		return errors.New("usage: jobctl logs [-f] <run-id>")
	}
	runID := fs.Arg(0)
	if *follow {
		return c.follow(runID)
	}

	// Without -f, print the current attempt's log page by page.
	from := int64(1)
	for {
		var page struct {
			Attempt int       `json:"attempt"`
			Lines   []logLine `json:"lines"`
			NextSeq int64     `json:"next_seq"`
		}
		path := fmt.Sprintf("/runs/%s/logs?from_seq=%d&limit=1000", url.PathEscape(runID), from)
		if err := c.do(http.MethodGet, path, nil, &page); err != nil {
			return err
		}
		for _, l := range page.Lines {
			printLine(l)
		}
		if len(page.Lines) < 1000 {
			return nil
		}
		from = page.NextSeq
	}
}

var errStreamEnd = errors.New("end of stream")

// follow streams a run's log until the run finishes, and fails if the run did.
func (c *client) follow(runID string) error {
	resp, err := http.Get(c.base + "/runs/" + url.PathEscape(runID) + "/logs/stream")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return apiError(resp)
	}

	var end struct {
		Status  string  `json:"status"`
		Attempt int     `json:"attempt"`
		Error   *string `json:"error"`
	}
	err = readEvents(resp.Body, func(event, data string) error {
		switch event {
		case "attempt":
			var a struct{ Attempt int }
			json.Unmarshal([]byte(data), &a)
			fmt.Printf("--- attempt %d ---\n", a.Attempt)
		case "line":
			var l logLine
			if err := json.Unmarshal([]byte(data), &l); err != nil {
				return err
			}
			printLine(l)
		case "end":
			if err := json.Unmarshal([]byte(data), &end); err != nil {
				return err
			}
			return errStreamEnd
		}
		return nil
	})
	if !errors.Is(err, errStreamEnd) {
		if err == nil {
			err = errors.New("stream closed before the run finished")
		}
		return err
	}

	fmt.Printf("--- run %s after %d attempt(s) ---\n", end.Status, end.Attempt)
	if end.Status != "SUCCEEDED" {
		msg := end.Status
		if end.Error != nil {
			msg += ": " + *end.Error
		}
		return errors.New("run " + msg)
	}
	return nil
}

// readEvents parses a server-sent event stream, calling fn for each event with
// its name and data. Comment lines such as ": ping" are skipped. It returns
// fn's first error, or the reader's.
func readEvents(r io.Reader, fn func(event, data string) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	var event string
	var data []string
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if event != "" || len(data) > 0 {
				if err := fn(event, strings.Join(data, "\n")); err != nil {
					return err
				}
			}
			event, data = "", nil
		case strings.HasPrefix(line, ":"):
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	return sc.Err()
}

func printLine(l logLine) {
	fmt.Printf("%s  %-5s  %s\n", l.TS.Local().Format("15:04:05"), l.Level, l.Line)
}
