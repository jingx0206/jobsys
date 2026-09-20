# jobsys

A small distributed job management system: an API server with a cron
scheduler and a web UI, Kafka dispatch, a pool of workers, and Postgres as the
source of truth. Runs locally on Docker Compose and on Kubernetes.

Open `http://localhost:8080` for the web UI: create and schedule jobs, trigger
runs, and follow a run's log live across its retries.

## Layout

| Path | Contents |
| --- | --- |
| `cmd/api` | HTTP API server, cron scheduler and web UI; `api migrate` applies migrations |
| `cmd/worker` | worker that claims and executes runs |
| `cmd/jobctl` | command-line client for the API |
| `web` | the web UI (HTML, JavaScript, CSS), embedded in the api binary |
| `internal/scheduler` | fires cron jobs when they are due |
| `internal/store` | Postgres access (jobs, runs, logs, claiming, reaping) |
| `internal/queue` | Kafka producer and consumer |
| `internal/job` | job types, payload validation, executors |
| `internal/api` | HTTP handlers and routing |
| `internal/worker` | consume loop, execution, heartbeats, retries, reaper and sweeper |
| `internal/app` | startup plumbing shared by both commands: env config, waiting for Postgres and Kafka |
| `migrations` | SQL schema, embedded in the api binary; `checks/` verifies it |
| `deploy/docker` | Dockerfiles |
| `deploy/k8s` | Kubernetes manifests |

## How a run executes

1. A run is queued either by `POST /jobs/{id}/run` or by the scheduler when a
   job's cron occurrence comes due. Either way a `QUEUED` row is inserted and
   its id published to the `job-dispatch` topic.
2. A worker in the `job-workers` consumer group reads the message and claims
   the run with `UPDATE ... WHERE status = 'QUEUED'`. Only one claim can
   succeed, so a duplicate message never runs a job twice.
3. The worker commits the offset, runs the job, heartbeats every few seconds,
   and writes the log in batches and the result at the end.

### Scheduling

The scheduler runs inside the API every `SCHEDULER_INTERVAL`. In one
transaction it locks due jobs (`next_run_at <= now()`), inserts a run for the
occurrence, and advances `next_run_at`. `SKIP LOCKED` plus the unique
`(job_id, scheduled_time)` key mean each occurrence fires exactly once, even
with several API replicas. A scheduler that was down fires each overdue job
once and then carries on from now, rather than replaying every missed
occurrence. Schedules take five-field cron expressions or descriptors such as
`@hourly` and `@every 30s`, evaluated in the job's time zone. Pause and resume
a job with `POST /jobs/{id}/pause` and `/resume`.

### Retries

When an attempt fails (the job errors or times out), the run is requeued as the
next attempt with exponential backoff, `RETRY_BASE_DELAY` doubling up to
`RETRY_MAX_DELAY`, until `max_retries` is used up and it fails for good. A
retry can't be claimed before its backoff ends, even if a stray message
arrives early. Workers dispatch retries as they come due.

### Recovery

All driven from Postgres rather than Kafka offsets:

| Failure | Handled by |
| --- | --- |
| Worker crashes mid-run | **Reaper**: a run with no heartbeat for `STALE_AFTER` is requeued as the next attempt and re-dispatched, or failed once `max_retries` is used up. |
| Worker shuts down mid-run | **Release**: after `SHUTDOWN_GRACE`, unfinished runs are requeued and re-dispatched immediately. |
| Dispatch message lost | **Sweeper**: a run `QUEUED` for longer than `SWEEP_AFTER` is re-dispatched. |
| A reaped worker wakes up | Heartbeats and results only apply while the worker still owns the attempt, so it stops and its result is discarded. |

Every worker runs the reaper, sweeper and retry dispatcher; their queries use
`SKIP LOCKED` so two workers never handle the same run.

## Web UI and jobctl

The API serves the web UI at `/`. It lists jobs (with Run, Pause and Resume),
has a form to create one, shows recent runs with their status (refreshing every
two seconds), and follows a selected run's log live, attempt by attempt, over
`GET /runs/{id}/logs/stream`. It is plain HTML, JavaScript and CSS with no
build step, embedded in the api binary.

`jobctl` does the same from a terminal:

```bash
go run ./cmd/jobctl create -name report -lines 20 -delay-ms 200 -fail-attempts 2
```

```bash
go run ./cmd/jobctl run -f <job-id>
```

Other commands: `jobs`, `status <run-id>`, `logs [-f] <run-id>`. Point it at
another API with `-api` or `JOBSYS_API`.

## Run with Docker Compose

Starts Postgres, Kafka, the API and two workers:

```bash
docker compose up -d --build
```

In order: Postgres becomes healthy, `migrate` runs `api migrate`, `kafka-init`
creates the topic, then the API and workers start. The API is on
`localhost:8080`; each worker's id is its container id.

| Task | Command |
| --- | --- |
| Follow worker logs | `docker compose logs -f worker` |
| Query the database | `docker exec -it jobsys-postgres psql -U jobsys -d jobsys` |
| See output files (shared by both workers) | `docker compose exec worker ls /data/output` |
| Hand a worker's runs to the other one | `docker stop <worker container>` (SIGTERM; running jobs get `SHUTDOWN_GRACE`, then are released) |
| Simulate a crash | `docker kill <worker container>` (the reaper requeues its run after `STALE_AFTER`) |
| Stop everything, keep data | `docker compose stop` |
| Stop and delete all data | `docker compose down -v` |

Images are built from `deploy/docker/Dockerfile` with `SERVICE=api` or
`SERVICE=worker`. The Postgres password defaults to a development value; set
`POSTGRES_PASSWORD` to override it.

## Run on Kubernetes

Tested on Docker Desktop's built-in cluster. Stop the Compose stack first
(`docker compose stop`) so the two don't compete for resources and port 8080.

Build the images, copy them onto the cluster node, and deploy:

```bash
docker compose build api worker
```

```bash
bash deploy/k8s/load-images.sh
```

```bash
kubectl apply -k deploy/k8s
```

```bash
kubectl -n jobsys port-forward svc/api 8080:8080
```

Docker Desktop's cluster runs its own containerd and cannot see Docker's
images, so `load-images.sh` streams them onto the node the way
`kind load docker-image` does. The manifests use `imagePullPolicy: Never`, so a
missing image fails fast with `ErrImageNeverPull`.

What gets deployed into the `jobsys` namespace:

| Object | Notes |
| --- | --- |
| `postgres` StatefulSet | 1Gi volume; credentials from the `jobsys-db` Secret (development values). |
| `kafka` StatefulSet | Single-node KRaft, 1Gi volume. |
| `kafka-init` Job | Creates the `job-dispatch` topic, retrying until Kafka is up. |
| `api` Deployment + Service | Startup, liveness (`/healthz`) and readiness (`/readyz`) probes. |
| `worker` Deployment | 2 replicas, `WORKER_ID` = pod name, 30s termination grace so shutdown can hand runs back. |

Every API and worker pod runs `api migrate` as an init container; an advisory
lock makes them take turns, so each migration applies once. Pods may start
before Postgres or Kafka is ready; they wait up to three minutes rather than
crash-looping.

| Task | Command |
| --- | --- |
| Watch pods | `kubectl -n jobsys get pods -w` |
| Follow worker logs | `kubectl -n jobsys logs -l app=worker -c worker --prefix -f` |
| Query the database | `kubectl -n jobsys exec -it postgres-0 -- psql -U jobsys -d jobsys` |
| Hand a worker's runs to another pod | `kubectl -n jobsys delete pod <worker pod>` |
| Simulate a crash (reaper recovers) | `kubectl -n jobsys delete pod <worker pod> --grace-period=0 --force` |
| Scale workers | `kubectl -n jobsys scale deployment/worker --replicas=3` |
| See partition assignment | `kubectl -n jobsys exec kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group job-workers --members` |
| Deploy rebuilt images | `bash deploy/k8s/load-images.sh`, then `kubectl -n jobsys rollout restart deployment` |
| Remove everything, including data | `kubectl delete -k deploy/k8s` |

The topic has 3 partitions, so at most 3 workers get work; a 4th replica joins
the consumer group but is assigned no partition and stays idle. Raise the
partition count before scaling past 3.

## Run without Docker

Needs Kafka and a Postgres the API can reach without a password in the URL.
Locally that is the native Postgres with credentials in
`%APPDATA%\postgresql\pgpass.conf`, which pgx reads automatically.

Start Kafka and create the `job-dispatch` topic (3 partitions). Topic
auto-creation is off, so the API and workers refuse to start until
`kafka-init` has run:

```bash
docker compose up -d kafka
```

```bash
docker compose up kafka-init
```

Create the database and apply the migrations:

```bash
createdb -h localhost -U postgres jobsys
```

```bash
go run ./cmd/api migrate
```

Start the API, then two workers in separate terminals (stop the Compose `api`
and `worker` containers first, or they will share the work):

```bash
go run ./cmd/api
```

```bash
WORKER_ID=w1 go run ./cmd/worker
```

```bash
WORKER_ID=w2 go run ./cmd/worker
```

Stop Kafka with `docker compose stop`; `docker compose down -v` also deletes
its data.

## Configuration

Both commands read `DATABASE_URL` (default
`postgres://postgres@localhost:5432/jobsys?sslmode=disable`) and
`KAFKA_BROKERS` (default `localhost:9092`, comma-separated). The API also
reads `PORT` (default `8080`) and `SCHEDULER_INTERVAL` (default `2s`, how
often it checks for due cron jobs).

Worker settings:

| Variable | Default | Meaning |
| --- | --- | --- |
| `WORKER_ID` | hostname-pid | Name recorded on runs this worker claims. Must be unique. |
| `WORKER_CONCURRENCY` | `2` | Runs executed at once. |
| `HEARTBEAT_INTERVAL` | `3s` | How often a running job reports it is alive. |
| `STALE_AFTER` | `15s` | Silence before the reaper takes a run back. At least 3x the heartbeat. |
| `MAINTENANCE_INTERVAL` | `5s` | How often the reaper, sweeper and retry dispatcher run. |
| `SWEEP_AFTER` | `60s` | How long a run may sit `QUEUED` before re-dispatch. |
| `SHUTDOWN_GRACE` | `10s` | Time running jobs get to finish on shutdown before release. |
| `RETRY_BASE_DELAY` | `5s` | Backoff after a first failed attempt; doubles each further attempt. |
| `RETRY_MAX_DELAY` | `5m` | Cap on the retry backoff. |
| `OUTPUT_DIR` | `<temp>/jobsys-output` | Where each successful run's output is written as `<run_id>.txt`. |

## API

| Method and path | Does |
| --- | --- |
| `POST /jobs` | Create a job. `cron_expr` is optional; without it the job only runs when triggered. |
| `GET /jobs?limit=` | List jobs, newest first. |
| `GET /jobs/{id}` | Get a job. |
| `POST /jobs/{id}/run` | Queue a manual run and publish it to Kafka. Returns 202 with the run, or 502 (and marks the run FAILED) if Kafka is unreachable. |
| `POST /jobs/{id}/pause` | Stop the job's schedule. Manual runs still work. |
| `POST /jobs/{id}/resume` | Restart the schedule from the next occurrence after now. |
| `GET /runs?job_id=&limit=` | Recent runs, newest first, optionally for one job. Output is omitted; get the run for it. |
| `GET /runs/{id}` | Run status, attempt, worker, timing, retry time, error and output. |
| `GET /runs/{id}/logs?attempt=&from_seq=&limit=` | Page through one attempt's log, the current attempt by default. Pass `next_seq` back as `from_seq` to continue. |
| `GET /runs/{id}/logs/stream` | Server-sent events following the run's log across every attempt until it finishes: `attempt`, `line`, then `end`. |
| `GET /healthz` | Liveness: the process is up. |
| `GET /readyz` | Readiness: Postgres is reachable. |

Create a job that writes five lines at 9am New York time every day:

```bash
curl -X POST localhost:8080/jobs -d '{"name": "daily-report", "type": "generate_text", "payload": {"lines": 5}, "cron_expr": "0 9 * * *", "timezone": "America/New_York"}'
```

`generate_text` payload fields:

| Field | Meaning |
| --- | --- |
| `lines` | Lines to write, 1 to 100000. |
| `prefix` | Start of each line; defaults to `line`. |
| `delay_ms` | Pause per line, up to 10000, for watching a run or killing its worker mid-run. |
| `fail_at` | Fail every attempt after this many lines, to exercise failure handling. |
| `fail_attempts` | Fail the first N attempts after writing every line, so the run succeeds on a retry. |

## Tests

```bash
go test ./...
```

The store tests run the worker SQL against a real database and skip unless
`JOBSYS_TEST_DATABASE_URL` points at a scratch database with the migrations
applied:

```bash
JOBSYS_TEST_DATABASE_URL=postgres://postgres@localhost:5432/jobsys_test go test ./internal/store/
```

To check the schema itself, see the header of
`migrations/checks/001_init_check.sql`.
