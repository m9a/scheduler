# Distributed Job Scheduler

A distributed job scheduler where each job is a Docker container running sequential task stages. A coordinator accepts jobs from clients, workers pull and execute them in containers, and report status back via gRPC.

## Domain Model

A **Job** is an immutable definition: a Docker image (`artifactUri`), key-value params, a priority, and optional input files.

```
Job(name, artifactUri, params, priority, inputFiles)
```

Runtime state is tracked separately:

- `JobStatus(id, job, state, taskStatuses, timestamps, reason)` — mutable runtime state of a submitted job
- `TaskStatus(id, taskIndex, taskName, state, timestamps, errorMessage, exitCode)` — mutable runtime state of a task within a job
- `InputFile(name, uri)` — an input file resolved to an object store URI

The job does not declare its tasks upfront. Tasks are created lazily by the coordinator when the SDK first reports each task's status at runtime.

## Job Lifecycle

These tables are the canonical state definitions — code follows them, not the
other way around. **Job and task state are owned by the worker.** Task state is
written only by the SDK (as the job runs); the worker decides the job's terminal
state from the container's exit. The coordinator writes state in exactly one
case: the worker is dead (heartbeat lost), when it fails the **job** — it never
touches task state. It otherwise only applies what the worker sends and de-dupes
no-ops; it never infers or overrides. A task interrupted by a crash, kill, or
dead worker keeps its **last reported state** (no one back-fills it to FAILED).
Status updates travel as one object — a job section, a task section, or both;
the task section is proxied unchanged from the SDK, plus the worker's RUNNING
stamp.

### Job states

```
QUEUED → STARTING → RUNNING → COMPLETED
              ↘         ↘ FAILED
            FAILED      ↘ TIMEOUT → KILLED
```

| State | Set by | Trigger |
|-------|--------|---------|
| QUEUED | coordinator | `submit()` accepts the job |
| STARTING | coordinator | a worker claims it |
| RUNNING | worker | first task reports — the worker stamps RUNNING on every task update; the coordinator de-dupes |
| COMPLETED | worker | container exit 0 |
| FAILED | worker | container exit ≠ 0 (a task that threw exits the container non-zero), or `docker run` failed to start |
| FAILED | coordinator | worker missed health checks (heartbeat lost) — the only state the coordinator writes |
| TIMEOUT | worker | execution deadline hit; kill has been initiated |
| KILLED | worker | container kill confirmed (always preceded by TIMEOUT, or future cancel) |
| CANCELLED | coordinator | client cancel — no CancelJob RPC yet, currently unreachable |

### Task states

```
PENDING → RUNNING → COMPLETED
                  ↘ FAILED
```

| State | Set by | Trigger |
|-------|--------|---------|
| PENDING | coordinator | transient — created on the task's first update, transitioned immediately |
| RUNNING | sdk | `@task` method started |
| COMPLETED | sdk | method returned normally |
| FAILED | sdk | method threw/raised (carries the error) |

A task is only ever advanced by the SDK. If the container dies (crash, kill, or
dead worker) while a task is mid-execution, that task keeps its **last reported
state** (typically RUNNING) — only the job goes terminal. Tasks that never
started are simply **absent** from the job's task list — there is no task
manifest (the JAR is the sole source of task definitions), so a task exists only
once it has reported. There is no SKIPPED state.

## Module Structure

| Module | Purpose |
|--------|---------|
| `scheduler-core` | Domain records (`Job`, `JobStatus`, `TaskStatus`, `InputFile`, `ObjectStore`), enums, exceptions. Zero infrastructure dependencies. |
| `scheduler-proto` | Protobuf/gRPC definitions + generated code. Proto files in `src/main/proto/scheduler/v1/`. |
| `scheduler-coordinator` | gRPC server, `JobManagerImpl`, `ProtoMapper`, wiring. Sub-packages: `client/` (UserRequestHandler), `worker/` (WorkerHandler). |
| `scheduler-worker` | WorkerAgent main loop, Docker process spawning, status forwarding, file staging. |

The **scheduler-sdk** lives in a [separate repository](../scheduler-sdk). It provides the `Task` interface and `JobProcess` runtime for both Java and Python job containers.

## Architecture

```
┌──────────┐       gRPC        ┌─────────────────┐       gRPC        ┌──────────────┐     docker run     ┌─────────────────┐
│  Client  │ ────────────────► │   Coordinator   │ ◄──────────────── │  WorkerAgent │ ──────────────────► │ Docker container │
└──────────┘  ClientService    └─────────────────┘   WorkerService   └──────────────┘                    └─────────────────┘
              (submit, query)   (job queue + state)  (pull, report)     │          ▲                        │
                                       │                               │          │  WebSocket (binary proto) │
                                       │                               │          └────────────────────────┘
                                    MinIO                              │           (SDK reports task status)
                                 (object store)                        │
                                                                       └── WorkerAgent spawns: docker run --rm <image>
```

- **Client → Coordinator**: Clients submit jobs and query status via `ClientService` gRPC.
- **Worker → Coordinator**: Workers register, pull jobs, and stream status via `WorkerService` gRPC.
- **Worker → Docker container**: WorkerAgent runs `docker run` with volume mounts for input/output. The container runs the SDK, which sends binary proto status updates to WorkerAgent over WebSocket. WorkerAgent forwards these to the coordinator via gRPC.

### Design decisions (developers, read this)

- **One status message** — the generated `StatusUpdate` proto is the single status type across SDK, worker, and coordinator. There are no parallel domain copies and no proto↔domain conversions; the proto flows SDK → worker → coordinator unchanged. (`scheduler-core` imports the proto — that's an essential library, not an infra dependency.)
- **One WebSocket connection** — a job container and its worker share exactly one WebSocket connection for all SDK→worker traffic (status + telemetry). No per-message or secondary connections.
- **State ownership** — job and task state are owned by the **worker**. The coordinator writes state in exactly **one** case: a worker misses its heartbeat / loses its connection, at which point it fails the **job** (never task state). Otherwise the coordinator only applies and de-dupes what the worker sends. Task state is only ever advanced by the SDK; a task interrupted by a crash/kill/dead worker keeps its last reported state.

## Protocol Exchanges

Three boundaries, one proto file each; every message is labelled `Sender → Receiver`
in the `.proto`. The diagrams are split per boundary to stay readable.

### Client ↔ Coordinator — `client_api.proto` (`ClientService`)

All unary, except `GetJobOutput` which server-streams file chunks.

```
Client                                Coordinator
  │  SubmitJob(name, image, params…)      │  inline inputs → MinIO, job QUEUED
  │ ────────────────────────────────────► │
  │ ◄──────────────────────────────────── │  SubmitJobResponse(Job)
  │                                        │
  │  GetJobStatus(jobId)                   │
  │ ────────────────────────────────────► │
  │ ◄──────────────────────────────────── │  GetJobStatusResponse(Job snapshot)
  │                                        │
  │  ListJobFiles / GetJobOutput(jobId)    │
  │ ────────────────────────────────────► │
  │ ◄════════════════════════════════════ │  GetJobOutput: server-streamed chunks
```

### Worker ↔ Coordinator — `worker_api.proto` (`WorkerService`)

Two unary calls to register/claim, a periodic heartbeat, and **two per-job client
streams** (status + telemetry).

```
Worker                                     Coordinator
  │  RegisterWorker(hostname, resources)      │
  │ ────────────────────────────────────────► │  assigns workerId
  │                                           │
  │  PullJob(workerId)         (poll loop)    │  claimNextJob → job STARTING
  │ ────────────────────────────────────────► │
  │                                           │
  │  Heartbeat(workerId)       (every N s)    │  miss ⇒ job FAILED / HEARTBEAT_LOST
  │ ────────────────────────────────────────► │
  │                                           │
  │  ReportStatus(stream StatusUpdate)        │  one stream per job — lifecycle:
  │ ════════════════════════════════════════► │  task updates + job RUNNING/terminal
  │                                           │
  │  ReportTelemetry(stream Report)           │  one stream per job — metrics/events
  │ ════════════════════════════════════════► │  + liveness ticks (lossy, latest-wins)
```

### Why one bidirectional stream (protocol direction)

The worker↔coordinator exchange above started as plain unary RPCs
(`RegisterWorker`, `PullJob`, `Heartbeat`) plus two per-job client streams
(status, telemetry). It works, but it is effectively **one-directional**: the
worker only ever *pushes*, and the coordinator can answer a worker-initiated call
but cannot **push** to the worker. Every coordinator→worker signal therefore has
to piggyback on a heartbeat/pull *response*, with latency bounded by the poll
interval.

That is tolerable for one occasional signal, but cancel / drain / preempt / "please
re-register after my restart" are all genuine coordinator→worker commands —
bolting each onto the heartbeat response does not scale.

**Direction (in progress):** collapse the worker↔coordinator exchange onto a
**single long-lived gRPC bidirectional stream per worker**. The stream is
worker-initiated, because only the worker can dial across the tailnet (see
Deployment & Security):

- worker → coordinator: register/resync handshake (opening message), heartbeat
  keepalive, status updates, telemetry.
- coordinator → worker: control commands (resync, drain, cancel/kill).
- **Liveness becomes stream health** — a broken stream is an immediate, *symmetric*
  signal (both ends observe it), so neither side has to infer the peer vanished.
- **Job claim stays pull-based** even on the stream, to keep the
  no-orphaned-assignment property the recovery model relies on (a queued job is
  never handed out unless the worker asks for it).

Why gRPC and not WebSocket here: this boundary is Java↔Java and already on gRPC
with typed protos, deadlines, and tailnet auth. WebSocket stays on the
language-agnostic SDK↔worker boundary only (next section).

### Job (SDK) ↔ Worker — `job_callback.proto` (one WebSocket)

A single binary WebSocket per job container; each frame is prefixed by a one-byte
type tag.

```
Job container (SDK)                        WorkerAgent
  │  [0x01] StatusUpdate (task state)         │  → ReportStatus stream
  │ ────────────────────────────────────────► │
  │ ◄──────────────────────────────────────── │  [0x02] ack (status frames only)
  │                                           │
  │  [0x03] Report (metrics/events)           │  → ReportTelemetry stream
  │ ────────────────────────────────────────► │
  │                                           │
  │  [0x04] Liveness (ping)                   │  consumed locally for stall detection;
  │ ────────────────────────────────────────► │  not forwarded — the worker emits its
  │                                           │  own liveness Report on the telemetry
  │                                           │  stream so silent jobs still report
```

### Failure detection — who acts

```
Worker dies ─► heartbeats stop ─► coordinator monitor ─► job FAILED  (HEARTBEAT_LOST)
Job stalls  ─► no activity     ─► worker JobLivenessMonitor ─► kill ─► job KILLED (UNRESPONSIVE)
```

## How a Job Runs

1. Client sends `SubmitJobRequest` → `UserRequestHandler` → resolves input files (inline content uploaded to MinIO, URIs validated) → `JobManager.submit()` → job QUEUED
2. Worker calls `RegisterWorker` → `WorkerHandler` assigns a worker ID
3. Worker calls `PullJob` → `WorkerHandler` → `JobManager.claimNextJob()` → job STARTING
4. WorkerAgent stages input files from MinIO to `/tmp/jobs/<jobId>/input/`
5. WorkerAgent opens a gRPC status stream to the coordinator
6. WorkerAgent spawns: `docker run --rm -v input:/workspace/input:ro -v output:/workspace/output -e EXECUTION_PAYLOAD=<base64> <artifactUri>`
7. Inside the container, the SDK (`JobProcess.run()`) executes tasks sequentially, sending binary proto `StatusUpdate` messages to WorkerAgent's WebSocket server for each task (RUNNING → COMPLETED/FAILED)
8. WorkerAgent forwards each status update to the coordinator via the gRPC stream. On the first RUNNING task, it also sends a job-level RUNNING update.
9. When the container exits, WorkerAgent interprets the exit code (see Failure Handling) and sends a terminal job status
10. WorkerAgent uploads output files from `/tmp/jobs/<jobId>/output/` and `stdout.log` to MinIO
11. WorkerAgent cleans up temp directories, closes the gRPC stream, and loops back to step 3

## Failure Handling

### Exit codes

| Exit code | Interpretation |
|-----------|---------------|
| 0 | Job COMPLETED — all tasks succeeded |
| -1 | Job KILLED — process timed out (`jobExecutionTimeoutMinutes`, default 10), destroyed with `destroyForcibly()` |
| Any other | Job FAILED — "Job process exited with code N" |

If `spawnJobProcess` throws `IOException` (container failed to start) or `InterruptedException`, the job is marked FAILED with the exception message.

### Task failure

When a task throws an exception, the SDK marks it FAILED and returns immediately — remaining tasks are never started. The container exits, WorkerAgent sees the non-zero exit code and marks the job FAILED.

### Timeout

The timeout is configurable via `jobExecutionTimeoutMinutes` in `worker_config.yaml` (default 10). When the timeout expires, `process.waitFor()` returns false and the process is killed with `destroyForcibly()`.

### Worker crash

WorkerAgent sends periodic heartbeats to the coordinator (`heartbeatIntervalSeconds`). If a worker stops sending heartbeats (crash, network partition), the coordinator's heartbeat monitor detects the dead worker and fails all its in-flight jobs with `HEARTBEAT_LOST`. (Covered end-to-end by `IntegrationTest#testWorkerHeartbeatLost`.)

### Job stall detection

The **worker** owns job liveness (it sees the job's frames), via `JobLivenessMonitor`, one per running job:

- **Liveness signal** — every frame the SDK sends over the WebSocket (status, telemetry, or a periodic liveness ping) counts as activity and refreshes the job's last-activity time.
- **Cadence** — the monitor ticks every `liveness.pingIntervalSeconds` (default 15s).
- **The check** — a job is *unresponsive* when either: it shows **no activity within `startupTimeoutSeconds`** of launch (default 30s, never started), **or** it goes **silent for `maxMissedPings × pingIntervalSeconds`** after starting (default 3 × 15s = 45s).
- **Action & owner** — on a stall, **the worker** (not the coordinator) gracefully stops the container (`shutdownGraceSeconds` grace) when `autoKill` is set, then reports the job `KILLED` / `UNRESPONSIVE`. Disabling `autoKill` keeps detection (surfaced as last-activity) but takes no action. (Covered by `IntegrationTest#testJobStallUnresponsive`.)

### When the service kills a container

Three triggers can end a running container. Two are worker-driven (it owns the job); the third is the coordinator acting only because the worker is gone.

| Trigger | Detected by | Config (default) | Action | Status propagated |
|---------|-------------|------------------|--------|-------------------|
| **Job stall** (unresponsive) | Worker — `JobLivenessMonitor` | `liveness.startupTimeoutSeconds` (30), `pingIntervalSeconds` (15), `maxMissedPings` (3), `shutdownGraceSeconds` (10), `autoKill` (true) | Graceful container stop | `KILLED` / `UNRESPONSIVE` |
| **Hard deadline** (timeout) | Worker — `JobLauncher` | `jobExecutionTimeoutMinutes` (10) | `destroyForcibly()` | `TIMEOUT` (kill in flight) → `KILLED` / `PROCESS_TIMEOUT` |
| **Worker dead** (heartbeat lost) | Coordinator — heartbeat monitor | worker `heartbeatIntervalSeconds` (5); coordinator `heartbeatTimeoutSeconds` (15), `heartbeatScanIntervalSeconds` (5) | No container kill — worker is gone | job `FAILED` / `HEARTBEAT_LOST` |

`TIMEOUT` precedes `KILLED` so the coordinator shows *why* the kill started before the kill is confirmed. The worker-dead case never kills a container (the coordinator can't reach it) — it only fails the job state.

### Coordinator idempotency

The coordinator ignores status updates for jobs already in a terminal state (COMPLETED, FAILED, KILLED, CANCELLED). Duplicate RUNNING updates are also safe — `canTransitionTo` rejects same-state transitions.

## Worker Identity

Each worker owns a **stable id** that survives its own restarts. On first boot the
worker generates a UUID and writes it to a local checkpoint file
(`worker_checkpoint.yaml`, path from `worker_config.yaml` → `checkpointPath`); on
every later boot it reads the same id back and sends it on every `RegisterWorker`.
The checkpoint file is the worker's own local state — distinct from the
user-authored `worker_config.yaml`.

Why it matters: the coordinator persists each in-flight job's **assigned worker
id**. After a restart of either side, the coordinator has to match a
re-registering worker to the jobs it owns. A fresh random id per boot would make
every reconnect look like a brand-new worker — the coordinator could never
distinguish "the same worker came back" from "a different worker appeared," and
would wrongly fail a live worker's jobs while seeding its registry. A stable,
worker-owned id is what makes reconnect-and-reconcile correct (matching is still
done by job id, so the stable id is an optimization that avoids ambiguity, not the
sole correctness lever).

One agent runs per host, so the id is just a generated UUID — there is no
host/machine-id derivation.

## Coordinator Failover & State Persistence

The coordinator keeps job state in memory for speed but **mirrors it to an embedded SQLite database**, written through on every state transition, so a restart recovers rather than losing everything.

- **Why embedded SQLite (not Postgres):** there is exactly **one** coordinator by design (see non-goals). On a reliable host, a single local file gives ACID durability with no external dependency or network hop. Postgres would only be warranted for multiple coordinators or off-host durability — neither of which this architecture needs.
- **What's persisted:** each job's definition (image, params, resources, inputs), lifecycle state + timestamps + failure info, per-task status, and its assigned worker; plus the **worker registry** (id + placement info), mirrored on register and removed on eviction so the heartbeat monitor has a worker list to watch immediately on restart.
- **What's not** (regenerated after restart): job liveness and live telemetry (resume on the next report), and the queue (rebuilt from `QUEUED` jobs).
- **Memory vs DB:** only **active** (non-terminal) jobs are held in memory; terminal jobs are served from SQLite (e.g. the UI's job list).
- **On restart:** non-terminal jobs are reloaded; in-flight jobs are held "pending reconciliation" through a startup grace window while workers re-register and resync their current task state (matched by job ID), so healthy jobs aren't prematurely failed.

### When the database is written

Writes happen only on a **state transition**, through the one synchronized path in `JobManager`:

- **Job submitted** → row inserted as `QUEUED` (no worker yet).
- **Job claimed by a worker** → `STARTING`, with the assigned worker id recorded.
- **Job state change** → `RUNNING` / `COMPLETED` / `FAILED` / `TIMEOUT` / `KILLED` / `CANCELLED`.
- **Task state change** → the job row is re-saved with the updated task (even when the job's own state doesn't change).
- **Worker heartbeat lost** → the coordinator writes the job `FAILED` (`HEARTBEAT_LOST`).
- **Worker registers / is evicted** → the worker row is written / deleted (a separate `workers` table in the same db file).
- **Retention sweep** → terminal rows past `retentionDays` are deleted.

Not written (regenerated, not persisted): telemetry/liveness reports, and duplicate no-op status updates (e.g. a repeated `RUNNING`).

### Job retention

Terminal jobs are kept for `retentionDays` (default 7). A periodic sweep **deletes** jobs whose `completed_at` is older than the window, keeping the `jobs` table bounded.

### Storage abstraction

There's nothing to "bring up" for SQLite — it's an embedded library, not a server: no process, port, or credentials. The coordinator opens the file at `coordinator.dbPath` on startup and creates the schema on first open. The store sits behind a `JobStore` interface that speaks only domain types (`JobStatus`); `SqliteJobStore` is the only place that knows SQL or schema, and `JobManager` depends on the interface. Swapping to Postgres (or another backend) is one new `JobStore` implementation, injected at startup — no changes to `JobManager` or the core.

## Input/Output Files

### Input files

Clients attach input files to `SubmitJobRequest` in two ways:

- **Inline content** (`bytes content`) — the coordinator uploads the bytes to MinIO under `jobs/<jobId>/input/<name>` and stores the resulting URI
- **URI reference** (`string uri`) — the coordinator validates the URI exists in MinIO and passes it through

By the time an `InputFile` reaches the domain layer, it always has a URI — inline content is a transport concern resolved at the RPC boundary.

WorkerAgent downloads all input files to `/tmp/jobs/<jobId>/input/` and mounts that directory read-only into the container at `/workspace/input`.

### Output files

The container writes output to `/workspace/output` (mounted read-write). After the container exits, WorkerAgent uploads everything under that directory to MinIO at `jobs/<jobId>/output/`. The stdout log is uploaded to `jobs/<jobId>/logs/stdout.log`.

Clients retrieve outputs via `ListJobFiles` and `GetJobOutput` (server-streaming for large files).

### ObjectStore

`ObjectStore` is a thin wrapper around `S3Client` for MinIO. Used by both coordinator (upload inline input files, serve output downloads) and worker (download inputs, upload outputs and logs).

## SDK

The SDK lives in the [scheduler-sdk](../scheduler-sdk) repository and supports both Java and Python.

**Java**: Job authors implement `Task` and call `JobProcess.run()` from their container's main method:

```java
public class MyEtlJob {
    public static void main(String[] args) {
        JobProcess.run(List.of(
            new ExtractTask(),
            new TransformTask(),
            new LoadTask()
        ));
    }
}
```

Each `Task` receives a `TaskContext` with `progress(fraction, message)` and `metric(name, value)` for structured reporting. The SDK reads the `EXECUTION_PAYLOAD` environment variable (base64-encoded JSON with `workerAgentUrl`, `jobId`, `params`) set by WorkerAgent.

**Python**: The `py-sdk` provides an equivalent `job_runner` module.

## Blocking Execution

WorkerAgent runs one job at a time — `executeJob` blocks on `process.waitFor()` until the container exits. To run multiple jobs concurrently, the main loop would need to submit `executeJob` calls to a thread pool and multiplex the task status HTTP handler by job ID.

## Build

```bash
mvn compile                      # compile all modules
mvn test                         # run tests
mvn compile -pl scheduler-proto  # regenerate proto code after .proto changes
```

## Configuration

Each process is configured by **one YAML file, located via an env var** — no CLI
flags, no second source. Built-in defaults apply per key and the file overrides
them; required fields with no sensible default (hostnames, MinIO, worker
resources) are validated at startup and the process refuses to start if missing
or if the file fails to parse.

| Process | Env var | File |
|---------|---------|------|
| Coordinator | `CONTROL_PLANE_CONFIG` | `control_plane_config.yaml` |
| Worker | `WORKER_CONFIG` | `worker_config.yaml` |
| CLI | `CONTROL_PLANE_CONFIG` | `control_plane_config.yaml` (coordinator endpoint + readiness URLs) |

The worker is **not** part of the control plane, so its settings live in
`worker_config.yaml`, never in `control_plane_config.yaml`.

### Coordinator — `control_plane_config.yaml`

| Key | Meaning |
|-----|---------|
| `coordinator.port` | gRPC port (metrics served on `port+1`) |
| `coordinator.heartbeatTimeoutSeconds` | declare a worker dead after this long without a heartbeat |
| `coordinator.heartbeatScanIntervalSeconds` | how often the monitor scans for dead workers |
| `minio.endpoint` / `accessKey` / `secretKey` / `bucket` | object store connection |
| `mlflow.enabled`, `metrics.enabled`, `*.port`, `registry.url` | infra-stack toggles/ports for `control-plane.sh` + CLI readiness |

### Worker — `worker_config.yaml`

| Key | Meaning |
|-----|---------|
| `coordinator.host` / `port` | where to reach the coordinator |
| `coordinator.heartbeatIntervalSeconds` | heartbeat send period (keep below the coordinator's timeout) |
| `coordinator.pollIntervalSeconds` | how often the worker polls for a job to claim |
| `hostname` | address advertised to the coordinator |
| `port` | WebSocket callback port (`0` = ephemeral) |
| `jobExecutionTimeoutMinutes` | hard deadline before the worker kills a job (TIMEOUT) |
| `resources.memory` / `cpu` / `gpu` / `capabilities` | what the worker advertises for placement |
| `docker.network` | docker network the job containers join |
| `minio.*` | object store connection (fetch inputs, upload outputs) |
| `mlflow.trackingUri` | MLflow URI injected into job containers |
| `liveness.startupTimeoutSeconds` | job must show activity within this of launch |
| `liveness.pingIntervalSeconds` | stall-check tick + liveness-forward cadence (match the SDK ping rate) |
| `liveness.maxMissedPings` | consecutive missed ping windows before "unresponsive" |
| `liveness.autoKill` | kill the container when flagged unresponsive |
| `liveness.shutdownGraceSeconds` | SIGTERM→SIGKILL grace on a graceful stop |

`scripts/control-plane.sh up|down|status` derives compose profiles/variables from
`control_plane_config.yaml` and logs the resolved compose to `.control-plane-resolved.yml`.

## Metrics & Observability

Three separate planes — don't conflate them:

| Plane | What | Emitted by | Viewed in |
|-------|------|-----------|-----------|
| **Infra metrics** | machine + scheduling stats (CPU/GPU/mem, queue, throughput) | worker + coordinator | Prometheus / Grafana |
| **Job telemetry** | "what's happening now" — progress, metrics, events | SDK (`ctx.progress/metric/event`) → worker → coordinator | `GetJobStatus` / CLI |
| **Training history** | per-step curves, params, artifacts | the job, directly | MLflow |

The **SDK never emits infra metrics** — the worker observes containers from the
outside and the coordinator measures scheduling; job telemetry and MLflow are the
SDK's only observability paths. So the metrics work touched the **coordinator** and
**worker** only.

### Infra metrics (Prometheus / Grafana)

Each component serves a Prometheus endpoint that is **always on**; the `metrics`
profile only controls whether Prometheus/Grafana run to scrape and display them.

```
   Coordinator  :9091/metrics ─┐
   (gRPC port+1)                ├──► Prometheus :9095 ──► Grafana :3000
   Worker       :9092/metrics ─┘                          ("Scheduler" dashboard)
   (docker stats, nvidia-smi)
```

- **Coordinator** (`CoordinatorMetrics`, `:9091`): counters for jobs
  submitted / finished-by-outcome / telemetry reports / heartbeat losses; a
  queue-wait histogram; scrape-time gauges for jobs-by-status, queue depth, and
  registered workers (read live from `JobManager`, never stale).
- **Worker** (`WorkerMetrics`, `:9092`): a running-jobs gauge, a job-duration
  histogram by outcome, per-container CPU/memory from `docker stats` (10s
  sampling), and host GPU util/memory from `nvidia-smi` (only when
  `resources.gpu: true`). Per-job series are dropped when the job finishes.

Full metric tables: [docs/metrics.md](docs/metrics.md).

### Planned

- `scheduler_job_stalled{job_id}` on the coordinator — a RUNNING job with no
  telemetry past a threshold. Now feasible: the worker forwards last-activity on
  the telemetry stream (see Protocol Exchanges).

## External Dependencies

The integration tests in `scheduler-worker` depend on `scheduler-client` from the [scheduler-sdk](../scheduler-sdk) repo. Install it to your local Maven repository before running tests:

```bash
cd ../scheduler-sdk
mvn install -pl scheduler-client
```

## Proto JAR

`scheduler-proto` is the single source of truth for all protobuf definitions and generated gRPC stubs. It is consumed by both the scheduler infrastructure (as a sibling module) and the [scheduler-sdk](../scheduler-sdk) `scheduler-client` module (as a Maven dependency from local `~/.m2`).

```
scheduler-proto (source of truth)
  │  mvn install → ~/.m2/repository
  │
  ├── scheduler-coordinator  (sibling module, resolved by reactor)
  └── scheduler-client       (separate repo, resolved from ~/.m2)
```

After changing any `.proto` file, republish to local Maven:

```bash
mvn install -pl scheduler-proto
```

Then rebuild downstream consumers:

```bash
cd ../scheduler-sdk
mvn compile -pl scheduler-client
```
