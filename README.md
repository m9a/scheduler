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
            FAILED      ↘ KILLED
```

| State | Set by | Trigger |
|-------|--------|---------|
| QUEUED | coordinator | `submit()` accepts the job |
| STARTING | coordinator | a worker claims it |
| RUNNING | worker | first task reports — the worker stamps RUNNING on every task update; the coordinator de-dupes |
| COMPLETED | worker | container exit 0 |
| FAILED | worker | container exit ≠ 0 (a task that threw exits the container non-zero), or `docker run` failed to start |
| FAILED | coordinator | worker missed health checks (heartbeat lost) — the only state the coordinator writes |
| KILLED | worker | container killed for unresponsiveness (stall detection), or future cancel |
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

Unary register/claim/heartbeat, two per-job client streams (status + telemetry),
and two coordinator→worker command streams (system + job).

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
  │                                           │
  │  SystemCommands(workerId)   (subscribe)   │  push: resync, drain
  │ ◄════════════════════════════════════════ │
  │                                           │
  │  JobCommands(workerId)      (subscribe)   │  push: cancel, preempt
  │ ◄════════════════════════════════════════ │
```

### Transport & protocol choices

The protocol mixes RPC shapes by interaction:

- **Unary** for request/response: register, claim, heartbeat.
- **Client streams** for worker→coordinator data: status, telemetry.
- **Server streams** for coordinator→worker push. The worker subscribes once; the
  coordinator pushes commands when it needs to act.

Push is split into **two** command streams by originator:

- **System commands** — coordinator-driven worker lifecycle (resync, drain).
- **Job commands** — user- or policy-driven, per job (cancel, preempt).

They stay separate so the two planes read cleanly and can be authorized and audited
on their own. Both are one-way (coordinator → worker); replies ride existing calls
(resync → `RegisterWorker`, cancel → terminal status on `ReportStatus`).

**Job claim stays pull-based.** A queued job is never handed out unless the worker
asks, so a coordinator restart can't orphan an assignment.

All of this rides **one HTTP/2 connection** per worker (gRPC multiplexes streams).
The worker dials the coordinator — only the worker can, across the tailnet (see
Deployment & Security). Liveness is the heartbeat plus the command stream's health.

### Coordinator → worker push (the trick)

gRPC clients always initiate a call — a server cannot dial a client. The worker is
the client, so to let the **coordinator** push (resync, drain, cancel, preempt) the
worker opens a **server-streaming** RPC and holds it open: it sends one
`SubscribeRequest`, then the coordinator sends commands down that stream whenever it
needs to. The worker runs no server of its own. If the stream drops, the worker
**resubscribes** (opens a fresh one) after a short delay.

### Coordinator Threading Model

The coordinator is a gRPC **server**, so it runs **one thread per in-flight RPC**,
plus background threads (e.g. the heartbeat monitor). It is not single-threaded.

A single worker's command stream can therefore be written from **several threads at
once**:

- a client's **cancel** request, on its own RPC thread;
- the **heartbeat monitor**, pushing resync/drain;
- **boot recovery**, requesting resync.

Sending a command means calling `onNext`, which writes the message as frames on that
one stream. `StreamObserver` is **not thread-safe**: if two threads call `onNext` on
the same stream at once, their frames interleave and the message is corrupted. So
each worker's stream **serializes its sends** (one lock per stream).

### Why gRPC

- Java↔Java control plane: typed stubs generated both ends, no hand-rolled framing.
- Many independent streams over one connection — clean plane separation, per-stream
  flow control.
- Deadlines, cancellation, and auth interceptors are built in.
- A WebSocket would be one byte channel — everything multiplexed by hand, back to a
  single envelope. We keep WebSocket for the polyglot SDK↔worker hop only, where a
  simple socket beats pulling gRPC into every SDK.

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
  │ ────────────────────────────────────────► │  never forwarded. The worker's stall
  │                                           │  detection is entirely local.
```

The worker re-stamps each `[0x03]` Report with the job's **last-liveness time** — owned by the `JobLivenessMonitor` (the epoch millis of the last frame it saw) — before forwarding, so the coordinator's last-activity comes from real telemetry, not a synthetic liveness Report. A job that stays silent — only sending `[0x04]` pings — surfaces no last-activity to the coordinator for now; a future cadence-based forwarder will (TODO.md #23).

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
6. WorkerAgent launches the container **detached**: `docker run -d --name job-<jobId> -v input:/workspace/input:ro -v output:/workspace/output -e EXECUTION_PAYLOAD=<base64> <artifactUri>`
7. Inside the container, the SDK (`JobProcess.run()`) reads `EXECUTION_PAYLOAD` (which carries the worker's WebSocket URL), dials the worker, then executes tasks sequentially, sending binary proto `StatusUpdate` messages for each task (RUNNING → COMPLETED/FAILED)
8. WorkerAgent forwards each status update to the coordinator via the gRPC stream. On the first RUNNING task, it also sends a job-level RUNNING update.
9. WorkerAgent blocks on `docker wait job-<jobId>`; the exit code it prints decides the terminal job status (see Failure Handling), sent on the same gRPC status stream
10. WorkerAgent uploads output files from `/tmp/jobs/<jobId>/output/` and `stdout.log` to MinIO
11. WorkerAgent removes the container (`docker rm`), cleans up temp directories, closes the gRPC stream, and loops back to step 3

### Job vs container lifecycle (detached mode)

The job and its container have **separate lifecycles**, on purpose.

- The container is launched with `docker run -d` and **owned by the Docker daemon**,
  not by the worker process. The worker is an observer, not the parent.
- Everything the worker needs is addressed by the container's **name** (`job-<jobId>`,
  derived from the job id — nothing extra to persist):
  - **Logs** — a follower thread runs `docker logs -f`, teeing each line to the worker
    log and to `stdout.log` (uploaded to MinIO at job end).
  - **Exit code** — `docker wait` blocks until the container stops and prints its exit
    code. It also answers if the container *already* exited. The worker maps that code
    to the terminal `StatusUpdate` (COMPLETED / FAILED) it sends the coordinator.
  - **Stats** — `WorkerMetrics` samples `docker stats` (and `nvidia-smi`) by name; see
    Metrics & Observability.
- **Why detached:** if the worker process dies, the container — a long training run —
  keeps running, and an exited container's exit code stays readable. A restarted worker
  can find the container by name and resume watching it with the *same* `docker wait` /
  `docker logs` calls it uses normally (re-attach — being built, see worker-recovery.md).
- **Container removal:** the worker removes the container explicitly, only after it has
  what it needs — after reading the exit code (or after a stall/cancel kill). There is
  no `--rm`; a container never silently deletes its own evidence.
- The SDK's WebSocket is **initiated by the SDK** (the worker's URL travels in
  `EXECUTION_PAYLOAD`), so a container can in principle re-dial a restarted worker —
  that reconnect is part of the re-attach work.

### Worker process model (docker CLI children)

Each docker step (`run -d`, `wait`, `logs -f`, `inspect`, `rm`) runs the docker CLI
as a **child OS process of the worker's JVM** (`ProcessBuilder` = fork/exec). The CLI
is a thin client: it sends one request to the Docker daemon over its socket and
streams back the answer.

- The **container is not one of these children**. The daemon runs it. Killing the
  worker and its CLI children does not touch the container.
- If the JVM dies, its child CLI processes are not automatically killed — the OS
  orphans them and they exit on their own. Either way it doesn't matter: they only
  watch, they own nothing.
- So `docker wait` is just a watcher. A worker crash means nobody is watching; the
  container keeps running. A restarted worker starts a fresh `docker wait` by name.
- The log follower is a worker **thread** that runs one `docker logs -f` child and
  copies each line into `stdout.log`.

### Container removal and logs

`docker rm` deletes the container **and its daemon-side log storage** — lines not yet
copied out are gone. So the order at job end is fixed: the log follower flushes the
remaining lines into `stdout.log` first, then the container is removed. `stdout.log`
(uploaded to MinIO) is the durable copy; the daemon's copy is disposable.

## Failure Handling

### Exit codes

The exit code is read from `docker wait` (the daemon reports it when the container
stops) and mapped by the worker to the terminal `StatusUpdate` it sends the
coordinator on the job's gRPC status stream:

| Exit code | Interpretation |
|-----------|---------------|
| 0 | Job COMPLETED — all tasks succeeded |
| Any other | Job FAILED — "Job process exited with code N" (unless the liveness monitor killed the container → KILLED / UNRESPONSIVE) |

If `spawnJobProcess` throws `IOException` (`docker run -d` failed — bad image, daemon
error) or `InterruptedException`, the job is marked FAILED with the exception message.

### Task failure

When a task throws an exception, the SDK marks it FAILED and returns immediately — remaining tasks are never started. The container exits, WorkerAgent sees the non-zero exit code and marks the job FAILED.

### Timeouts

There is no run deadline. A training job may run for days. Its duration cannot be
predicted, so nothing caps it. The guards below watch behavior instead.

| Timeout | Config (default) | What it watches | Who acts | On expiry | Job/container state change |
|---------|------------------|-----------------|----------|-----------|----------------------------|
| Image pull | worker `docker.imagePullTimeoutMinutes` (10 min) | `docker run -d`, including the image pull | JobLauncher (worker) | Abort the start. Remove any half-created container. | STARTING → FAILED (`PROCESS_START_FAILED`) |
| Job startup | worker `liveness.startupTimeoutSeconds` (30 s) | First SDK activity after launch | JobLivenessMonitor (worker) | Gracefully stop the container (when `autoKill`) | STARTING/RUNNING → KILLED (`UNRESPONSIVE`) |
| Job silence | worker `liveness.maxMissedPings` × `pingIntervalSeconds` (3 × 15 s = 45 s) | SDK activity after the first frame | JobLivenessMonitor (worker) | Gracefully stop the container (when `autoKill`) | RUNNING → KILLED (`UNRESPONSIVE`) |
| Stop grace | worker `liveness.shutdownGraceSeconds` (10 s) | SIGTERM → SIGKILL window inside any graceful stop | Docker (told by JobLauncher) | Force kill | none — part of the stop |
| Worker silence | coordinator `heartbeatTimeoutSeconds` (300 s) | Time since the worker's last heartbeat | Heartbeat monitor (coordinator) | Evict the worker. Fail its jobs. The container is not killed — it keeps running, unwatched. | RUNNING → FAILED (`HEARTBEAT_LOST`) |
| Boot grace | coordinator `reregistrationTimeoutSeconds` (30 s) | Time workers get to re-register after a coordinator restart | Heartbeat monitor (coordinator) | Sweeps begin | none |

One missed heartbeat changes nothing. Silence is what counts: ~3 consecutive
missed 5 s beats reach the 15 s budget and trigger eviction. Eviction does not
drain the worker — drain is a separate command.

Cadences, for reference (they pace checks, they don't expire): worker heartbeat
send `heartbeatIntervalSeconds` (5 s) · coordinator sweep
`heartbeatScanIntervalSeconds` (5 s) · job pull `pollIntervalSeconds` (5 s) ·
liveness tick `pingIntervalSeconds` (15 s).

### Worker health check (heartbeats)

Two clocks, one on each side:

- **Worker sends.** A background loop sends `Heartbeat` every
  `heartbeatIntervalSeconds` (default 5).
- **Coordinator watches.** Each received beat stamps the worker's `lastHeartbeat` —
  the silence clock restarts on every beat.
- **Monitor sweeps.** Every `heartbeatScanIntervalSeconds` (default 5) the monitor
  scans the registry. A worker silent longer than `heartbeatTimeoutSeconds`
  (default 15) is evicted and its in-flight jobs are failed (`HEARTBEAT_LOST`).
- **No explicit miss counter.** The timeout *is* the miss budget:
  `timeout / interval` ≈ consecutive missed beats tolerated (15 / 5 = 3).
- **Rule of thumb.** Keep `timeout ≥ 3 × interval`, so one lost packet or a GC
  pause doesn't kill a healthy worker.
- **Boot grace.** After a coordinator restart the first sweep waits
  `reregistrationTimeoutSeconds` (default 30), so seeded workers can re-register
  before any eviction.

```
Worker                                  Coordinator
  │ ── Heartbeat ──────────────────────► lastHeartbeat = now
  │      every heartbeatIntervalSeconds (5)
  │ ── Heartbeat ──────────────────────► lastHeartbeat = now
  │                                          │
  ✕ (crash / partition)                      │ monitor sweep, every
                                             │ heartbeatScanIntervalSeconds (5):
                                             │ now − lastHeartbeat > 15s ?
                                             ▼
                                       evict worker; jobs → FAILED (HEARTBEAT_LOST)
```

This is worker-level health only. Job-level stall detection is separate and
worker-owned (`JobLivenessMonitor`, which *does* have an explicit miss counter,
`liveness.maxMissedPings`) — see "Job stall detection".

### Worker crash

If a worker stops sending heartbeats (crash, network partition), the health check
above fails all its in-flight jobs with `HEARTBEAT_LOST`. (Covered end-to-end by
`IntegrationTest#testWorkerHeartbeatLost`.)

The job's **container is not affected** by the worker dying: it runs detached under the
Docker daemon and keeps going (see "Job vs container lifecycle"). Today the job is
still failed via `HEARTBEAT_LOST` if the worker stays down past the timeout; recovery
that re-attaches a restarted worker to its still-running containers is in progress
(worker-recovery.md).

### Job stall detection

The **worker** owns job liveness (it sees the job's frames), via `JobLivenessMonitor`, one per running job:

- **Liveness signal** — every frame the SDK sends over the WebSocket (status, telemetry, or a periodic liveness ping) counts as activity and refreshes the monitor's `lastLivenessAt` (epoch millis of the last frame). This is worker-local for stall detection; the same value is stamped onto forwarded telemetry so the coordinator can surface it.
- **Cadence** — the monitor ticks every `liveness.pingIntervalSeconds` (default 15s).
- **The check** — a job is *unresponsive* when either: it shows **no activity within `startupTimeoutSeconds`** of launch (default 30s, never started), **or** it goes **silent for `maxMissedPings × pingIntervalSeconds`** after starting (default 3 × 15s = 45s).
- **Action & owner** — on a stall, **the worker** (not the coordinator) gracefully stops the container (`shutdownGraceSeconds` grace) when `autoKill` is set, then reports the job `KILLED` / `UNRESPONSIVE`. Disabling `autoKill` keeps detection but takes no action. (Covered by `IntegrationTest#testJobStallUnresponsive`.)

### When the service kills a container

Two triggers can end a running container. One is worker-driven (it owns the job); the other is the coordinator acting only because the worker is gone. There is no run deadline (see "Timeouts").

| Trigger | Detected by | Config (default) | Action | Status propagated |
|---------|-------------|------------------|--------|-------------------|
| **Job stall** (unresponsive) | Worker — `JobLivenessMonitor` | `liveness.startupTimeoutSeconds` (30), `pingIntervalSeconds` (15), `maxMissedPings` (3), `shutdownGraceSeconds` (10), `autoKill` (true) | Graceful container stop | `KILLED` / `UNRESPONSIVE` |
| **Worker dead** (heartbeat lost) | Coordinator — heartbeat monitor | worker `heartbeatIntervalSeconds` (5); coordinator `heartbeatTimeoutSeconds` (300), `heartbeatScanIntervalSeconds` (5) | No container kill — worker is gone | job `FAILED` / `HEARTBEAT_LOST` |

The worker-dead case never kills a container (the coordinator can't reach it) — it only fails the job state.

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

## Worker Lifecycle

The worker is a client of the coordinator. On startup (`WorkerAgent.run`):

1. **Restore identity** — read the stable `workerId` from `worker_checkpoint.yaml`
   (created on first boot). See Worker Identity.
2. **Register** — unary `RegisterWorker(workerId, resources)`. This blocks until it
   succeeds; everything below depends on it.
3. **Start heartbeat** — a background loop sends `Heartbeat` every few seconds so the
   coordinator's monitor knows the worker is alive.
4. **Subscribe to commands** — open the two server-streams (`SystemCommands`,
   `JobCommands`). The coordinator pushes on them. On stream error/close the worker
   **resubscribes** after a short delay, until it reconnects or shuts down.
5. **Claim jobs** — loop: `PullJob`; run the job; repeat. Skipped while draining.

Streams come in two scopes:

- **One per worker** (long-lived): the two command streams.
- **One per job**: `ReportStatus` and `ReportTelemetry`, opened while a job runs.

**Re-registration** happens either when the worker restarts, or when the coordinator
pushes a `Resync` command to a still-running worker (the worker calls `RegisterWorker`
again). It is distinct from **resubscribe**, which only re-opens a dropped command
stream and does not re-register.

If the coordinator is down at startup, `register` fails and the worker exits; the
supervisor (Docker restart policy) relaunches it until registration succeeds.

## Coordinator Failover & State Persistence

The coordinator keeps job state in memory for speed but **mirrors it to an embedded SQLite database**, written through on every state transition, so a restart recovers rather than losing everything.

- **Why embedded SQLite (not Postgres):** there is exactly **one** coordinator by design (see non-goals). On a reliable host, a single local file gives ACID durability with no external dependency or network hop. Postgres would only be warranted for multiple coordinators or off-host durability — neither of which this architecture needs.
- **What's persisted:** each job's definition (image, params, resources, inputs), lifecycle state + timestamps + failure info, per-task status, and its assigned worker; plus the **worker registry** (id + placement info), mirrored on register and removed on eviction so the heartbeat monitor has a worker list to watch immediately on restart.
- **What's not** (regenerated after restart): job liveness and live telemetry (resume on the next report), and the queue (rebuilt from `QUEUED` jobs).
- **Memory vs DB:** only **active** (non-terminal) jobs are held in memory; terminal jobs are served from SQLite (e.g. the UI's job list).
- **On restart:** non-terminal jobs are reloaded (`QUEUED` re-queued, in-flight jobs' worker assignments rebuilt) and the worker registry is seeded from the store. The heartbeat monitor holds off for `reregistrationTimeoutSeconds` so workers can reconnect before any in-flight job is failed. (Job-level resync — workers re-declaring their current jobs — is the next step.)

### When the database is written

Writes happen only on a **state transition**, through the one synchronized path in `JobManager`:

- **Job submitted** → row inserted as `QUEUED` (no worker yet).
- **Job claimed by a worker** → `STARTING`, with the assigned worker id recorded.
- **Job state change** → `RUNNING` / `COMPLETED` / `FAILED` / `KILLED` / `CANCELLED`.
- **Task state change** → the job row is re-saved with the updated task (even when the job's own state doesn't change).
- **Worker heartbeat lost** → the coordinator writes the job `FAILED` (`HEARTBEAT_LOST`).
- **Worker registers / is evicted** → the worker row is written / deleted (a separate `workers` table in the same db file).
- **Retention sweep** → terminal rows past `retentionDays` are deleted.

Not written (regenerated, not persisted): telemetry/liveness reports, and duplicate no-op status updates (e.g. a repeated `RUNNING`).

### Job retention

Terminal jobs are kept for `retentionDays` (default 7). A periodic sweep **deletes** jobs whose `completed_at` is older than the window, keeping the `jobs` table bounded.

### Storage abstraction

There's nothing to "bring up" for SQLite — it's an embedded library, not a server: no process, port, or credentials. The coordinator opens the file at `coordinator.dbPath` on startup and creates the schema on first open. The store sits behind a `JobStore` interface that speaks only domain types (`JobStatus`); `SqliteJobStore` is the only place that knows SQL or schema, and `JobManager` depends on the interface. Swapping to Postgres (or another backend) is one new `JobStore` implementation, injected at startup — no changes to `JobManager` or the core.

## Worker Failover & Recovery

When the worker **process** dies (crash, OOM, upgrade, host reboot), its job containers
keep running — the Docker daemon owns them, not the worker (see "Job vs container
lifecycle"). Recovery is how a restarted worker takes those jobs back instead of
leaking or re-running them.

Worker-side recovery is **implemented**: detached containers, the durable status
store with coordinator-acked cleanup, boot recovery with re-attach, best-effort
failure of lost containers, and SDK WebSocket reconnect (sibling repo). Still open:
coordinator reconciliation after a slow restart. Full plan in worker-recovery.md.

### Design choices and trade-offs

- **Re-attach, not relaunch.** Training jobs are not idempotent: they write model and
  checkpoint files as they go. Re-running one on the same output dir corrupts or
  overwrites partial results. So a restarted worker resumes watching the running
  container. A container that is genuinely gone fails the job — the user decides what
  to do with partial output. Nothing is ever silently re-run.
- **Detached containers are the enabler.** A foreground `docker run --rm` child dies
  with the worker and deletes its own exit code. Detached (`docker run -d`, no `--rm`),
  the container survives the worker, and normal watching and re-attach use the *same*
  calls (`docker wait` / `docker logs` by name). One mechanism, not two.
- **The status store is the memory.** The worker's SQLite status store (one row per
  job/task, written through before every send) is what a restarted worker reads to
  know which jobs it owned. No store row → the worker never claimed it.
- **The container is the truth.** Recovery trusts `docker inspect`, not the stored
  state: the store says what the job *was* doing, the daemon says what the container
  *is* doing. Decisions come from the live answer.
- **Rows leave the store only on the coordinator's ack.** One coarse ack per job:
  when the worker closes the job's `ReportStatus` stream, the coordinator — having
  applied every update on it, including the terminal one — sends one close-ack
  response. Only that ack triggers `store.ack(jobId)`, which drops the job's rows.
  A timeout or a stream error before the ack keeps the rows; the register flush
  re-delivers them on the next connect. So a job's rows always mean "the
  coordinator may not have this yet."

### Boot recovery

On boot, before registering, the worker runs `recover()`: read the status store, and
for each non-terminal job inspect its container (`ContainerState`):

| Container | Decision |
|-----------|----------|
| RUNNING | Re-attach: resume `docker wait`, restart liveness, re-open streams |
| EXITED | Fail best-effort: salvage logs + outputs, report FAILED / `NOT_FOUND_ON_RECOVERY`, remove the container after the ack |
| ABSENT | Fail best-effort: salvage leftover outputs, report FAILED / `NOT_FOUND_ON_RECOVERY`, never relaunch |

Terminal rows in the store are skipped — they only wait for the register flush to
deliver them. Boot order: resolve worker id → `recover()` → register → heartbeat →
subscribe → re-attach running jobs → pull loop.

### Register reconciliation (slow restart)

A worker down longer than `heartbeatTimeoutSeconds` has already lost its jobs:
the coordinator's monitor failed them as `HEARTBEAT_LOST`. The returning worker
must not resurrect them.

- At register, the worker sends the jobs it still holds (`known_jobs`).
- The coordinator answers with `dead_job_ids`: the held jobs it already marked
  terminal. An unknown job id is never called dead — the coordinator does not
  tell a worker to discard work on missing data.
- The worker **discards** each dead job before acting on any recovery decision:
  a running container is stopped, outputs and logs are salvaged, temp dirs are
  cleaned. It is never re-attached or re-asserted — the coordinator's verdict is
  final, and the user may have already resubmitted the job.
- The discard report is dropped by the coordinator (the job is terminal); it
  exists to draw the close ack that clears the worker's store rows.

The default `heartbeatTimeoutSeconds` is 300 — deliberately generous, so a
worker upgrade or host reboot re-attaches instead of losing jobs. This mirrors
what Kubernetes and YARN do: short outage → work-preserving recovery; declared
dead → fence the orphan.

### Re-attach

A RUNNING decision re-runs the same per-job wiring a fresh job gets, minus staging
and `docker run`: re-open the status and telemetry streams, start a fresh liveness
monitor (its startup window gives the briefly-unwatched container time to ping
again), rebind the WebSocket handlers, and block on `docker wait job-{id}`. The
worker first re-asserts job RUNNING — the crash may have happened before any task
update, leaving the coordinator at STARTING. From there the job finishes exactly
like a normal one: terminal report, output upload, container removal, store ack.

The job's SDK notices the dead WebSocket at its next send (a liveness ping at the
latest) and reconnects — the worker's URL is stable across a restart. Status
updates queue in the SDK until the worker acks them, so transitions made during
the gap arrive once the connection is back. See "SDK delivery guarantees" in the
scheduler-sdk README.

### Lost containers (`NOT_FOUND_ON_RECOVERY`)

A recovered job whose container is not running is failed **best-effort** with one
coarse reason, `FAILURE_REASON_NOT_FOUND_ON_RECOVERY`. The worker does not try to
reconstruct what happened — **the user reads the job's checkpoint to determine
where it stopped** and whether to re-run. Work done so far is salvaged (outputs
and, for an exited container, logs), never silently re-run.

All the scenarios that land here:

- The container **finished while the worker was down** — success, failure, or a
  liveness kill the worker never observed (found `EXITED`).
- The container **never launched** — the worker crashed between claiming the job
  and a successful `docker run` (found `ABSENT`).
- The container was **removed externally** — `docker rm` / `docker system prune`,
  a daemon reset, or a host rebuild (found `ABSENT`).
- The container **finished and was removed by the worker**, which then crashed
  before reporting the result (found `ABSENT`; the real outcome is lost).

TODO (TODO.md #24): differentiate these scenarios and recover the real outcome
(read the exit code for `EXITED`; persist the terminal state before container
removal) instead of the coarse FAILED.

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
| `coordinator.reregistrationTimeoutSeconds` | after a restart, how long the monitor holds off before evicting, so workers can reconnect |
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
| `metricsPort` | Prometheus `/metrics` port (default 9092, matches `metrics/prometheus.yml`) |
| `resources.memory` / `cpu` / `gpu` / `capabilities` | what the worker advertises for placement |
| `docker.network` | docker network the job containers join |
| `docker.imagePullTimeoutMinutes` | bound on `docker run -d` incl. the image pull (default 10); on expiry the job FAILS (`PROCESS_START_FAILED`) |
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
  telemetry past a threshold. Feasible once the worker forwards last-activity on a
  steady cadence for silent jobs (TODO.md #23); today only re-stamped real
  telemetry reaches the coordinator.

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
