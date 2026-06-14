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
| -1 | Job KILLED — process timed out (configurable, default 10 min), destroyed with `destroyForcibly()` |
| Any other | Job FAILED — "Job process exited with code N" |

If `spawnJobProcess` throws `IOException` (container failed to start) or `InterruptedException`, the job is marked FAILED with the exception message.

### Task failure

When a task throws an exception, the SDK marks it FAILED and returns immediately — remaining tasks are never started. The container exits, WorkerAgent sees the non-zero exit code and marks the job FAILED.

### Timeout

The timeout is configurable via the `WorkerAgent` constructor (`jobExecutionTimeout`, default `Duration.ofMinutes(10)`). When the timeout expires, `process.waitFor()` returns false and the process is killed with `destroyForcibly()`.

### Worker crash

WorkerAgent sends periodic heartbeats to the coordinator. If a worker stops sending heartbeats (crash, network partition), the coordinator's heartbeat monitor detects the dead worker and fails all its in-flight jobs with `HEARTBEAT_LOST`.

### Coordinator idempotency

The coordinator ignores status updates for jobs already in a terminal state (COMPLETED, FAILED, KILLED, CANCELLED). Duplicate RUNNING updates are also safe — `canTransitionTo` rejects same-state transitions.

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

## Control Plane & Infra Metrics

`control-plane.yaml` is the single config for the control plane: which stacks
are enabled (MLflow, Prometheus/Grafana) and the coordinator's settings (port,
heartbeat timeouts, MinIO). `scripts/control-plane.sh up|down|status` derives
compose profiles/variables from it and logs the fully resolved compose to
`.control-plane-resolved.yml` for debugging. The coordinator reads the same
file via `--config`. Available metrics per component:
[docs/metrics.md](docs/metrics.md).

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
