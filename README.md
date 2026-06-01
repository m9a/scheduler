# Distributed Job Scheduler

A distributed job scheduler where each job is a Docker container running sequential task stages. A coordinator accepts jobs from clients, workers pull and execute them in containers, and report status back via gRPC.

## Domain Model

A **Job** is an immutable definition: a Docker image (`artifactUri`), key-value params, a priority, and optional input files.

```
Job(name, artifactUri, params, priority, inputFiles)
```

Runtime state is tracked separately:

- `JobState(id, job, status, taskStates, timestamps, reason)` — mutable runtime state of a submitted job
- `TaskState(id, taskIndex, taskName, status, timestamps, errorMessage, exitCode)` — mutable runtime state of a task within a job
- `InputFile(name, uri)` — an input file resolved to an object store URI

The job does not declare its tasks upfront. Tasks are created lazily by the coordinator when the SDK first reports each task's status at runtime.

## Job Lifecycle

### JobStatus

```
QUEUED → STARTING → RUNNING → COMPLETED
                  ↘         ↘
                 FAILED    FAILED
                  ↘         ↘
                KILLED    KILLED
                  ↘         ↘
               CANCELLED  CANCELLED
```

| Status | Description |
|--------|-------------|
| QUEUED | Submitted, waiting in the priority queue |
| STARTING | Claimed by a worker, container not yet running |
| RUNNING | At least one task has started executing |
| COMPLETED | All tasks finished successfully |
| FAILED | A task failed or the container exited non-zero |
| KILLED | Job process timed out, destroyed forcibly |
| CANCELLED | Cancelled by client |

### TaskStatus

```
PENDING → RUNNING → COMPLETED
                  ↘
                 FAILED

PENDING → SKIPPED  (remaining tasks after a failure)
```

| Status | Description |
|--------|-------------|
| PENDING | Not yet started |
| RUNNING | Currently executing |
| COMPLETED | Finished successfully |
| FAILED | Threw an exception or errored out |
| SKIPPED | Skipped because a prior task failed |

## Module Structure

| Module | Purpose |
|--------|---------|
| `scheduler-core` | Domain records (`Job`, `JobState`, `TaskState`, `InputFile`, `ObjectStore`), enums, exceptions. Zero infrastructure dependencies. |
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
