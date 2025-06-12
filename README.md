# Distributed Job Scheduler

A distributed job management system. A job is a single JAR containing sequential task stages. A coordinator accepts jobs, workers pull and execute them, and report status back via gRPC.

## Domain Model

### Job and Task

A **Job** is a unit of work defined by a JAR path and an ordered list of tasks:
- `Job(name, jarPath, mainClass, tasks, priority)` — immutable definition
- `Task(name)` — a named stage within a job

All tasks in a job share the same JAR and main class. The JAR implements a framework (scheduler-sdk) that defines the tasks as sequential stages. Tasks are not independent programs — they are stages that run within a single JVM process, where earlier stages may produce data consumed by later stages.

### Execution state

- `JobExecution(id, job, status, taskExecutions, timestamps, errorMessage)` — runtime state of a submitted job
- `TaskExecution(id, taskIndex, status, timestamps, errorMessage, exitCode)` — runtime state of a task within a job

### Job lifecycle

```
QUEUED → STARTING → RUNNING → COMPLETED
                  ↘         ↘
                 FAILED    FAILED
                  ↘         ↘
                CANCELLED  CANCELLED
```

- **QUEUED** — submitted, waiting in the queue
- **STARTING** — claimed by a worker, not yet executing
- **RUNNING** — worker is actively executing tasks
- **COMPLETED** — all tasks finished successfully
- **FAILED** — a task failed, remaining tasks skipped
- **CANCELLED** — cancelled by client

## Module Structure

```
scheduler/
├── scheduler-core/          (domain records, JobManager interface, exceptions)
├── scheduler-proto/         (protobuf/gRPC definitions + generated code)
├── job-sdk/                 (Task interface, JobRunner — framework for job JARs)
├── scheduler-coordinator/   (gRPC server, job state management, wiring)
│   ├── client/              (UserClientHandler — handles user/client RPCs)
│   └── worker/              (WorkerHandler — handles worker RPCs)
└── scheduler-worker/        (gRPC client, job execution, process management)
```

## Architecture

```
┌──────────┐         gRPC          ┌─────────────────┐         gRPC          ┌────────┐
│  Client  │ ───────────────────►  │   Coordinator   │  ◄─────────────────── │ Worker │
└──────────┘   ClientService       └─────────────────┘    WorkerService      └────────┘
               (submit, query)      (job queue + state)   (pull, report)
```

- **Client → Coordinator**: Clients submit jobs and query status via the `ClientService` gRPC service.
- **Worker → Coordinator**: Workers pull available jobs and report task progress via the `WorkerService` gRPC service.

### Worker architecture

There are two JVM processes involved in running a job:

| Term | Where it runs | What it does |
|------|---------------|--------------|
| **JobExecutor** | Worker JVM (`scheduler-worker`) | Spawns the job process and waits for it to exit. Manages the OS process lifecycle only — does not communicate with the job process directly. |
| **JobRunner** | Job process (`job-sdk`) | Runs inside the spawned child JVM. Executes tasks sequentially and POSTs status updates back to WorkerAgent via HTTP. |
| **"job process"** | Child JVM | The child JVM that JobExecutor spawns (`java -jar`). JobRunner runs inside it. |

```
Worker JVM
├── WorkerAgent                receives status from JobRunner (via HTTP),
│                              forwards to coordinator (via gRPC)
├── TaskStatusReporter         converts SDK updates to proto and streams via gRPC
└── JobExecutor                spawns the job process (child JVM), reads its stdout
```

```
Worker JVM                                    Job process (child JVM)
─────────────────────────────────             ─────────────────────────────────
                                              main() {
JobExecutor                                     JobRunner.run(List.of(
  └─ spawns: java                                   new ExtractTask(),
       -Dscheduler.callback.url=...                 new TransformTask(),
       -Dscheduler.job.id=...                       new LoadTask()
       -jar job.jar  ──────────────────────►    ));
                                              }

                                              JobRunner runs each task:
                                                1. POST /task-status {RUNNING}
WorkerAgent ◄──────── HTTP POST ───────────    2. task.execute()
  │                                             3. POST /task-status {COMPLETED}
  │  receives TaskStatusUpdate                     ... next task ...
  │
  ▼
TaskStatusReporter
  │
  │  converts to ReportTaskStatusRequest
  │  and streams via gRPC
  ▼
Coordinator (WorkerHandler)
  │
  │  calls JobManager.updateTaskStatus()
  │  transitions job/task state
  ▼
JobManagerImpl
  └─ task RUNNING   → job STARTING→RUNNING
  └─ task COMPLETED → (if all done) job→COMPLETED
  └─ task FAILED    → skip remaining, job→FAILED
```

JobExecutor passes two system properties when spawning the job process:
- `scheduler.callback.url` — WorkerAgent's task status HTTP server URL
- `scheduler.job.id` — the job execution ID

The job process never talks to the coordinator directly. All status reporting goes through WorkerAgent.

### Message exchange

```
Client                          Coordinator                         Worker
  │                                 │                                  │
  │  SubmitJobRequest               │                                  │
  │  (name, jarPath, tasks,         │                                  │
  │   mainClass, priority)          │                                  │
  │ ──────────────────────────────► │                                  │
  │                                 │  stores JobExecution(QUEUED)     │
  │  SubmitJobResponse              │                                  │
  │  (Job with id, status)          │                                  │
  │ ◄────────────────────────────── │                                  │
  │                                 │                                  │
  │                                 │          RegisterWorkerRequest   │
  │                                 │          (hostname, capacity)    │
  │                                 │ ◄──────────────────────────────  │
  │                                 │          RegisterWorkerResponse  │
  │                                 │          (worker_id)             │
  │                                 │ ──────────────────────────────►  │
  │                                 │                                  │
  │                                 │              PullJobRequest      │
  │                                 │              (worker_id)         │
  │                                 │ ◄──────────────────────────────  │
  │                                 │  claims job → STARTING           │
  │                                 │              PullJobResponse     │
  │                                 │              (Job or empty)      │
  │                                 │ ──────────────────────────────►  │
  │                                 │                                  │
  │                                 │                  Worker spawns   │
  │                                 │                  java -jar       │
  │                                 │                  process         │
  │                                 │                                  │
  │                                 │         ReportTaskStatus         │
  │                                 │         (job_id, task_index,     │
  │                                 │          RUNNING)                │
  │                                 │ ◄──────────────────────────────  │
  │                                 │         ReportTaskStatus         │
  │                                 │         (job_id, task_index,     │
  │                                 │          COMPLETED)              │
  │                                 │ ◄──────────────────────────────  │
  │                                 │         ... next task ...        │
  │                                 │ ◄──────────────────────────────  │
  │                                 │  all tasks done → job COMPLETED  │
  │                                 │                                  │
  │  GetJobStatusRequest            │                                  │
  │  (job_id)                       │                                  │
  │ ──────────────────────────────► │                                  │
  │  GetJobStatusResponse           │                                  │
  │  (Job with status, timestamps)  │                                  │
  │ ◄────────────────────────────── │                                  │
  │                                 │                                  │
  │                                 │              Heartbeat           │
  │                                 │              (worker_id)         │
  │                                 │ ◄──────────────────────────────  │
  │                                 │              HeartbeatResponse   │
  │                                 │              (should_drain)      │
  │                                 │ ──────────────────────────────►  │
```

**ClientService** (implemented):
- `SubmitJob` — unary RPC, client sends job definition with jar path and task list
- `GetJobStatus` — unary RPC, client queries by job id

**WorkerService** (implemented: RegisterWorker, PullJob, ReportTaskStatus; not yet: Heartbeat):
- `RegisterWorker` — unary, worker announces itself
- `PullJob` — unary, worker requests next available job
- `ReportTaskStatus` — client-streaming, worker streams task progress updates
- `Heartbeat` — unary, periodic liveness check

### How a request flows

1. Client sends a `SubmitJobRequest` over gRPC
2. `UserClientHandler` receives the proto message
3. `ProtoMapper.toDomain()` converts the proto request into domain objects (`Job`, `Task`)
4. `JobManager.submit()` creates a `JobExecution` with `TaskExecution` entries and queues it
5. `ProtoMapper.toProto()` converts the `JobExecution` back to a proto `Job` message for the response

### Protobuf messages

Proto definitions live in `scheduler-proto/src/main/proto/scheduler/v1/`.

| File | Purpose |
|------|---------|
| `common.proto` | Shared types: `Job`, `Task`, `JobStatus`, `TaskStatus` |
| `client_service.proto` | `ClientService` RPCs and messages (`SubmitJobRequest`, `GetJobStatusRequest`, `TaskDefinition`) |
| `worker_service.proto` | `WorkerService` RPCs and messages (`RegisterWorkerRequest`, `PullJobRequest`, `ReportTaskStatusRequest`, `HeartbeatRequest`) |

`TaskDefinition` is the input message for defining tasks when submitting a job (just a name). `Task` in `common.proto` is the full representation returned in responses (includes id, sequence number, status).

### Proto ↔ domain mapping

The domain model (`scheduler-core`) has no dependency on protobuf. `ProtoMapper` in `scheduler-coordinator` translates between the two:

| Direction | From | To |
|-----------|------|----|
| Inbound | `SubmitJobRequest` (proto) | `Job` (domain) |
| Outbound | `JobExecution` (domain) | `Job` (proto) |
| Outbound | `JobStatus` (domain enum) | `JobStatus` (proto enum) |
| Outbound | `TaskStatus` (domain enum) | `TaskStatus` (proto enum) |
| Inbound | `TaskStatus` (proto enum) | `TaskStatus` (domain enum) |

### Regenerating proto code

When you modify `.proto` files, regenerate the Java classes:

```bash
mvn compile -pl scheduler-proto
```

This runs the `protobuf-maven-plugin` which invokes `protoc` to generate Java message classes and gRPC service stubs into `scheduler-proto/target/generated-sources/protobuf/`. Your IDE should pick these up automatically — if not, mark `target/generated-sources/protobuf/java` and `target/generated-sources/protobuf/grpc-java` as Generated Sources Roots.

## Build

```bash
mvn compile    # compile all modules
mvn test       # run tests
```
