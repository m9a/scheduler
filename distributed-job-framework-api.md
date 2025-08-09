# Distributed Job Framework — Annotation-Based API Design

A compile-time, annotation-processor-driven framework that lets developers define **jobs** composed of **tasks**, which are then executed in isolated JVM processes across a pool of distributed workers orchestrated by a centralized coordinator.

---

## Table of Contents

1. [Core Annotations (Developer-Facing API)](#1-core-annotations-developer-facing-api)
2. [Usage Example](#2-usage-example)
3. [Generated Artifacts](#3-generated-artifacts)
4. [Runtime Contracts (Framework Library)](#4-runtime-contracts-framework-library)
5. [Architecture Overview](#5-architecture-overview)
6. [Compile-Time Validations](#6-compile-time-validations)
7. [Design Decisions & Rationale](#7-design-decisions--rationale)

---

## 1. Core Annotations (Developer-Facing API)

### 1.1 `@Job` — Job Definition

Marks a class as a distributed job. The annotation processor generates a `JobDescriptor`, a serializable manifest, and a harness main-class used to boot the isolated JVM.

```java
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.SOURCE)
public @interface Job {
    /** Unique job identifier. Defaults to fully-qualified class name. */
    String id() default "";

    /** Human-readable description for the coordinator dashboard. */
    String description() default "";

    /** Maximum wall-clock seconds before the job is killed. */
    int timeoutSeconds() default 3600;

    /** How many times a failed job may be retried on any worker. */
    int maxRetries() default 0;

    /** Resource requirements hint for the coordinator's scheduler. */
    ResourceProfile resource() default @ResourceProfile;
}
```

### 1.2 `@ResourceProfile` — Resource Hints

Used exclusively inside `@Job` to declare resource requirements that the coordinator's scheduler uses for placement decisions.

```java
@Target({})
@Retention(RetentionPolicy.SOURCE)
public @interface ResourceProfile {
    int minMemoryMb() default 512;
    int cpuCores()    default 1;
    String[] labels() default {};   // e.g. "gpu", "high-mem"
}
```

### 1.3 `@Task` — Task Definition

Marks a method inside a `@Job` class as a task — the smallest schedulable unit of work. Tasks run sequentially or in a DAG defined by `dependsOn`.

```java
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.SOURCE)
public @interface Task {
    /** Task name. Defaults to method name. */
    String name() default "";

    /** Ordering weight when no explicit dependencies exist. */
    int order() default 0;

    /**
     * Names of tasks that must complete before this one starts.
     * Forms a DAG — the processor validates there are no cycles.
     */
    String[] dependsOn() default {};

    /** If true, failure here aborts the entire job. */
    boolean critical() default true;
}
```

### 1.4 `@Param` — Parameter Injection

Injects a named parameter from the job submission payload. Applied to `@Job` constructor parameters or `@Task` method parameters. The processor validates that every `@Param` has a matching entry in the generated manifest schema.

```java
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.SOURCE)
public @interface Param {
    String value();                     // parameter name / key
    String defaultValue() default "";   // empty = required
}
```

### 1.5 `@Context` — Framework Context Injection

Injects a handle to the framework's context (progress reporting, inter-task data passing, cancellation checks).

```java
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.SOURCE)
public @interface Context {}
```

### 1.6 `@BeforeJob` / `@AfterJob` — Lifecycle Hooks

```java
/** Runs once before the first task. Setup resources, connections, etc. */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.SOURCE)
public @interface BeforeJob {}

/** Runs once after the last task (or on failure). Cleanup. */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.SOURCE)
public @interface AfterJob {}
```

---

## 2. Usage Example

```java
@Job(
    id = "etl-daily-sales",
    description = "Extracts sales data, transforms, and loads into the warehouse",
    timeoutSeconds = 7200,
    maxRetries = 2,
    resource = @ResourceProfile(minMemoryMb = 2048, cpuCores = 2)
)
public class DailySalesEtlJob {

    private final String region;
    private final DbConnection db;

    // Constructor params are injected from the submission payload
    public DailySalesEtlJob(@Param("region") String region,
                            @Param("dbUrl")  String dbUrl) {
        this.region = region;
        this.db = DbConnection.open(dbUrl);
    }

    @BeforeJob
    void ensureSchemaExists() {
        db.execute("CREATE TABLE IF NOT EXISTS staging_sales (...)");
    }

    @Task(name = "extract", order = 1)
    void extractFromSource(@Context JobContext ctx) {
        ctx.report(Status.RUNNING, "Pulling data for " + region);
        // ... extraction logic ...
        ctx.put("rowCount", extractedRows.size());   // pass data downstream
    }

    @Task(name = "transform", dependsOn = "extract")
    void applyTransformations(@Context JobContext ctx,
                              @Param("fiscalYear") String fiscalYear) {
        int rows = ctx.get("rowCount", Integer.class);
        ctx.report(Status.RUNNING, "Transforming " + rows + " rows");
        // ... transformation logic ...
    }

    @Task(name = "load", dependsOn = "transform", critical = true)
    void loadIntoWarehouse(@Context JobContext ctx) {
        // ... load logic ...
        ctx.report(Status.RUNNING, "Load complete");
    }

    @AfterJob
    void cleanup() {
        db.close();
    }
}
```

---

## 3. Generated Artifacts

For each `@Job`-annotated class, the annotation processor emits three artifacts at compile time.

### 3.1 Job Descriptor — Compile-Time Metadata Registry

```java
/**
 * AUTO-GENERATED — do not edit.
 * Source: com.example.jobs.DailySalesEtlJob
 */
public final class DailySalesEtlJob_Descriptor implements JobDescriptor {

    @Override public String id()          { return "etl-daily-sales"; }
    @Override public String description() { return "Extracts sales data ..."; }
    @Override public int timeoutSeconds() { return 7200; }
    @Override public int maxRetries()     { return 2; }

    @Override
    public ResourceRequirements resources() {
        return new ResourceRequirements(2048, 2, Set.of());
    }

    /** Declared parameter schema — used by coordinator for validation. */
    @Override
    public List<ParamDescriptor> parameters() {
        return List.of(
            new ParamDescriptor("region",     String.class, true,  ""),
            new ParamDescriptor("dbUrl",      String.class, true,  ""),
            new ParamDescriptor("fiscalYear", String.class, true,  "")
        );
    }

    /** Task DAG — used by the harness to drive execution order. */
    @Override
    public List<TaskDescriptor> tasks() {
        return List.of(
            new TaskDescriptor("extract",   0, List.of(),            true),
            new TaskDescriptor("transform", 1, List.of("extract"),   true),
            new TaskDescriptor("load",      2, List.of("transform"), true)
        );
    }

    @Override
    public Class<?> jobClass() { return DailySalesEtlJob.class; }
}
```

### 3.2 Job Harness — Entry Point for the Isolated JVM

The worker agent spawns each job as:

```
java -cp <classpath> DailySalesEtlJob_Harness <base64-encoded-payload>
```

```java
/**
 * AUTO-GENERATED — entry point for the child JVM process.
 */
public final class DailySalesEtlJob_Harness {

    public static void main(String[] args) throws Exception {
        // 1. Deserialize the execution payload (params + coordinator address)
        ExecutionPayload payload = ExecutionPayload.decode(args[0]);

        // 2. Open a reporting channel back to the worker agent
        try (JobReporter reporter = JobReporter.connect(payload.reportbackUri())) {

            // 3. Build the JobContext
            JobContext ctx = new DefaultJobContext(reporter);

            try {
                // 4. Instantiate the job (inject @Param values)
                DailySalesEtlJob job = new DailySalesEtlJob(
                    payload.param("region", String.class),
                    payload.param("dbUrl",  String.class)
                );

                // 5. @BeforeJob
                reporter.taskStarted("__before_job");
                job.ensureSchemaExists();
                reporter.taskCompleted("__before_job");

                // 6. Execute tasks in topological order
                reporter.taskStarted("extract");
                job.extractFromSource(ctx);
                reporter.taskCompleted("extract");

                reporter.taskStarted("transform");
                job.applyTransformations(ctx,
                    payload.param("fiscalYear", String.class));
                reporter.taskCompleted("transform");

                reporter.taskStarted("load");
                job.loadIntoWarehouse(ctx);
                reporter.taskCompleted("load");

                // 7. @AfterJob (normal path)
                reporter.taskStarted("__after_job");
                job.cleanup();
                reporter.taskCompleted("__after_job");

                reporter.jobSucceeded();

            } catch (Throwable t) {
                // @AfterJob (error path) — best-effort
                try {
                    new DailySalesEtlJob(
                        payload.param("region", String.class),
                        payload.param("dbUrl", String.class))
                    .cleanup();
                } catch (Exception ignored) {}

                reporter.jobFailed(t);
                System.exit(1);
            }
        }
    }
}
```

### 3.3 Service Loader Registration

The processor generates a `META-INF/services/com.framework.JobDescriptor` file listing all descriptors so the worker agent can discover available jobs at startup via `ServiceLoader`.

---

## 4. Runtime Contracts (Framework Library)

### 4.1 `JobContext` — Developer's Window into the Framework

```java
public interface JobContext {
    /** Report progress / status to the coordinator. */
    void report(Status status, String message);

    /** Pass data from one task to a downstream task. */
    void put(String key, Object value);
    <T> T get(String key, Class<T> type);

    /** Check if the coordinator has requested cancellation. */
    boolean isCancelled();

    /** Structured metrics (counters, gauges) surfaced in the dashboard. */
    Metrics metrics();
}
```

### 4.2 Worker ↔ Coordinator Communication Protocol

```java
public interface CoordinatorClient {
    /** Worker heartbeats + capacity advertisement. */
    void heartbeat(WorkerStatus status);

    /** Coordinator pushes job assignments to the worker. */
    Stream<JobAssignment> receiveAssignments();

    /** Worker reports per-task and per-job lifecycle events. */
    void reportEvent(JobEvent event);
}

public sealed interface JobEvent {
    record TaskStarted   (String jobId, String taskName, Instant at)                  implements JobEvent {}
    record TaskCompleted (String jobId, String taskName, Instant at)                  implements JobEvent {}
    record TaskFailed    (String jobId, String taskName, Instant at, String error)    implements JobEvent {}
    record JobSucceeded  (String jobId, Instant at)                                   implements JobEvent {}
    record JobFailed     (String jobId, Instant at, String error, boolean retriable)  implements JobEvent {}
}
```

### 4.3 Worker Agent — The Long-Lived JVM on Each Machine

```java
public final class WorkerAgent {

    private final CoordinatorClient coordinator;
    private final Map<String, JobDescriptor> registry;  // from ServiceLoader
    private final ExecutorService processPool;

    /**
     * For each assignment, spawn an isolated child JVM:
     *   java -cp <job-classpath>
     *        -Xmx<resource.minMemoryMb>m
     *        <JobClass>_Harness
     *        <base64-encoded-payload>
     */
    public void executeJob(JobAssignment assignment) {
        JobDescriptor desc = registry.get(assignment.jobId());

        ProcessBuilder pb = new ProcessBuilder(
            "java",
            "-cp", buildClasspath(desc),
            "-Xmx" + desc.resources().memoryMb() + "m",
            desc.jobClass().getName() + "_Harness",
            assignment.payload().encode()
        );

        pb.inheritIO();
        Process process = pb.start();
        // monitor process, enforce timeout, relay events to coordinator
    }
}
```

---

## 5. Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│                       COORDINATOR                            │
│  ┌──────────┐  ┌──────────────┐  ┌────────────────────────┐ │
│  │ REST /   │  │  Scheduler   │  │   Job State Machine    │ │
│  │ gRPC API │──│  (assigns    │──│ PENDING → RUNNING →    │ │
│  │          │  │   jobs to    │  │ SUCCEEDED / FAILED /   │ │
│  └──────────┘  │   workers)   │  │ RETRYING               │ │
│                └──────────────┘  └────────────────────────┘ │
│                       ▲  │                                   │
│           heartbeats  │  │  assignments                      │
│            + events   │  ▼                                   │
└───────────────────────┼──┼───────────────────────────────────┘
                        │  │
          ┌─────────────┘  └──────────────┐
          ▼                               ▼
┌──────────────────┐            ┌──────────────────┐
│   WORKER NODE A  │            │   WORKER NODE B  │
│ ┌──────────────┐ │            │ ┌──────────────┐ │
│ │ WorkerAgent  │ │            │ │ WorkerAgent  │ │
│ │ (long-lived  │ │            │ │ (long-lived  │ │
│ │  JVM)        │ │            │ │  JVM)        │ │
│ └──┬───┬───┬───┘ │            │ └──┬───┬───────┘ │
│    │   │   │     │            │    │   │         │
│    ▼   ▼   ▼     │            │    ▼   ▼         │
│ ┌───┐┌───┐┌───┐  │            │ ┌───┐┌───┐       │
│ │JVM││JVM││JVM│  │            │ │JVM││JVM│       │
│ │ A ││ B ││ C │  │            │ │ D ││ E │       │
│ └───┘└───┘└───┘  │            │ └───┘└───┘       │
│  (isolated job   │            │  (isolated job   │
│   processes)     │            │   processes)     │
└──────────────────┘            └──────────────────┘
```

### Execution Flow

1. **Submission** — A client submits a job via the coordinator's REST/gRPC API with a parameter payload.
2. **Validation** — The coordinator validates the payload against the `JobDescriptor`'s parameter schema.
3. **Scheduling** — The scheduler selects a worker whose available resources match the job's `ResourceProfile`.
4. **Assignment** — The coordinator pushes a `JobAssignment` to the chosen worker.
5. **Process Spawn** — The worker agent spawns a new JVM process running the generated `_Harness` class.
6. **Execution** — The harness runs `@BeforeJob` → tasks in topological order → `@AfterJob`.
7. **Reporting** — The harness reports task-level and job-level events back to the worker agent, which relays them to the coordinator.
8. **Completion** — The coordinator transitions the job state to `SUCCEEDED` or `FAILED` (potentially triggering a retry).

---

## 6. Compile-Time Validations

The annotation processor rejects the build with clear error messages for the following conditions:

| Validation | Error Message |
|---|---|
| `@Task` method not inside a `@Job` class | `@Task methods must be declared in a @Job-annotated class` |
| Cycle in `dependsOn` graph | `Circular dependency detected: extract → transform → extract` |
| `dependsOn` references a non-existent task | `Task "load" depends on unknown task "trnasform"` |
| `@Param` names collide | `Duplicate parameter name "region" in job "etl-daily-sales"` |
| Multiple `@BeforeJob` or `@AfterJob` | `Only one @BeforeJob method is allowed per job` |
| `@Task` method returns non-void | `@Task methods must return void (use JobContext.put() for outputs)` |
| Non-serializable `@Param` type | `Parameter "config" of type DbConfig is not serializable` |

---

## 7. Design Decisions & Rationale

### Why `RetentionPolicy.SOURCE`?

The annotations are consumed entirely by the processor at compile time. The generated harness and descriptor carry all runtime information, so no reflective annotation scanning is needed — this keeps the isolated child JVM startup fast and avoids reflection.

### Why a Generated Harness Instead of a Generic Reflective Runner?

A code-generated `main` class means the child JVM does zero reflection, has a minimal classpath, and can be AOT-compiled (GraalVM `native-image`) for sub-second cold starts. It also gives compile-time proof that the wiring is correct.

### Why `JobContext.put/get` Instead of Task Return Values?

Tasks form a DAG, not a linear pipeline — a task may feed data to multiple downstream tasks. A keyed context map is more flexible than chaining return values and avoids coupling the task method signatures to each other.

### Why Process-Level Isolation?

A rogue job can `System.exit()`, leak memory, or deadlock — none of which should impact the worker agent or other concurrent jobs. Separate JVMs provide OS-level fault boundaries at the cost of startup latency (mitigated by AOT compilation or CRaC snapshots).
