package com.scheduler.coordinator;

import com.scheduler.core.*;
import com.scheduler.core.exception.JobNotFoundException;
import com.scheduler.coordinator.persistence.JobStore;
import com.scheduler.proto.job.StatusUpdate;
import com.scheduler.proto.v1.FailureReason;
import com.scheduler.proto.v1.JobState;
import com.scheduler.proto.v1.ReportEntry;
import com.scheduler.proto.v1.TaskState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <b>The coordinator's state core</b> — passive store for job lifecycle, shared
 * by both edges. The worker owns the full job lifecycle (start, monitor, kill,
 * report); this class applies whatever the worker sends and answers client reads.
 * It writes state on its own in exactly one case — {@link #failJobsForWorker},
 * when a worker's heartbeat is lost (see CLAUDE.md "State ownership").
 * No transport: both gRPC handlers unpack protos before calling in.
 *
 * <pre>
 * ClientHandler  (Client → Coordinator edge) ──► submit(), getJob()
 * WorkerHandler  (Worker → Coordinator edge) ──► claimNextJob(), handleStatusUpdate(),
 *                                                handleReport(), failJobsForWorker()
 * CoordinatorMetrics (scrape time)           ──► queueDepth(), jobCountsByState()
 * </pre>
 */
public class JobManager {

    private static final Logger log = LoggerFactory.getLogger(JobManager.class);

    // Durable mirror — written through on every state transition so a restart
    // recovers (see README "Coordinator Failover & State Persistence"). The
    // manager depends only on the interface; the SQLite impl is injected.
    private final JobStore store;

    // All submitted jobs by ID. Written by submit(), claimNextJob() (state → STARTING),
    // handleStatusUpdate() (state changes), and failJobsForWorker() (state → FAILED).
    private final ConcurrentHashMap<String, JobStatus> jobs = new ConcurrentHashMap<>();

    // Job IDs waiting to be claimed, in submission order. submit() appends,
    // claimNextJob() iterates to find the first job the worker can satisfy.
    // Guarded by synchronized(this) — all mutating methods are synchronized.
    private final LinkedList<String> queue = new LinkedList<>();

    // Maps jobId → workerId for all in-flight (non-terminal) jobs.
    // claimNextJob() inserts, handleStatusUpdate() and failJobsForWorker() remove
    // when the job reaches a terminal state. Used by the heartbeat monitor to find
    // which jobs to fail when a worker dies.
    private final ConcurrentHashMap<String, String> jobWorker = new ConcurrentHashMap<>();

    // jobId -> last-activity epoch millis, reported by the worker (which owns job
    // liveness — it sees the SDK's frames and pings). Read by the client API;
    // removed when the job goes terminal.
    private final ConcurrentHashMap<String, Long> lastActivityMillis = new ConcurrentHashMap<>();

    public JobManager(JobStore store) {
        this.store = store;
    }

    /**
     * Rebuilds in-memory state from the durable store on boot. Reloads non-terminal
     * jobs, re-queues {@code QUEUED} ones (claim is pull-based with no ack, so a
     * queued job was never handed to a worker), and rebuilds the jobId→worker map
     * for in-flight jobs so the heartbeat monitor and resync can act on them.
     * Terminal jobs stay only in the store; live telemetry/liveness resume on the
     * next report. Call once at startup, before the gRPC server accepts requests.
     */
    public synchronized void recover() {
        List<JobStore.PersistedJob> reloaded = store.loadNonTerminal();
        int queued = 0;
        int inFlight = 0;
        for (JobStore.PersistedJob persisted : reloaded) {
            JobStatus job = persisted.status();
            jobs.put(job.id(), job);
            if (job.state() == JobState.JOB_STATE_QUEUED) {
                queue.add(job.id());
                queued++;
            } else if (persisted.assignedWorkerId() != null) {
                jobWorker.put(job.id(), persisted.assignedWorkerId());
                inFlight++;
            } else {
                log.warn("Recovered non-terminal job with no assigned worker: jobId={}, state={}",
                        job.id(), job.state());
            }
        }
        log.info("Recovered {} non-terminal job(s) from store: {} queued, {} in-flight",
                reloaded.size(), queued, inFlight);
    }


    public synchronized JobStatus submit(String jobId, Job job) {
        JobStatus execution = new JobStatus(
                jobId,
                job,
                JobState.JOB_STATE_QUEUED,
                new LinkedHashMap<>(),
                Instant.now(),
                null, null, null, null
        );

        jobs.put(execution.id(), execution);
        queue.add(execution.id());
        store.save(execution, null);  // QUEUED, no worker yet
        CoordinatorMetrics.JOBS_SUBMITTED.inc();
        return execution;
    }

    // ── metrics snapshot accessors (read at Prometheus scrape time) ─────────

    public synchronized int queueDepth() {
        return queue.size();
    }

    public Map<JobState, Integer> jobCountsByState() {
        Map<JobState, Integer> counts = new LinkedHashMap<>();
        for (JobStatus job : jobs.values()) {
            counts.merge(job.state(), 1, Integer::sum);
        }
        return counts;
    }


    public JobStatus getJob(String jobId) {
        JobStatus job = jobs.get(jobId);
        if (job == null) {
            throw new JobNotFoundException(jobId);
        }
        return job;
    }

    /**
     * Snapshot of all jobs for the read-only HTTP API (UI). Newest first by
     * submission time. A copied list so the HTTP layer never holds the live map.
     */
    public List<JobStatus> listJobs() {
        List<JobStatus> all = new java.util.ArrayList<>(jobs.values());
        all.sort(java.util.Comparator.comparing(JobStatus::createdAt).reversed());
        return all;
    }


    // TODO: future enhancements for scheduling:
    //  - Priority queue: use job.priority (stored but currently ignored) to order within the queue
    //  - Bin packing: prefer workers with smallest sufficient resources
    //  - CPU/GPU affinity: pin containers to specific cores/devices via --cpuset-cpus / --gpus
    //  - Fair-share scheduling: per-user/team quotas to prevent starvation
    //  - Preemption: high-priority jobs evict lower-priority running jobs
    //  - Resource accounting: track in-use vs available per worker for concurrent execution
    //  - Auto-scaling: spin up/down workers based on queue depth and demand
    //  - Coordinator concurrency review: evaluate finer-grained locking for high worker/job counts
    //  - Microbenchmarks: JMH benchmarks for claimNextJob, handleStatusUpdate, submit
    //  - Worker/job health checks with retry and backoff
    //  - Docker image caching: pre-pull images on workers to avoid cold-start latency

    public synchronized Optional<JobStatus> claimNextJob(WorkerInfo worker) {
        Iterator<String> it = queue.iterator();
        while (it.hasNext()) {
            String jobId = it.next();
            JobStatus job = jobs.get(jobId);
            if (job == null || job.state() != JobState.JOB_STATE_QUEUED) {
                it.remove();
                continue;
            }
            ResourceRequirements req = job.job().resources();
            if (req.satisfiedBy(worker.memoryMb(), worker.cpuCores(), worker.gpu(), worker.capabilities())) {
                it.remove();
                JobStatus claimed = job.claim();
                jobs.put(jobId, claimed);
                jobWorker.put(jobId, worker.id());
                store.save(claimed, worker.id());  // STARTING, now assigned
                CoordinatorMetrics.QUEUE_WAIT.observe(
                        Duration.between(claimed.createdAt(), Instant.now()).toMillis() / 1000.0);
                return Optional.of(claimed);
            }
        }
        return Optional.empty();
    }


    /**
     * Applies a status update from the worker — one {@link StatusUpdate}
     * proto, job section always present, task section only when a task changed
     * (see README "Job Lifecycle"). The task section is applied <b>before</b> the
     * job section so a terminal update sees the final task states.
     *
     * <p>The coordinator never infers state here — it applies exactly what the
     * worker sends and de-dupes a no-op (same-state) update. A repeated
     * job-RUNNING (the worker stamps it on every task update) is therefore a
     * no-op once the job is RUNNING. Updates for already-terminal jobs are
     * dropped — they're late messages arriving after the heartbeat monitor
     * already failed the job.
     */
    public synchronized void handleStatusUpdate(StatusUpdate update) {
        String jobId = update.getJobId();
        JobStatus job = getJob(jobId);

        if (JobStates.isTerminal(job.state())) {
            log.warn("Ignoring status update for terminal job: jobId={}, jobState={}, taskIndex={}, taskState={}",
                    jobId, update.getJobState(), update.getTaskIndex(), update.getTaskState());
            return;
        }

        // Captured before applyJobStatus, which clears the assignment on a terminal
        // transition — we still want to persist which worker ran the job.
        String assignedWorker = jobWorker.get(jobId);
        boolean changed = false;

        if (update.getTaskState() != TaskState.TASK_STATE_UNSPECIFIED) {
            applyTaskStatus(job, update.getTaskIndex(),
                    update.getTaskName().isEmpty() ? null : update.getTaskName(),
                    update.getTaskState(),
                    update.getErrorMessage().isEmpty() ? null : update.getErrorMessage());
            changed = true;
        }

        JobState jobState = update.getJobState();
        if (jobState != JobState.JOB_STATE_UNSPECIFIED && job.state() != jobState) {
            FailureReason reason = update.getFailureReason() != FailureReason.FAILURE_REASON_UNSPECIFIED
                    ? update.getFailureReason() : null;
            String detail = update.getFailureDetail().isEmpty() ? null : update.getFailureDetail();
            applyJobStatus(jobId, job, jobState, reason, detail);
            changed = true;
        }

        // One write-through per applied update — a no-op (duplicate RUNNING) skips it.
        if (changed) {
            store.save(jobs.get(jobId), assignedWorker);
        }
    }

    private void applyJobStatus(String jobId, JobStatus job, JobState jobState,
                                FailureReason failureReason, String failureDetail) {
        JobStatus updated = switch (jobState) {
            case JOB_STATE_RUNNING -> job.start();
            case JOB_STATE_TIMEOUT -> job.timeout(failureReason, failureDetail);
            case JOB_STATE_COMPLETED -> job.complete();
            case JOB_STATE_FAILED -> job.fail(failureReason, failureDetail);
            case JOB_STATE_KILLED -> job.kill(failureReason, failureDetail);
            case JOB_STATE_CANCELLED -> job.cancel();
            default -> throw new IllegalArgumentException("Unexpected job state update: " + jobState);
        };
        if (JobStates.isTerminal(jobState)) {
            jobWorker.remove(jobId);
            lastActivityMillis.remove(jobId);
            CoordinatorMetrics.JOBS_FINISHED.labels(CoordinatorMetrics.jobStateLabel(jobState)).inc();
        }
        jobs.put(jobId, updated);
        String reasonMessage = failureReason != null ? ": " + FailureMessages.format(failureReason, failureDetail) : "";
        log.info("Job {} is now {}{}", jobId, jobState, reasonMessage);
    }

    private void applyTaskStatus(JobStatus job, int taskIndex, String taskName,
                                 TaskState taskState, String errorMessage) {
        TaskStatus task = job.taskStatuses().computeIfAbsent(taskIndex,
                idx -> new TaskStatus(UUID.randomUUID().toString(), idx, "task-" + idx));
        switch (taskState) {
            case TASK_STATE_RUNNING -> task.start(taskName);
            case TASK_STATE_COMPLETED -> task.complete(taskName);
            case TASK_STATE_FAILED -> task.fail(taskName, errorMessage);
            default -> throw new IllegalArgumentException("Unexpected task state update: " + taskState);
        }
    }

    /**
     * Merges a telemetry batch forwarded by the worker into the task's latest-wins
     * snapshot. Creates the TaskStatus if telemetry arrives before the first status
     * update (status and telemetry travel on separate RPCs, so order isn't guaranteed).
     */
    public synchronized void handleReport(String jobId, int taskIndex, long timestampMs, List<ReportEntry> entries) {
        CoordinatorMetrics.TELEMETRY_REPORTS.inc();
        JobStatus job = getJob(jobId);
        if (JobStates.isTerminal(job.state())) {
            // Telemetry flushed at task end can trail the terminal status; the final
            // values still matter, but a job failed by the heartbeat monitor stays failed.
            log.debug("Applying late report to terminal job: jobId={}, taskIndex={}", jobId, taskIndex);
        } else {
            // Every report doubles as a liveness signal — the worker owns liveness and
            // stamps each Report with the job's last-activity time (see CLAUDE.md "State
            // ownership"). Max-wins since unary reports can arrive out of order.
            lastActivityMillis.merge(jobId, timestampMs, Math::max);
        }
        // A liveness-only report carries no entries — don't materialize a phantom task.
        if (entries.isEmpty()) {
            return;
        }
        TaskStatus task = job.taskStatuses().computeIfAbsent(taskIndex,
                idx -> new TaskStatus(UUID.randomUUID().toString(), idx, "task-" + idx));
        task.applyReports(entries);
    }

    /** Last-activity epoch millis for a job, or 0 if none reported. Read by the client API. */
    public long lastActivity(String jobId) {
        return lastActivityMillis.getOrDefault(jobId, 0L);
    }


    /**
     * Fails all non-terminal jobs assigned to the given worker.
     * Called by the heartbeat monitor when a worker stops sending heartbeats —
     * the only case where the coordinator (not the worker) writes job state,
     * because the worker is dead and can't (see CLAUDE.md "State ownership").
     * Tasks are left at their last reported state.
     * Synchronized on the same lock as handleStatusUpdate so a late status
     * update and a heartbeat-triggered failure can't race.
     */
    public synchronized int failJobsForWorker(String workerId, FailureReason reason) {
        int count = 0;
        for (Map.Entry<String, String> entry : jobWorker.entrySet()) {
            if (!entry.getValue().equals(workerId)) {
                continue;
            }
            String jobId = entry.getKey();
            JobStatus job = jobs.get(jobId);
            if (job == null || JobStates.isTerminal(job.state())) {
                jobWorker.remove(jobId);
                lastActivityMillis.remove(jobId);
                continue;
            }
            // Only the job state is written here; any in-progress task keeps its
            // last reported state (the worker is dead and can't report a terminal).
            JobStatus failed = job.fail(reason, null);
            jobs.put(jobId, failed);
            store.save(failed, workerId);  // persist the dead-worker fail
            jobWorker.remove(jobId);
            lastActivityMillis.remove(jobId);
            CoordinatorMetrics.JOBS_FINISHED.labels(
                    CoordinatorMetrics.jobStateLabel(JobState.JOB_STATE_FAILED)).inc();
            log.info("Failed job due to dead worker: jobId={}, workerId={}, reason={}",
                    jobId, workerId, FailureMessages.text(reason));
            count++;
        }
        return count;
    }
}
