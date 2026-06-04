package com.scheduler.coordinator;

import com.scheduler.core.*;
import com.scheduler.core.exception.JobNotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Passive state store for job lifecycle. The worker owns the full job lifecycle
 * (start, monitor, kill, report) — this class applies whatever the worker sends.
 *
 * <pre>
 * UserRequestHandler ──► submit(), getJob()
 * WorkerHandler      ──► claimNextJob(), handleStatusUpdate(), failJobsForWorker()
 * </pre>
 */
public class JobManagerImpl {

    private static final Logger log = LoggerFactory.getLogger(JobManagerImpl.class);

    // All submitted jobs by ID. Written by submit(), claimNextJob() (status → STARTING),
    // handleStatusUpdate() (status changes), and failJobsForWorker() (status → FAILED).
    private final ConcurrentHashMap<String, JobState> jobs = new ConcurrentHashMap<>();

    // Job IDs waiting to be claimed, in submission order. submit() appends,
    // claimNextJob() iterates to find the first job the worker can satisfy.
    // Guarded by synchronized(this) — all mutating methods are synchronized.
    private final LinkedList<String> queue = new LinkedList<>();

    // Maps jobId → workerId for all in-flight (non-terminal) jobs.
    // claimNextJob() inserts, handleStatusUpdate() and failJobsForWorker() remove
    // when the job reaches a terminal state. Used by the heartbeat monitor to find
    // which jobs to fail when a worker dies.
    private final ConcurrentHashMap<String, String> jobWorker = new ConcurrentHashMap<>();


    public synchronized JobState submit(String jobId, Job job) {
        JobState execution = new JobState(
                jobId,
                job,
                JobStatus.QUEUED,
                new LinkedHashMap<>(),
                Instant.now(),
                null, null, null, null
        );

        jobs.put(execution.id(), execution);
        queue.add(execution.id());
        return execution;
    }


    public JobState getJob(String jobId) {
        JobState job = jobs.get(jobId);
        if (job == null) {
            throw new JobNotFoundException(jobId);
        }
        return job;
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

    public synchronized Optional<JobState> claimNextJob(WorkerInfo worker) {
        Iterator<String> it = queue.iterator();
        while (it.hasNext()) {
            String jobId = it.next();
            JobState job = jobs.get(jobId);
            if (job == null || job.status() != JobStatus.QUEUED) {
                it.remove();
                continue;
            }
            ResourceRequirements req = job.job().resources();
            if (req.satisfiedBy(worker.memoryMb(), worker.cpuCores(), worker.capabilities())) {
                it.remove();
                JobState claimed = job.claim();
                jobs.put(jobId, claimed);
                jobWorker.put(jobId, worker.id());
                return Optional.of(claimed);
            }
        }
        return Optional.empty();
    }


    /**
     * Applies a status update from the worker. A single update can carry a job-level
     * change, a task-level change, or both:
     *
     * <ul>
     *   <li><b>Job-level only</b> (jobStatus set, taskStatus null) — transitions the job
     *       via named methods (e.g. start(), complete(), fail()).</li>
     *   <li><b>Task-level only</b> (jobStatus null, taskStatus set) — creates the task
     *       entry if first seen, then applies the task status via named methods.</li>
     *   <li><b>Both</b> — applies the job transition first, then the task update.</li>
     * </ul>
     *
     * <p>Updates for jobs already in a terminal state are silently dropped — this
     * handles late-arriving messages from workers after the heartbeat monitor has
     * already failed the job.
     */
    public synchronized void handleStatusUpdate(String jobId,
                                                 JobStatus jobStatus,
                                                 FailureReason failureReason, String failureDetail,
                                                 int taskIndex, String taskName,
                                                 TaskStatus taskStatus, String errorMessage) {
        JobState job = getJob(jobId);

        if (job.status().isTerminal()) {
            log.warn("Ignoring status update for terminal job: jobId={}, jobStatus={}, taskIndex={}, taskStatus={}",
                    jobId, jobStatus, taskIndex, taskStatus);
            return;
        }

        if (jobStatus != null && job.status() != jobStatus) {
            job = applyJobStatus(jobId, job, jobStatus, failureReason, failureDetail);
        }

        if (taskStatus != null) {
            applyTaskStatus(job, taskIndex, taskName, taskStatus, errorMessage);
        }
    }

    private JobState applyJobStatus(String jobId, JobState job, JobStatus jobStatus,
                                     FailureReason failureReason, String failureDetail) {
        JobState updated = switch (jobStatus) {
            case RUNNING -> job.start();
            case COMPLETED -> job.complete();
            case FAILED -> job.fail(failureReason, failureDetail);
            case KILLED -> job.kill(failureReason, failureDetail);
            case CANCELLED -> job.cancel();
            default -> throw new IllegalArgumentException("Unexpected job status update: " + jobStatus);
        };
        if (jobStatus.isTerminal()) {
            jobWorker.remove(jobId);
        }
        jobs.put(jobId, updated);
        String reasonMessage = failureReason != null ? ": " + failureReason.toMessage(failureDetail) : "";
        log.info("Job {} is now {}{}", jobId, jobStatus, reasonMessage);
        return updated;
    }

    private void applyTaskStatus(JobState job, int taskIndex, String taskName,
                                  TaskStatus taskStatus, String errorMessage) {
        TaskState task = job.taskStates().computeIfAbsent(taskIndex,
                idx -> new TaskState(UUID.randomUUID().toString(), idx, "task-" + idx));
        switch (taskStatus) {
            case RUNNING -> task.start(taskName);
            case COMPLETED -> task.complete(taskName);
            case FAILED -> task.fail(taskName, errorMessage);
            case SKIPPED -> task.skip(taskName);
            default -> throw new IllegalArgumentException("Unexpected task status update: " + taskStatus);
        }
    }


    /**
     * Fails all non-terminal jobs assigned to the given worker.
     * Called by the heartbeat monitor when a worker stops sending heartbeats.
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
            JobState job = jobs.get(jobId);
            if (job == null || job.status().isTerminal()) {
                jobWorker.remove(jobId);
                continue;
            }
            JobState failed = job.fail(reason, null);
            jobs.put(jobId, failed);
            jobWorker.remove(jobId);
            log.info("Failed job due to dead worker: jobId={}, workerId={}, reason={}",
                    jobId, workerId, reason.message());
            count++;
        }
        return count;
    }
}
