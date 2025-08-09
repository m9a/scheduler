package com.scheduler.coordinator;

import com.scheduler.core.*;
import com.scheduler.core.exception.JobNotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Manages job lifecycle state in memory.
 * Tasks are created lazily as status updates arrive from the worker.
 *
 * <pre>
 * UserRequestHandler ──► submit(), getJob()
 * WorkerHandler ──► claimNextJob(), updateTaskStatus(), finalizeJob()
 * </pre>
 */
public class JobManagerImpl {

    private static final Logger log = LoggerFactory.getLogger(JobManagerImpl.class);

    private final ConcurrentHashMap<String, JobState> jobs = new ConcurrentHashMap<>();
    private final LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();


    public JobState submit(Job job) {
        JobState execution = new JobState(
                UUID.randomUUID().toString(),
                job,
                JobStatus.QUEUED,
                new ArrayList<>(),
                Instant.now(),
                null, null, null
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


    public Optional<JobState> claimNextJob(String workerId) {
        String jobId = queue.poll();
        if (jobId == null) {
            return Optional.empty();
        }

        JobState job = jobs.get(jobId);
        if (job == null) {
            throw new IllegalStateException("Job %s in queue but not in map".formatted(jobId));
        }
        if (job.status() != JobStatus.QUEUED) {
            throw new IllegalStateException(
                    "Job %s in queue but has status %s".formatted(jobId, job.status()));
        }

        JobState claimed = job.withStatus(JobStatus.STARTING);
        jobs.put(jobId, claimed);
        return Optional.of(claimed);
    }


    public synchronized void updateTaskStatus(String jobId, int taskIndex, String taskName,
                                              TaskStatus status, String errorMessage) {
        JobState job = getJob(jobId);
        List<TaskState> taskStates = job.taskStates();

        // Lazily create TaskState entries as they arrive
        while (taskIndex >= taskStates.size()) {
            int nextIndex = taskStates.size();
            taskStates.add(new TaskState(UUID.randomUUID().toString(), nextIndex, taskName));
        }

        TaskState task = taskStates.get(taskIndex);
        task.setStatus(status);

        if (status == TaskStatus.RUNNING) {
            task.setStartedAt(Instant.now());
            if (job.status() == JobStatus.STARTING) {
                JobState running = job.withStatus(JobStatus.RUNNING).withStartedAt(Instant.now());
                jobs.put(jobId, running);
                log.info("Job {} is now RUNNING", jobId);
            }
        } else if (status == TaskStatus.COMPLETED) {
            task.setCompletedAt(Instant.now());
        } else if (status == TaskStatus.FAILED) {
            task.setCompletedAt(Instant.now());
            task.setErrorMessage(errorMessage);
            JobState failed = job.withStatus(JobStatus.FAILED).withCompletedAt(Instant.now())
                    .withErrorMessage(errorMessage);
            jobs.put(jobId, failed);
            log.info("Job {} FAILED at task {}: {}", jobId, taskIndex, errorMessage);
        }
    }


    public synchronized void finalizeJob(String jobId) {
        JobState execution = getJob(jobId);

        if (execution.status().isTerminal()) {
            return;
        }

        boolean allCompleted = !execution.taskStates().isEmpty()
                && execution.taskStates().stream().allMatch(t -> t.status() == TaskStatus.COMPLETED);
        if (allCompleted) {
            JobState completed = execution.withStatus(JobStatus.COMPLETED).withCompletedAt(Instant.now());
            jobs.put(jobId, completed);
            log.info("Job {} COMPLETED", jobId);
        } else {
            JobState failed = execution.withStatus(JobStatus.FAILED).withCompletedAt(Instant.now())
                    .withErrorMessage("Process terminated before all tasks completed");
            jobs.put(jobId, failed);
            log.info("Job {} FAILED: process terminated before all tasks completed", jobId);
        }
    }
}
