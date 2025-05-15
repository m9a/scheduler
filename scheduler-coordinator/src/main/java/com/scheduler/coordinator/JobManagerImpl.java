package com.scheduler.coordinator;

import com.scheduler.core.*;
import com.scheduler.core.api.JobManager;
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
 * In-memory implementation of {@link JobManager}. Manages job lifecycle state.
 *
 * <pre>
 * UserClientHandler ──► submit(), getJob()
 * WorkerHandler ──► claimNextJob(), updateTaskStatus()
 * </pre>
 */
class JobManagerImpl implements JobManager {

    private static final Logger log = LoggerFactory.getLogger(JobManagerImpl.class);

    private final ConcurrentHashMap<String, JobExecution> jobs = new ConcurrentHashMap<>();
    private final LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();

    @Override
    public JobExecution submit(Job job) {
        List<TaskExecution> taskExecutions = new ArrayList<>();
        for (int i = 0; i < job.tasks().size(); i++) {
            taskExecutions.add(new TaskExecution(UUID.randomUUID().toString(), i));
        }

        JobExecution execution = new JobExecution(
                UUID.randomUUID().toString(),
                job,
                JobStatus.QUEUED,
                taskExecutions,
                Instant.now(),
                null, null, null
        );

        jobs.put(execution.id(), execution);
        queue.add(execution.id());
        return execution;
    }

    @Override
    public JobExecution getJob(String jobId) {
        JobExecution job = jobs.get(jobId);
        if (job == null) {
            throw new JobNotFoundException(jobId);
        }
        return job;
    }

    @Override
    public Optional<JobExecution> claimNextJob(String workerId) {
        String jobId = queue.poll();
        if (jobId == null) {
            return Optional.empty();
        }

        JobExecution job = jobs.get(jobId);
        if (job == null) {
            throw new IllegalStateException("Job %s in queue but not in map".formatted(jobId));
        }
        if (job.status() != JobStatus.QUEUED) {
            throw new IllegalStateException(
                    "Job %s in queue but has status %s".formatted(jobId, job.status()));
        }

        JobExecution claimed = job.withStatus(JobStatus.STARTING);
        jobs.put(jobId, claimed);
        return Optional.of(claimed);
    }

    @Override
    public synchronized void updateTaskStatus(String jobId, int taskIndex, TaskStatus status, String errorMessage) {
        JobExecution execution = getJob(jobId);
        List<TaskExecution> taskExecutions = execution.taskExecutions();

        if (taskIndex < 0 || taskIndex >= taskExecutions.size()) {
            throw new IllegalArgumentException(
                    "Task index %d out of range for job %s (has %d tasks)".formatted(taskIndex, jobId, taskExecutions.size()));
        }

        TaskExecution task = taskExecutions.get(taskIndex);
        task.setStatus(status);

        if (status == TaskStatus.RUNNING) {
            task.setStartedAt(Instant.now());
            if (execution.status() == JobStatus.STARTING) {
                JobExecution running = execution.withStatus(JobStatus.RUNNING).withStartedAt(Instant.now());
                jobs.put(jobId, running);
                log.info("Job {} is now RUNNING", jobId);
            }
        } else if (status == TaskStatus.COMPLETED) {
            task.setCompletedAt(Instant.now());
            boolean allCompleted = taskExecutions.stream()
                    .allMatch(t -> t.status() == TaskStatus.COMPLETED);
            if (allCompleted) {
                JobExecution completed = execution.withStatus(JobStatus.COMPLETED).withCompletedAt(Instant.now());
                jobs.put(jobId, completed);
                log.info("Job {} COMPLETED", jobId);
            }
        } else if (status == TaskStatus.FAILED) {
            task.setCompletedAt(Instant.now());
            task.setErrorMessage(errorMessage);
            for (TaskExecution remaining : taskExecutions) {
                if (remaining.status() == TaskStatus.PENDING) {
                    remaining.setStatus(TaskStatus.SKIPPED);
                }
            }
            JobExecution failed = execution.withStatus(JobStatus.FAILED).withCompletedAt(Instant.now())
                    .withErrorMessage(errorMessage);
            jobs.put(jobId, failed);
            log.info("Job {} FAILED at task {}: {}", jobId, taskIndex, errorMessage);
        }
    }
}
