package com.scheduler.core;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public record JobExecution(
        String id,
        Job job,
        JobStatus status,
        List<TaskExecution> taskExecutions,
        Instant createdAt,
        Instant startedAt,
        Instant completedAt,
        String errorMessage
) {

    public JobExecution {
        Objects.requireNonNull(id, "Job ID must not be null");
        Objects.requireNonNull(job, "Job must not be null");
        Objects.requireNonNull(status, "Job status must not be null");
        taskExecutions = taskExecutions == null ? List.of() : List.copyOf(taskExecutions);
    }

    public JobExecution withStatus(JobStatus newStatus) {
        if (!status.canTransitionTo(newStatus)) {
            throw new IllegalStateException(
                    "Cannot transition job %s from %s to %s".formatted(id, status, newStatus));
        }
        return new JobExecution(id, job, newStatus, taskExecutions,
                createdAt, startedAt, completedAt, errorMessage);
    }

    public JobExecution withStartedAt(Instant startedAt) {
        return new JobExecution(id, job, status, taskExecutions,
                createdAt, startedAt, completedAt, errorMessage);
    }

    public JobExecution withCompletedAt(Instant completedAt) {
        return new JobExecution(id, job, status, taskExecutions,
                createdAt, startedAt, completedAt, errorMessage);
    }

    public JobExecution withErrorMessage(String errorMessage) {
        return new JobExecution(id, job, status, taskExecutions,
                createdAt, startedAt, completedAt, errorMessage);
    }
}
