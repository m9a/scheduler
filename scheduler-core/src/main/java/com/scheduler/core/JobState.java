package com.scheduler.core;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public record JobState(
        String id,
        Job job,
        JobStatus status,
        List<TaskState> taskStates,
        Instant createdAt,
        Instant startedAt,
        Instant completedAt,
        String errorMessage
) {

    public JobState {
        Objects.requireNonNull(id, "Job ID must not be null");
        Objects.requireNonNull(job, "Job must not be null");
        Objects.requireNonNull(status, "Job status must not be null");
        taskStates = taskStates == null ? new ArrayList<>() : new ArrayList<>(taskStates);
    }

    public JobState withStatus(JobStatus newStatus) {
        if (!status.canTransitionTo(newStatus)) {
            throw new IllegalStateException(
                    "Cannot transition job %s from %s to %s".formatted(id, status, newStatus));
        }
        return new JobState(id, job, newStatus, taskStates,
                createdAt, startedAt, completedAt, errorMessage);
    }

    public JobState withStartedAt(Instant startedAt) {
        return new JobState(id, job, status, taskStates,
                createdAt, startedAt, completedAt, errorMessage);
    }

    public JobState withCompletedAt(Instant completedAt) {
        return new JobState(id, job, status, taskStates,
                createdAt, startedAt, completedAt, errorMessage);
    }

    public JobState withErrorMessage(String errorMessage) {
        return new JobState(id, job, status, taskStates,
                createdAt, startedAt, completedAt, errorMessage);
    }
}
