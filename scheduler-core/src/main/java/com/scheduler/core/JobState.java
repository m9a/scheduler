package com.scheduler.core;

import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public record JobState(
        String id,
        Job job,
        JobStatus status,
        Map<Integer, TaskState> taskStates,
        Instant createdAt,
        Instant startedAt,
        Instant completedAt,
        FailureReason failureReason,
        String failureDetail
) {

    public JobState {
        Objects.requireNonNull(id, "Job ID must not be null");
        Objects.requireNonNull(job, "Job must not be null");
        Objects.requireNonNull(status, "Job status must not be null");
        taskStates = taskStates == null ? new LinkedHashMap<>() : new LinkedHashMap<>(taskStates);
    }

    public JobState claim() {
        return withStatus(JobStatus.STARTING);
    }

    public JobState start() {
        JobState updated = withStatus(JobStatus.RUNNING);
        return new JobState(id, job, updated.status, taskStates,
                createdAt, Instant.now(), completedAt, failureReason, failureDetail);
    }

    public JobState complete() {
        withStatus(JobStatus.COMPLETED);
        return new JobState(id, job, JobStatus.COMPLETED, taskStates,
                createdAt, startedAt, Instant.now(), failureReason, failureDetail);
    }

    public JobState fail(FailureReason reason, String detail) {
        withStatus(JobStatus.FAILED);
        return new JobState(id, job, JobStatus.FAILED, taskStates,
                createdAt, startedAt, Instant.now(), reason, detail);
    }

    public JobState kill(FailureReason reason, String detail) {
        withStatus(JobStatus.KILLED);
        return new JobState(id, job, JobStatus.KILLED, taskStates,
                createdAt, startedAt, Instant.now(), reason, detail);
    }

    public JobState cancel() {
        withStatus(JobStatus.CANCELLED);
        return new JobState(id, job, JobStatus.CANCELLED, taskStates,
                createdAt, startedAt, Instant.now(), failureReason, failureDetail);
    }

    private JobState withStatus(JobStatus newStatus) {
        if (!status.canTransitionTo(newStatus)) {
            throw new IllegalStateException(
                    "Cannot transition job %s from %s to %s".formatted(id, status, newStatus));
        }
        return new JobState(id, job, newStatus, taskStates,
                createdAt, startedAt, completedAt, failureReason, failureDetail);
    }
}
