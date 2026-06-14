package com.scheduler.core;

import com.scheduler.proto.v1.FailureReason;
import com.scheduler.proto.v1.JobState;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Mutable runtime snapshot of a submitted job: its lifecycle {@link JobState},
 * per-task {@link TaskStatus}, timestamps, and failure info. The immutable
 * definition is {@link Job}. State transitions go through the lifecycle methods,
 * which enforce {@link JobStates} and throw on an illegal move.
 */
public record JobStatus(
        String id,
        Job job,
        JobState state,
        Map<Integer, TaskStatus> taskStatuses,
        Instant createdAt,
        Instant startedAt,
        Instant completedAt,
        FailureReason failureReason,
        String failureDetail
) {

    public JobStatus {
        Objects.requireNonNull(id, "Job ID must not be null");
        Objects.requireNonNull(job, "Job must not be null");
        Objects.requireNonNull(state, "Job state must not be null");
        taskStatuses = taskStatuses == null ? new LinkedHashMap<>() : new LinkedHashMap<>(taskStatuses);
    }

    public JobStatus claim() {
        return withState(JobState.JOB_STATE_STARTING);
    }

    public JobStatus start() {
        JobStatus updated = withState(JobState.JOB_STATE_RUNNING);
        return new JobStatus(id, job, updated.state, taskStatuses,
                createdAt, Instant.now(), completedAt, failureReason, failureDetail);
    }

    public JobStatus complete() {
        withState(JobState.JOB_STATE_COMPLETED);
        return new JobStatus(id, job, JobState.JOB_STATE_COMPLETED, taskStatuses,
                createdAt, startedAt, Instant.now(), failureReason, failureDetail);
    }

    public JobStatus fail(FailureReason reason, String detail) {
        withState(JobState.JOB_STATE_FAILED);
        return new JobStatus(id, job, JobState.JOB_STATE_FAILED, taskStatuses,
                createdAt, startedAt, Instant.now(), reason, detail);
    }

    /** Deadline hit, kill initiated — records the reason now; kill() follows once confirmed. */
    public JobStatus timeout(FailureReason reason, String detail) {
        withState(JobState.JOB_STATE_TIMEOUT);
        return new JobStatus(id, job, JobState.JOB_STATE_TIMEOUT, taskStatuses,
                createdAt, startedAt, completedAt, reason, detail);
    }

    public JobStatus kill(FailureReason reason, String detail) {
        withState(JobState.JOB_STATE_KILLED);
        return new JobStatus(id, job, JobState.JOB_STATE_KILLED, taskStatuses,
                createdAt, startedAt, Instant.now(), reason, detail);
    }

    public JobStatus cancel() {
        withState(JobState.JOB_STATE_CANCELLED);
        return new JobStatus(id, job, JobState.JOB_STATE_CANCELLED, taskStatuses,
                createdAt, startedAt, Instant.now(), failureReason, failureDetail);
    }

    private JobStatus withState(JobState newState) {
        if (!JobStates.canTransitionTo(state, newState)) {
            throw new IllegalStateException(
                    "Cannot transition job %s from %s to %s".formatted(id, state, newState));
        }
        return new JobStatus(id, job, newState, taskStatuses,
                createdAt, startedAt, completedAt, failureReason, failureDetail);
    }
}
