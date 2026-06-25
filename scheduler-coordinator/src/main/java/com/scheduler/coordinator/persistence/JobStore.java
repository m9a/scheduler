package com.scheduler.coordinator.persistence;

import com.scheduler.core.JobStatus;

import java.util.List;
import java.util.Optional;

/**
 * Durable mirror of the coordinator's job state. {@link com.scheduler.coordinator.JobManager}
 * writes through to it on every state transition so a restart recovers instead of
 * losing everything; on boot the manager reloads non-terminal jobs from it.
 *
 * <p>The assigned worker lives in {@code JobManager.jobWorker} (not on
 * {@link JobStatus}), so it travels alongside the job here as {@link PersistedJob}.
 */
public interface JobStore extends AutoCloseable {

    /** A persisted job plus its assigned worker id (null when unassigned/queued). */
    record PersistedJob(JobStatus status, String assignedWorkerId) {}

    /** Insert or replace the full job snapshot (definition + lifecycle + tasks + assignment). */
    void save(JobStatus job, String assignedWorkerId);

    Optional<PersistedJob> find(String jobId);

    /** All jobs, newest first — the source for the UI list / history reads. */
    List<PersistedJob> listAll();

    /** Non-terminal jobs only — reloaded into memory on coordinator boot. */
    List<PersistedJob> loadNonTerminal();

    /** Retention sweep: delete terminal jobs whose completed_at is before the cutoff. Returns rows removed. */
    int deleteTerminalCompletedBefore(long cutoffEpochMillis);

    @Override
    void close();
}
