package com.scheduler.worker.persistence;

import com.scheduler.proto.job.StatusUpdate;

import java.time.Duration;
import java.util.List;

/**
 * Durable mirror of the status the worker forwards to the coordinator.
 * {@code WorkerAgent} writes through here on every {@link StatusUpdate} it sends.
 * On (re)connect the worker replays {@link #loadAllJobs()} in its register call,
 * so a coordinator that missed updates during an outage re-learns current and
 * terminal state.
 *
 * <p>The worker has no domain status type — it only relays protos. So the store
 * persists {@link StatusUpdate} fields directly and rebuilds them on load (see
 * CLAUDE.md "one status message"). The table holds only unacked rows:
 * {@link #ack} drops a job once the coordinator confirms its terminal, and
 * {@link #prune} bounds what a permanently-dead coordinator would leave behind.
 */
public interface WorkerStatusStore extends AutoCloseable {

    /** Write-through the latest status for a job/task (latest-wins per row). */
    void update(StatusUpdate update);

    /**
     * All persisted updates, rebuilt as {@link StatusUpdate}s — the
     * {@code known_jobs} payload sent on register. Per job, task entries come
     * before the job entry, so task state lands before a terminal update trips
     * the coordinator's already-terminal guard.
     */
    List<StatusUpdate> loadAllJobs();

    /** Coordinator confirmed the terminal delivery for this job — drop all its rows. */
    void ack(String jobId);

    /** Retention sweep: drop job entries terminal longer than {@code retentionPeriod}. Returns rows removed. */
    int prune(Duration retentionPeriod);

    @Override
    void close();
}
