package com.scheduler.coordinator.persistence;

import com.scheduler.core.WorkerInfo;

import java.util.List;

/**
 * Durable mirror of the coordinator's worker registry. The coordinator keeps the
 * live registry in memory (see {@code WorkerRegistry}) but writes through to this
 * store so a restart can re-seed the registry before any worker re-registers —
 * without it the heartbeat monitor would have no workers to watch and could never
 * fail the in-flight jobs of a worker that died during the outage (see README
 * "Coordinator Failover & State Persistence").
 *
 * <p>Persists id + minimal placement info (hostname, resources, capabilities).
 * {@code lastHeartbeat} round-trips but is overwritten with the boot time when the
 * registry is seeded, so a reloaded worker gets a fresh grace window rather than
 * being evicted immediately for a stale timestamp.
 */
public interface WorkerStore extends AutoCloseable {

    /** Insert or replace the worker row — called on register. */
    void save(WorkerInfo worker);

    /** Remove the worker row — called when the heartbeat monitor evicts a dead worker. */
    void delete(String workerId);

    /** All persisted workers — reloaded into the registry on coordinator boot. */
    List<WorkerInfo> loadAll();

    @Override
    void close();
}
