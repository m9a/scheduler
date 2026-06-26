package com.scheduler.coordinator.persistence;

import com.scheduler.core.WorkerInfo;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * In-memory {@link WorkerStore} for tests — same contract as
 * {@link SqliteWorkerStore} without a file, so registry tests can assert what was
 * persisted on register and removed on eviction.
 */
public class InMemoryWorkerStore implements WorkerStore {

    private final Map<String, WorkerInfo> saved = new LinkedHashMap<>();

    @Override
    public synchronized void save(WorkerInfo worker) {
        saved.put(worker.id(), worker);
    }

    @Override
    public synchronized void delete(String workerId) {
        saved.remove(workerId);
    }

    @Override
    public synchronized List<WorkerInfo> loadAll() {
        return new ArrayList<>(saved.values());
    }

    @Override
    public void close() {
        // nothing to release
    }
}
