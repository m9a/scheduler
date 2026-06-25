package com.scheduler.coordinator.persistence;

import com.scheduler.core.JobStates;
import com.scheduler.core.JobStatus;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * In-memory {@link JobStore} for tests — same contract as {@link SqliteJobStore}
 * without a file, so JobManager tests exercise write-through and can assert what
 * was persisted.
 */
public class InMemoryJobStore implements JobStore {

    private final Map<String, PersistedJob> saved = new LinkedHashMap<>();

    @Override
    public synchronized void save(JobStatus job, String assignedWorkerId) {
        saved.put(job.id(), new PersistedJob(job, assignedWorkerId));
    }

    @Override
    public synchronized Optional<PersistedJob> find(String jobId) {
        return Optional.ofNullable(saved.get(jobId));
    }

    @Override
    public synchronized List<PersistedJob> listAll() {
        List<PersistedJob> all = new ArrayList<>(saved.values());
        all.sort(Comparator.comparing((PersistedJob p) -> p.status().createdAt()).reversed());
        return all;
    }

    @Override
    public synchronized List<PersistedJob> loadNonTerminal() {
        List<PersistedJob> out = new ArrayList<>();
        for (PersistedJob p : saved.values()) {
            if (!JobStates.isTerminal(p.status().state())) {
                out.add(p);
            }
        }
        return out;
    }

    @Override
    public synchronized int deleteTerminalCompletedBefore(long cutoffEpochMillis) {
        int before = saved.size();
        saved.values().removeIf(p ->
                JobStates.isTerminal(p.status().state())
                        && p.status().completedAt() != null
                        && p.status().completedAt().toEpochMilli() < cutoffEpochMillis);
        return before - saved.size();
    }

    @Override
    public void close() {
        // nothing to release
    }
}
