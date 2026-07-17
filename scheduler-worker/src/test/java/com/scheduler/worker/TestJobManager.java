package com.scheduler.worker;

import com.scheduler.coordinator.JobManager;
import com.scheduler.coordinator.persistence.SqliteJobStore;
import com.scheduler.coordinator.persistence.SqliteWorkerStore;
import com.scheduler.coordinator.worker.WorkerHandler;

import java.io.IOException;
import java.nio.file.Files;

/**
 * Test helper for the worker integration tests, which stand up a real
 * coordinator. Builds a {@link JobManager} (and matching {@link WorkerHandler})
 * backed by throwaway SQLite files so the persistence path is exercised
 * end-to-end without sharing state between tests.
 */
final class TestJobManager {

    private TestJobManager() {}

    static JobManager create() {
        try {
            return create(Files.createTempFile("coord", ".db"));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temp SQLite job store for test", e);
        }
    }

    /** Same, on a caller-owned db file — failover tests reuse it across "restarts". */
    static JobManager create(java.nio.file.Path dbPath) {
        return new JobManager(new SqliteJobStore(dbPath));
    }

    static WorkerHandler workerHandler(JobManager jobManager) {
        try {
            return new WorkerHandler(jobManager, new SqliteWorkerStore(Files.createTempFile("workers", ".db")));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temp SQLite worker store for test", e);
        }
    }
}
