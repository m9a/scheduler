package com.scheduler.worker;

import com.scheduler.coordinator.JobManager;
import com.scheduler.coordinator.persistence.SqliteJobStore;

import java.io.IOException;
import java.nio.file.Files;

/**
 * Test helper for the worker integration tests, which stand up a real
 * coordinator. Builds a {@link JobManager} backed by a throwaway SQLite file so
 * the persistence path is exercised end-to-end without sharing state between
 * tests.
 */
final class TestJobManager {

    private TestJobManager() {}

    static JobManager create() {
        try {
            return new JobManager(new SqliteJobStore(Files.createTempFile("coord", ".db")));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temp SQLite job store for test", e);
        }
    }
}
