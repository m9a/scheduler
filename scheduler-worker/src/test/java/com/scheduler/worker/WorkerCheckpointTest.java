package com.scheduler.worker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class WorkerCheckpointTest {

    @Test
    void generatesWhenMissing(@TempDir Path dir) {
        Path file = dir.resolve("worker_checkpoint.yaml");
        String id = WorkerCheckpoint.resolveOrCreate(file);

        assertNotNull(id);
        assertFalse(id.isBlank());
        assertTrue(Files.isRegularFile(file), "checkpoint file should be written");
    }

    @Test
    void reusesExistingId(@TempDir Path dir) {
        Path file = dir.resolve("worker_checkpoint.yaml");
        String first = WorkerCheckpoint.resolveOrCreate(file);
        String second = WorkerCheckpoint.resolveOrCreate(file);

        assertEquals(first, second, "same id must survive across restarts");
    }

    @Test
    void regeneratesWhenBlank(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("worker_checkpoint.yaml");
        Files.writeString(file, "workerId: \"\"\n");

        String id = WorkerCheckpoint.resolveOrCreate(file);
        assertFalse(id.isBlank());
    }

    @Test
    void createsParentDirs(@TempDir Path dir) {
        Path file = dir.resolve("nested/state/worker_checkpoint.yaml");
        String id = WorkerCheckpoint.resolveOrCreate(file);

        assertNotNull(id);
        assertTrue(Files.isRegularFile(file));
    }
}
