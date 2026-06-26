package com.scheduler.coordinator.persistence;

import com.scheduler.core.WorkerInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SqliteWorkerStoreTest {

    private WorkerInfo worker(String id, String hostname) {
        return new WorkerInfo(id, hostname, 4096, 8, true, Set.of("cuda", "avx512"),
                Instant.ofEpochMilli(1000), Instant.ofEpochMilli(2000));
    }

    @Test
    void roundTrip(@TempDir Path dir) {
        SqliteWorkerStore store = new SqliteWorkerStore(dir.resolve("scheduler.db"));
        try {
            store.save(worker("w-1", "gpu-box"));

            List<WorkerInfo> all = store.loadAll();
            assertEquals(1, all.size());
            WorkerInfo w = all.get(0);
            assertEquals("w-1", w.id());
            assertEquals("gpu-box", w.hostname());
            assertEquals(4096, w.memoryMb());
            assertEquals(8, w.cpuCores());
            assertTrue(w.gpu());
            assertEquals(Set.of("cuda", "avx512"), w.capabilities());
            assertEquals(Instant.ofEpochMilli(1000), w.registeredAt());
            assertEquals(Instant.ofEpochMilli(2000), w.lastHeartbeat());
        } finally {
            store.close();
        }
    }

    @Test
    void saveReplaces(@TempDir Path dir) {
        SqliteWorkerStore store = new SqliteWorkerStore(dir.resolve("scheduler.db"));
        try {
            store.save(worker("w-1", "old-host"));
            store.save(new WorkerInfo("w-1", "new-host", 1024, 2, false, Set.of(),
                    Instant.ofEpochMilli(1000), Instant.ofEpochMilli(5000)));

            List<WorkerInfo> all = store.loadAll();
            assertEquals(1, all.size());
            assertEquals("new-host", all.get(0).hostname());
            assertFalse(all.get(0).gpu());
            assertTrue(all.get(0).capabilities().isEmpty());
        } finally {
            store.close();
        }
    }

    @Test
    void deleteRemoves(@TempDir Path dir) {
        SqliteWorkerStore store = new SqliteWorkerStore(dir.resolve("scheduler.db"));
        try {
            store.save(worker("w-1", "host-1"));
            store.save(worker("w-2", "host-2"));
            store.delete("w-1");

            List<WorkerInfo> all = store.loadAll();
            assertEquals(1, all.size());
            assertEquals("w-2", all.get(0).id());
        } finally {
            store.close();
        }
    }

    // A reopened store on the same file sees prior writes — the point of persisting:
    // a coordinator restart can re-seed the registry from disk.
    @Test
    void survivesReopen(@TempDir Path dir) {
        Path db = dir.resolve("scheduler.db");
        SqliteWorkerStore store = new SqliteWorkerStore(db);
        store.save(worker("w-1", "host-1"));
        store.close();

        SqliteWorkerStore reopened = new SqliteWorkerStore(db);
        try {
            assertEquals(1, reopened.loadAll().size());
            assertEquals("w-1", reopened.loadAll().get(0).id());
        } finally {
            reopened.close();
        }
    }
}
