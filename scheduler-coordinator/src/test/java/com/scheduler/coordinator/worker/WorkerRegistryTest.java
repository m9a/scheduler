package com.scheduler.coordinator.worker;

import com.scheduler.core.WorkerInfo;
import com.scheduler.coordinator.persistence.InMemoryWorkerStore;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class WorkerRegistryTest {

    private WorkerInfo worker(String id, Instant lastHeartbeat) {
        return new WorkerInfo(id, "host-" + id, 2048, 4, false, Set.of(),
                Instant.now(), lastHeartbeat);
    }

    // On boot, persisted workers are loaded into the registry with a fresh heartbeat
    // so the monitor has a worker list to watch (and won't evict them immediately).
    @Test
    void seedLoadsPersistedWorkers() {
        InMemoryWorkerStore store = new InMemoryWorkerStore();
        store.save(worker("w-1", Instant.ofEpochMilli(1)));
        store.save(worker("w-2", Instant.ofEpochMilli(1)));

        WorkerRegistry registry = new WorkerRegistry(store);
        Instant boot = Instant.now();
        registry.seed(boot);

        assertEquals(2, registry.count());
        assertEquals(boot, registry.find("w-1").orElseThrow().lastHeartbeat());
    }

    @Test
    void registerPersists() {
        InMemoryWorkerStore store = new InMemoryWorkerStore();
        WorkerRegistry registry = new WorkerRegistry(store);

        registry.register(worker("w-1", Instant.now()));

        assertEquals(1, store.loadAll().size());
        assertEquals("w-1", store.loadAll().get(0).id());
    }

    // The monitor evicts a silent worker from memory AND deletes it from the store,
    // so a later restart doesn't re-seed a worker that's already known dead.
    @Test
    void evictionDeletes() throws InterruptedException {
        InMemoryWorkerStore store = new InMemoryWorkerStore();
        WorkerRegistry registry = new WorkerRegistry(store);
        registry.register(worker("dead", Instant.now().minusSeconds(60)));

        AtomicReference<String> evicted = new AtomicReference<>();
        registry.startMonitor(Duration.ofMillis(1), Duration.ofMillis(20), Duration.ofMillis(20),
                w -> evicted.set(w.id()));
        try {
            long deadline = System.currentTimeMillis() + 2000;
            while (System.currentTimeMillis() < deadline && !store.loadAll().isEmpty()) {
                Thread.sleep(20);
            }
            assertTrue(store.loadAll().isEmpty(), "evicted worker should be deleted from the store");
            assertEquals(0, registry.count());
            assertEquals("dead", evicted.get());
        } finally {
            registry.shutdownMonitor();
        }
    }
}
