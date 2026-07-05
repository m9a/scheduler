package com.scheduler.coordinator.worker;

import com.scheduler.core.WorkerInfo;
import com.scheduler.coordinator.persistence.WorkerStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * The coordinator's record of which workers exist and when each was last heard
 * from. Pure worker state, no transport. {@link WorkerHandler} writes to it as
 * RPCs arrive (register, heartbeat). The monitor from {@link #startMonitor}
 * scans it and evicts workers whose heartbeat went silent, calling the supplied
 * callback (wired to {@code JobManager.failJobsForWorker}) for each dead worker.
 * {@code CoordinatorMetrics} reads {@link #count} at scrape time.
 */
class WorkerRegistry {

    private static final Logger log = LoggerFactory.getLogger(WorkerRegistry.class);

    // Durable mirror — written through on register/eviction so a restart can
    // re-seed the in-memory registry before any worker re-registers (see WorkerStore).
    private final WorkerStore store;

    private final ConcurrentHashMap<String, WorkerInfo> workers = new ConcurrentHashMap<>();
    private ScheduledExecutorService monitor;

    WorkerRegistry(WorkerStore store) {
        this.store = store;
    }

    void register(WorkerInfo worker) {
        workers.put(worker.id(), worker);
        store.save(worker);
    }

    /**
     * Loads persisted workers into the in-memory registry on boot. Each gets
     * stamped with {@code lastHeartbeat} (the boot time), so a reloaded worker
     * gets a fresh grace window instead of being evicted for a stale timestamp.
     * Not written through — these rows are already in the store.
     */
    void seed(Instant lastHeartbeat) {
        for (WorkerInfo worker : store.loadAll()) {
            workers.put(worker.id(), worker.withLastHeartbeat(lastHeartbeat));
        }
        log.info("Seeded {} worker(s) from store on boot", workers.size());
    }

    Optional<WorkerInfo> find(String workerId) {
        return Optional.ofNullable(workers.get(workerId));
    }

    void updateHeartbeat(String workerId) {
        workers.computeIfPresent(workerId, (id, worker) -> worker.withLastHeartbeat(Instant.now()));
    }

    int count() {
        return workers.size();
    }

    /** Snapshot of all registered workers for the read-only HTTP API (UI). */
    java.util.List<WorkerInfo> list() {
        return new java.util.ArrayList<>(workers.values());
    }

    /**
     * Starts a background thread that scans all workers on an interval. It evicts
     * any worker whose last heartbeat is older than the timeout, calling
     * {@code onDeadWorker} first. The first scan waits {@code firstScanDelay} — on
     * boot that is the re-registration window, so seeded workers have time to
     * reconnect before any eviction.
     */
    void startMonitor(Duration heartbeatSilenceTimeout, Duration heartbeatScanInterval, Duration firstScanDelay,
                      Consumer<WorkerInfo> onDeadWorker) {
        monitor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "heartbeat-monitor");
            t.setDaemon(true);
            return t;
        });
        monitor.scheduleAtFixedRate(() -> {
            try {
                Instant cutoff = Instant.now().minus(heartbeatSilenceTimeout);
                for (WorkerInfo worker : workers.values()) {
                    if (worker.lastHeartbeat().isBefore(cutoff)) {
                        log.warn("Worker heartbeat lost: workerId={}, hostname={}, lastHeartbeat={}",
                                worker.id(), worker.hostname(), worker.lastHeartbeat());
                        onDeadWorker.accept(worker);
                        workers.remove(worker.id());
                        store.delete(worker.id());
                    }
                }
            } catch (Exception e) {
                log.error("Heartbeat monitor scan failed: {}", e.getMessage(), e);
            }
        }, firstScanDelay.toMillis(), heartbeatScanInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    void shutdownMonitor() {
        if (monitor != null) {
            monitor.shutdown();
        }
    }
}
