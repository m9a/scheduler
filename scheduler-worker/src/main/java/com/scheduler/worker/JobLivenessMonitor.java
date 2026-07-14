package com.scheduler.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Worker-owned per-job liveness tracking and stall detection. Every inbound SDK
 * frame (status/report/liveness ping) calls {@link #recordActivity()} via
 * {@link JobCallbackHandler}'s activity listener. On each tick the monitor checks
 * for a stall — no activity within {@code startupTimeout} of launch, or
 * {@code maxMissedPings} consecutive missed {@code pingInterval} windows after the
 * first ping — and, when {@code autoKill}, runs the {@code onUnresponsive}
 * callback (a graceful container stop) once.
 *
 * <p>This is entirely worker-local: it sends nothing to the coordinator. The
 * worker is the authority on job liveness because it owns the job and receives
 * its pings (see CLAUDE.md "State ownership"). After a kill, {@link WorkerAgent}
 * reports KILLED / UNRESPONSIVE. This monitor owns the one liveness clock:
 * {@link #lastLivenessAt()} is the epoch millis of the last frame received (the
 * last time the job proved it was alive). The worker stamps that onto forwarded
 * telemetry (see {@link WorkerAgent#relayTelemetry}), so the coordinator's
 * per-job last-activity comes from this monitor, not a separate clock.
 */
class JobLivenessMonitor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(JobLivenessMonitor.class);

    /** Detection thresholds, from the worker's {@code liveness} config. */
    record Config(long startupTimeoutMs, long pingIntervalMs, int maxMissedPings, boolean autoKill) {}

    private final String jobId;
    private final Config config;
    private final Runnable onUnresponsive;
    private final ScheduledExecutorService scheduler;
    private final long startedAtMillis;

    // Epoch millis of the last frame received — the last liveness pass. Updated on
    // every inbound frame; read by stall detection and stamped onto telemetry.
    private volatile long lastLivenessAt;
    private volatile boolean sawActivity;
    private volatile boolean unresponsive;

    JobLivenessMonitor(String jobId, Config config, Runnable onUnresponsive) {
        this.jobId = jobId;
        this.config = config;
        this.onUnresponsive = onUnresponsive;
        this.startedAtMillis = System.currentTimeMillis();
        this.lastLivenessAt = startedAtMillis;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "liveness-monitor-" + jobId);
            t.setDaemon(true);
            return t;
        });
    }

    void start() {
        scheduler.scheduleAtFixedRate(this::tick,
                config.pingIntervalMs(), config.pingIntervalMs(), TimeUnit.MILLISECONDS);
    }

    void recordActivity() {
        lastLivenessAt = System.currentTimeMillis();
        sawActivity = true;
    }

    /** Epoch millis of the last liveness pass (last frame received). */
    long lastLivenessAt() {
        return lastLivenessAt;
    }

    boolean isUnresponsive() {
        return unresponsive;
    }

    private void tick() {
        if (unresponsive || !config.autoKill()) {
            return;
        }
        long now = System.currentTimeMillis();
        boolean stalled = sawActivity
                ? now - lastLivenessAt > (long) config.maxMissedPings() * config.pingIntervalMs()
                : now - startedAtMillis > config.startupTimeoutMs();
        if (stalled) {
            unresponsive = true;
            log.warn("Job {} is unresponsive ({}) — terminating the container", jobId,
                    sawActivity ? "no liveness for " + config.maxMissedPings() + " pings"
                                : "no activity within startup timeout");
            try {
                onUnresponsive.run();
            } catch (Exception e) {
                log.warn("Failed to terminate unresponsive job {}: {}", jobId, e.getMessage());
            }
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
