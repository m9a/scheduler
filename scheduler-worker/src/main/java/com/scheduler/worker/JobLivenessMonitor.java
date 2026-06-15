package com.scheduler.worker;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Worker-owned per-job liveness tracking. Every inbound SDK frame
 * (status/report/liveness ping) calls {@link #recordActivity()} via
 * {@link JobCallbackServer}'s activity listener; a scheduled task forwards the
 * latest activity time to the coordinator (throttled — not per ping) so it can
 * be surfaced on {@code GetJobStatus}. The worker is the authority on job
 * liveness because it owns the job and receives its pings (see CLAUDE.md
 * "State ownership").
 *
 * <p>Phase 2: tracking + reporting only. Stall escalation (kill an unresponsive
 * container) is Phase 3.
 */
class JobLivenessMonitor implements AutoCloseable {

    // How often the last-activity time is forwarded to the coordinator. Larger
    // than the SDK's ping interval (15s) so a quietly-working job stays fresh.
    private static final long REPORT_INTERVAL_MS = 20_000;

    private final String jobId;
    private final CoordinatorClient coordinator;
    private final ScheduledExecutorService scheduler;
    private volatile long lastActivityAtMillis;

    JobLivenessMonitor(String jobId, CoordinatorClient coordinator) {
        this.jobId = jobId;
        this.coordinator = coordinator;
        this.lastActivityAtMillis = System.currentTimeMillis();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "liveness-monitor-" + jobId);
            t.setDaemon(true);
            return t;
        });
    }

    void start() {
        scheduler.scheduleAtFixedRate(
                () -> coordinator.reportLiveness(jobId, lastActivityAtMillis),
                REPORT_INTERVAL_MS, REPORT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    void recordActivity() {
        lastActivityAtMillis = System.currentTimeMillis();
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
