package com.scheduler.worker;

import com.scheduler.proto.job.Report;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Worker-owned per-job liveness tracking and stall detection. Every inbound SDK
 * frame (status/report/liveness ping) calls {@link #recordActivity()} via
 * {@link JobCallbackHandler}'s activity listener. On each tick the monitor:
 * <ol>
 *   <li>forwards the latest activity time to the coordinator (surfaced on
 *       {@code GetJobStatus});</li>
 *   <li>checks for a stall — no activity within {@code startupTimeout} of launch,
 *       or {@code maxMissedPings} consecutive missed {@code pingInterval} windows
 *       after the first ping — and, when {@code autoKill}, runs the
 *       {@code onUnresponsive} callback (a graceful container stop) once.</li>
 * </ol>
 *
 * <p>The worker is the authority on job liveness because it owns the job and
 * receives its pings (see CLAUDE.md "State ownership"). After a kill,
 * {@link WorkerAgent} reports KILLED / UNRESPONSIVE.
 */
class JobLivenessMonitor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(JobLivenessMonitor.class);

    /** Detection thresholds, from the worker's {@code liveness} config. */
    record Config(long startupTimeoutMs, long pingIntervalMs, int maxMissedPings, boolean autoKill) {}

    private final String jobId;
    private final Consumer<Report> telemetrySink;
    private final Config config;
    private final Runnable onUnresponsive;
    private final ScheduledExecutorService scheduler;
    private final long startedAtMillis;

    private volatile long lastActivityAtMillis;
    private volatile boolean sawActivity;
    private volatile boolean unresponsive;

    JobLivenessMonitor(String jobId, Consumer<Report> telemetrySink, Config config, Runnable onUnresponsive) {
        this.jobId = jobId;
        this.telemetrySink = telemetrySink;
        this.config = config;
        this.onUnresponsive = onUnresponsive;
        this.startedAtMillis = System.currentTimeMillis();
        this.lastActivityAtMillis = startedAtMillis;
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
        lastActivityAtMillis = System.currentTimeMillis();
        sawActivity = true;
    }

    boolean isUnresponsive() {
        return unresponsive;
    }

    private void tick() {
        if (telemetrySink != null) {
            // Liveness rides the telemetry pipe: an entry-less Report whose timestamp is
            // the job's last-activity time. The coordinator max-records it and skips the
            // empty entries, so silent jobs still surface a last-activity without a
            // dedicated RPC (see CLAUDE.md "State ownership").
            telemetrySink.accept(Report.newBuilder()
                    .setJobId(jobId)
                    .setTimestampMs(lastActivityAtMillis)
                    .build());
        }
        if (unresponsive || !config.autoKill()) {
            return;
        }
        long now = System.currentTimeMillis();
        boolean stalled = sawActivity
                ? now - lastActivityAtMillis > (long) config.maxMissedPings() * config.pingIntervalMs()
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
