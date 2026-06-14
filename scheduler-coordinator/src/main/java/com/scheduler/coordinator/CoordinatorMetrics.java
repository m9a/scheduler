package com.scheduler.coordinator;

import com.scheduler.proto.v1.JobState;
import com.scheduler.coordinator.worker.WorkerHandler;
import io.prometheus.client.Collector;
import io.prometheus.client.Counter;
import io.prometheus.client.GaugeMetricFamily;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.HTTPServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Prometheus metrics for the coordinator, scraped from {@code /metrics}.
 *
 * <p>Counters/histograms are static and incremented inline by JobManager and
 * WorkerHandler at the relevant mutation points. Current-level gauges (queue depth,
 * jobs by status, registered workers) are computed at scrape time by reading the
 * live state via {@link #init} — no background sampling thread, never stale.
 *
 * <pre>
 * Coordinator.main ──► init(jobManager, workerHandler) + startServer(port)
 * Prometheus       ──► GET /metrics (scrape)
 * </pre>
 */
public final class CoordinatorMetrics {

    private static final Logger log = LoggerFactory.getLogger(CoordinatorMetrics.class);

    static final Counter JOBS_SUBMITTED = Counter.build()
            .name("scheduler_jobs_submitted_total")
            .help("Jobs accepted by submit()").register();

    // Terminal outcomes only (completed/failed/killed/cancelled) — rate() gives throughput.
    static final Counter JOBS_FINISHED = Counter.build()
            .name("scheduler_jobs_finished_total")
            .labelNames("status")
            .help("Jobs that reached a terminal status").register();

    static final Counter TELEMETRY_REPORTS = Counter.build()
            .name("scheduler_telemetry_reports_total")
            .help("Telemetry reports forwarded by workers via ReportTelemetry").register();

    // Public: incremented from the worker subpackage (WorkerHandler heartbeat monitor).
    public static final Counter HEARTBEAT_LOSSES = Counter.build()
            .name("scheduler_worker_heartbeat_losses_total")
            .help("Workers evicted because their heartbeat timed out").register();

    // Submit → claim latency: how long jobs sit in the queue before a worker takes them.
    static final Histogram QUEUE_WAIT = Histogram.build()
            .name("scheduler_job_queue_wait_seconds")
            .help("Time from job submission until a worker claims it")
            .buckets(0.1, 0.5, 1, 5, 15, 60, 300, 1800).register();

    // Set by init(); read at scrape time. Volatile: scrape happens on the HTTP thread.
    private static volatile JobManager jobManager;
    private static volatile WorkerHandler workerHandler;

    static {
        new StateCollector().register();
    }

    private HTTPServer server;

    /** Points the scrape-time gauges at the live coordinator state. */
    public static void init(JobManager jobs, WorkerHandler workers) {
        jobManager = jobs;
        workerHandler = workers;
    }

    /** Metric label for a job state — the proto name without its {@code JOB_STATE_} prefix. */
    static String jobStateLabel(JobState state) {
        return state.name().substring("JOB_STATE_".length()).toLowerCase();
    }

    public void startServer(int port) throws IOException {
        server = new HTTPServer(port);
        log.info("Metrics server listening on :{}/metrics", port);
    }

    public void stop() {
        if (server != null) {
            server.close();
        }
    }

    /** Computes current-level gauges from live coordinator state on each scrape. */
    private static final class StateCollector extends Collector {
        @Override
        public List<MetricFamilySamples> collect() {
            List<MetricFamilySamples> samples = new ArrayList<>();
            JobManager jm = jobManager;
            WorkerHandler wh = workerHandler;
            if (jm != null) {
                GaugeMetricFamily jobsByStatus = new GaugeMetricFamily(
                        "scheduler_jobs", "Jobs currently known, by status", List.of("status"));
                Map<JobState, Integer> counts = jm.jobCountsByState();
                for (JobState state : JobState.values()) {
                    if (state == JobState.JOB_STATE_UNSPECIFIED || state == JobState.UNRECOGNIZED) {
                        continue;  // synthetic proto values — not real job states
                    }
                    jobsByStatus.addMetric(List.of(jobStateLabel(state)),
                            counts.getOrDefault(state, 0));
                }
                samples.add(jobsByStatus);

                GaugeMetricFamily queueDepth = new GaugeMetricFamily(
                        "scheduler_queue_depth", "Jobs waiting to be claimed", jm.queueDepth());
                samples.add(queueDepth);
            }
            if (wh != null) {
                samples.add(new GaugeMetricFamily(
                        "scheduler_workers_registered", "Workers currently registered",
                        wh.workerCount()));
            }
            return samples;
        }
    }
}
