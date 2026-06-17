package com.scheduler.worker;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.HTTPServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Prometheus metrics for the worker, scraped from {@code /metrics}. Created by
 * WorkerAgent; the server and the container sampler only start when a metrics
 * port is configured ({@code metricsPort} in config.yaml, 0 = disabled).
 *
 * <p>Container CPU/memory come from {@code docker stats} sampled on a fixed
 * interval for the currently running job container — jobs never instrument
 * themselves for machine metrics. Host GPU utilization comes from
 * {@code nvidia-smi} when the worker is configured with {@code resources.gpu}.
 *
 * <pre>
 * WorkerAgent.executeJob ──► jobStarted()/jobFinished()   (running gauge, duration)
 * sampler thread         ──► docker stats / nvidia-smi    (cpu/mem/gpu gauges)
 * Prometheus             ──► GET /metrics (scrape)
 * </pre>
 */
final class WorkerMetrics {

    private static final Logger log = LoggerFactory.getLogger(WorkerMetrics.class);
    private static final long SAMPLE_INTERVAL_S = 10;
    private static final long SAMPLE_TIMEOUT_S = 5;

    static final Gauge JOBS_RUNNING = Gauge.build()
            .name("scheduler_worker_jobs_running")
            .help("Jobs currently executing on this worker").register();

    static final Counter JOBS_KILLED_UNRESPONSIVE = Counter.build()
            .name("scheduler_worker_jobs_killed_unresponsive_total")
            .help("Jobs killed by the worker after going unresponsive (no liveness pings)").register();

    static final Histogram JOB_DURATION = Histogram.build()
            .name("scheduler_worker_job_duration_seconds")
            .labelNames("outcome")
            .help("Wall-clock job duration by outcome (completed/failed/killed)")
            .buckets(1, 10, 60, 300, 1800, 7200).register();

    static final Gauge CONTAINER_CPU = Gauge.build()
            .name("scheduler_job_container_cpu_percent")
            .labelNames("job_id", "job_name")
            .help("CPU usage of the job container, from docker stats").register();

    static final Gauge CONTAINER_MEM = Gauge.build()
            .name("scheduler_job_container_memory_used_bytes")
            .labelNames("job_id", "job_name")
            .help("Memory usage of the job container, from docker stats").register();

    static final Gauge GPU_UTILIZATION = Gauge.build()
            .name("scheduler_gpu_utilization_percent")
            .labelNames("gpu")
            .help("Host GPU utilization, from nvidia-smi").register();

    static final Gauge GPU_MEMORY = Gauge.build()
            .name("scheduler_gpu_memory_used_bytes")
            .labelNames("gpu")
            .help("Host GPU memory in use, from nvidia-smi").register();

    private final boolean sampleGpu;
    private HTTPServer server;
    private ScheduledExecutorService sampler;

    // The single running job (worker executes one at a time); null between jobs.
    private volatile RunningJob runningJob;

    private record RunningJob(String jobId, String jobName, long startMs) {}

    WorkerMetrics(boolean sampleGpu) {
        this.sampleGpu = sampleGpu;
    }

    void start(int port) throws IOException {
        server = new HTTPServer(port);
        sampler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "metrics-sampler");
            t.setDaemon(true);
            return t;
        });
        sampler.scheduleAtFixedRate(this::sample, SAMPLE_INTERVAL_S, SAMPLE_INTERVAL_S, TimeUnit.SECONDS);
        log.info("Worker metrics server listening on :{}/metrics (sampling every {}s)", port, SAMPLE_INTERVAL_S);
    }

    void stop() {
        if (sampler != null) {
            sampler.shutdown();
        }
        if (server != null) {
            server.close();
        }
    }

    void jobStarted(String jobId, String jobName) {
        runningJob = new RunningJob(jobId, jobName, System.currentTimeMillis());
        JOBS_RUNNING.inc();
    }

    void jobFinished(String jobId, String jobName, String outcome) {
        RunningJob job = runningJob;
        runningJob = null;
        JOBS_RUNNING.dec();
        if (job != null) {
            JOB_DURATION.labels(outcome.toLowerCase(Locale.ROOT))
                    .observe((System.currentTimeMillis() - job.startMs()) / 1000.0);
        }
        // Drop the per-job series so finished jobs don't linger as stale gauges.
        CONTAINER_CPU.remove(jobId, jobName);
        CONTAINER_MEM.remove(jobId, jobName);
    }

    private void sample() {
        try {
            RunningJob job = runningJob;
            if (job != null) {
                sampleContainer(job);
            }
            if (sampleGpu) {
                sampleGpu();
            }
        } catch (Exception e) {
            log.warn("Metrics sampling failed: {}", e.getMessage());
        }
    }

    private void sampleContainer(RunningJob job) throws IOException, InterruptedException {
        String line = runOneLine("docker", "stats", "--no-stream",
                "--format", "{{.CPUPerc}};{{.MemUsage}}", "job-" + job.jobId());
        if (line == null) {
            return;  // container not up yet or already gone — sampler retries next tick
        }
        String[] parts = line.split(";");
        if (parts.length != 2) {
            log.warn("Unexpected docker stats output for job {}: {}", job.jobId(), line);
            return;
        }
        CONTAINER_CPU.labels(job.jobId(), job.jobName()).set(parsePercent(parts[0]));
        // MemUsage is "used / limit" — only the used part matters here.
        CONTAINER_MEM.labels(job.jobId(), job.jobName()).set(parseSize(parts[1].split("/")[0]));
    }

    private void sampleGpu() throws IOException, InterruptedException {
        String output = runOneLine("nvidia-smi",
                "--query-gpu=index,utilization.gpu,memory.used", "--format=csv,noheader,nounits");
        if (output == null) {
            return;
        }
        // One line per GPU: "0, 87, 14321" (index, util %, memory MiB)
        for (String line : output.split("\n")) {
            String[] cols = line.split(",");
            if (cols.length != 3) {
                continue;
            }
            String gpu = cols[0].trim();
            GPU_UTILIZATION.labels(gpu).set(Double.parseDouble(cols[1].trim()));
            GPU_MEMORY.labels(gpu).set(Double.parseDouble(cols[2].trim()) * 1024 * 1024);
        }
    }

    /** Runs a sampling command with a timeout; returns trimmed stdout, or null on failure. */
    private String runOneLine(String... command) throws IOException, InterruptedException {
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!output.isEmpty()) {
                    output.append('\n');
                }
                output.append(line);
            }
        }
        if (!process.waitFor(SAMPLE_TIMEOUT_S, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            log.warn("Sampling command timed out: {}", String.join(" ", command));
            return null;
        }
        if (process.exitValue() != 0) {
            log.debug("Sampling command failed (exit {}): {}", process.exitValue(), String.join(" ", command));
            return null;
        }
        return output.isEmpty() ? null : output.toString().trim();
    }

    /** Parses docker stats CPU like "12.34%". */
    static double parsePercent(String value) {
        return Double.parseDouble(value.trim().replace("%", ""));
    }

    /** Parses docker stats sizes like "123.4MiB", "1.5GiB", "512KiB", "100B". */
    static double parseSize(String value) {
        String v = value.trim();
        double multiplier = 1;
        if (v.endsWith("GiB")) {
            multiplier = 1024L * 1024 * 1024;
            v = v.substring(0, v.length() - 3);
        } else if (v.endsWith("MiB")) {
            multiplier = 1024L * 1024;
            v = v.substring(0, v.length() - 3);
        } else if (v.endsWith("KiB")) {
            multiplier = 1024;
            v = v.substring(0, v.length() - 3);
        } else if (v.endsWith("B")) {
            v = v.substring(0, v.length() - 1);
        }
        return Double.parseDouble(v.trim()) * multiplier;
    }
}
