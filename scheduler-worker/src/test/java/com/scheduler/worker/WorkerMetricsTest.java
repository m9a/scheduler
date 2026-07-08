package com.scheduler.worker;

import io.prometheus.client.CollectorRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerMetricsTest {

    private static double sample(String name, String[] labelNames, String[] labels) {
        Double value = CollectorRegistry.defaultRegistry.getSampleValue(name, labelNames, labels);
        return value == null ? 0 : value;
    }

    @Test
    void testParsePercent() {
        assertEquals(12.34, WorkerMetrics.parsePercent("12.34%"));
        assertEquals(0.0, WorkerMetrics.parsePercent("0.00%"));
        assertEquals(150.5, WorkerMetrics.parsePercent(" 150.5% "));
    }

    @Test
    void testParseSize() {
        assertEquals(100, WorkerMetrics.parseSize("100B"));
        assertEquals(512 * 1024, WorkerMetrics.parseSize("512KiB"));
        assertEquals(123.4 * 1024 * 1024, WorkerMetrics.parseSize("123.4MiB"), 1);
        assertEquals(1.5 * 1024 * 1024 * 1024, WorkerMetrics.parseSize("1.5GiB"), 1);
        assertEquals(2L * 1024 * 1024 * 1024, WorkerMetrics.parseSize(" 2GiB "), 1);
    }

    // One job through start → finish: running gauge up then down, duration
    // observed, and the per-job container series removed at the end.
    @Test
    void testJobLifecycle() {
        WorkerMetrics metrics = new WorkerMetrics(false);
        double completedBefore = sample("scheduler_worker_job_duration_seconds_count",
                new String[]{"outcome"}, new String[]{"completed"});

        metrics.jobStarted("job-m1", "metrics-job");
        assertEquals(1, CollectorRegistry.defaultRegistry.getSampleValue("scheduler_worker_jobs_running"));

        // Sampler would normally set these; set directly to verify cleanup on finish.
        WorkerMetrics.CONTAINER_CPU.labels("job-m1", "metrics-job").set(42);

        metrics.jobFinished("job-m1", "metrics-job", "COMPLETED");
        assertEquals(0, CollectorRegistry.defaultRegistry.getSampleValue("scheduler_worker_jobs_running"));
        assertEquals(completedBefore + 1, sample("scheduler_worker_job_duration_seconds_count",
                new String[]{"outcome"}, new String[]{"completed"}));
        // Per-job container series removed so finished jobs don't linger.
        assertNull(CollectorRegistry.defaultRegistry.getSampleValue("scheduler_job_container_cpu_percent",
                new String[]{"job_id", "job_name"}, new String[]{"job-m1", "metrics-job"}));
    }
}
