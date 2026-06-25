package com.scheduler.coordinator;

import com.scheduler.core.Job;
import com.scheduler.core.WorkerInfo;
import com.scheduler.proto.job.StatusUpdate;
import com.scheduler.proto.v1.FailureReason;
import com.scheduler.proto.v1.JobState;
import io.prometheus.client.CollectorRegistry;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CoordinatorMetricsTest {

    private static final WorkerInfo WORKER = new WorkerInfo("w1", "localhost", 8192, 8, false,
            Set.of(), Instant.now(), Instant.now());

    private static double counterValue(String name, String... labels) {
        Double value = labels.length == 0
                ? CollectorRegistry.defaultRegistry.getSampleValue(name)
                : CollectorRegistry.defaultRegistry.getSampleValue(name, new String[]{"status"}, labels);
        return value == null ? 0 : value;
    }

    @Test
    void testCountersAndGauges() {
        JobManager jobManager = new JobManager(new com.scheduler.coordinator.persistence.InMemoryJobStore());
        CoordinatorMetrics.init(jobManager, null);

        double submittedBefore = counterValue("scheduler_jobs_submitted_total");
        double completedBefore = counterValue("scheduler_jobs_finished_total", "completed");

        jobManager.submit("job-1", new Job("metrics-job", "img:latest", null, 0, null, null));
        assertEquals(submittedBefore + 1, counterValue("scheduler_jobs_submitted_total"));
        assertEquals(1, jobManager.queueDepth());
        assertEquals(1, jobManager.jobCountsByState().get(JobState.JOB_STATE_QUEUED));

        jobManager.claimNextJob(WORKER);
        assertEquals(0, jobManager.queueDepth());
        // Queue-wait histogram observed the claim.
        assertTrue(counterValue("scheduler_job_queue_wait_seconds_count") >= 1);

        jobManager.handleStatusUpdate(jobUpdate("job-1", JobState.JOB_STATE_RUNNING));
        jobManager.handleStatusUpdate(jobUpdate("job-1", JobState.JOB_STATE_COMPLETED));
        assertEquals(completedBefore + 1, counterValue("scheduler_jobs_finished_total", "completed"));
        assertEquals(1, jobManager.jobCountsByState().get(JobState.JOB_STATE_COMPLETED));
    }

    private static StatusUpdate jobUpdate(String jobId, JobState state) {
        return StatusUpdate.newBuilder().setJobId(jobId).setJobState(state).build();
    }

    @Test
    void testHeartbeatFailureCountsAsFailed() {
        JobManager jobManager = new JobManager(new com.scheduler.coordinator.persistence.InMemoryJobStore());
        double failedBefore = counterValue("scheduler_jobs_finished_total", "failed");

        jobManager.submit("job-hb", new Job("hb-job", "img:latest", null, 0, null, null));
        jobManager.claimNextJob(WORKER);
        jobManager.failJobsForWorker("w1", FailureReason.FAILURE_REASON_HEARTBEAT_LOST);

        assertEquals(failedBefore + 1, counterValue("scheduler_jobs_finished_total", "failed"));
    }
}
