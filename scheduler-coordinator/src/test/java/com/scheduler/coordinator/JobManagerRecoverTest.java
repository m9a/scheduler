package com.scheduler.coordinator;

import com.scheduler.core.Job;
import com.scheduler.core.JobStatus;
import com.scheduler.core.ResourceRequirements;
import com.scheduler.coordinator.persistence.InMemoryJobStore;
import com.scheduler.proto.v1.FailureReason;
import com.scheduler.proto.v1.JobState;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JobManagerRecoverTest {

    private Job sampleJob() {
        return new Job("etl", "img:v1", Map.of(), 0, List.of(), new ResourceRequirements(0, 0, false, java.util.Set.of()));
    }

    private JobStatus job(String id, JobState state, Instant completedAt) {
        return new JobStatus(id, sampleJob(), state, new LinkedHashMap<>(),
                Instant.ofEpochMilli(100),
                state == JobState.JOB_STATE_QUEUED ? null : Instant.ofEpochMilli(200),
                completedAt, null, null);
    }

    // recover() reloads non-terminal jobs, re-queues QUEUED, rebuilds the
    // jobId→worker map for in-flight jobs, and leaves terminal jobs in the store.
    @Test
    void recoverRebuildsState() {
        InMemoryJobStore store = new InMemoryJobStore();
        store.save(job("queued", JobState.JOB_STATE_QUEUED, null), null);
        store.save(job("running", JobState.JOB_STATE_RUNNING, null), "w-1");
        store.save(job("done", JobState.JOB_STATE_COMPLETED, Instant.ofEpochMilli(300)), "w-1");

        JobManager manager = new JobManager(store);
        manager.recover();

        // QUEUED re-queued; terminal job not loaded into memory.
        assertEquals(1, manager.queueDepth());
        assertEquals(JobState.JOB_STATE_QUEUED, manager.getJob("queued").state());
        assertEquals(JobState.JOB_STATE_RUNNING, manager.getJob("running").state());
        assertThrows(Exception.class, () -> manager.getJob("done"));

        // jobWorker rebuilt: failing w-1's jobs hits the reloaded in-flight job.
        assertEquals(1, manager.failJobsForWorker("w-1", FailureReason.FAILURE_REASON_HEARTBEAT_LOST));
    }
}
