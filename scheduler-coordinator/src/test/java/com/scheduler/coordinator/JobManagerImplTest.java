package com.scheduler.coordinator;

import com.scheduler.core.*;
import com.scheduler.core.exception.JobNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class JobManagerImplTest {

    private JobManagerImpl jobManager;

    @BeforeEach
    void setUp() {
        jobManager = new JobManagerImpl();
    }

    @Test
    void submit() {
        Job job = new Job("test-job", "registry.example.com/job:v1", null, 5);

        JobState execution = jobManager.submit("job-1", job);

        assertEquals("job-1", execution.id());
        assertEquals("test-job", execution.job().name());
        assertEquals(JobStatus.QUEUED, execution.status());
        assertNotNull(execution.createdAt());
        assertTrue(execution.taskStates().isEmpty());
    }

    @Test
    void getJob() {
        Job job = new Job("test-job", "my-job:latest", null, 0);
        JobState submitted = jobManager.submit("job-1", job);

        JobState result = jobManager.getJob(submitted.id());

        assertEquals(submitted.id(), result.id());
    }

    @Test
    void getJobNotFound() {
        assertThrows(JobNotFoundException.class, () -> jobManager.getJob("nonexistent"));
    }

    @Test
    void claimNextJob() {
        Job job = new Job("test-job", "my-job:latest", null, 0);
        jobManager.submit("job-1", job);

        Optional<JobState> claimed = jobManager.claimNextJob("worker-1");

        assertTrue(claimed.isPresent());
        assertEquals(JobStatus.STARTING, claimed.get().status());
    }

    @Test
    void claimNextJobEmpty() {
        Optional<JobState> claimed = jobManager.claimNextJob("worker-1");

        assertTrue(claimed.isEmpty());
    }

    @Test
    void taskRunningTransitionsTaskOnly() {
        JobState claimed = submitAndClaim("job");

        jobManager.handleStatusUpdate(claimed.id(), null, null, null,
                0, "extract", TaskStatus.RUNNING, null);

        JobState updated = jobManager.getJob(claimed.id());
        // Job stays STARTING — worker sends explicit RUNNING
        assertEquals(JobStatus.STARTING, updated.status());
        assertEquals(TaskStatus.RUNNING, updated.taskStates().get(0).status());
    }

    @Test
    void jobRunningFromWorker() {
        JobState claimed = submitAndClaim("job");

        jobManager.handleStatusUpdate(claimed.id(), JobStatus.RUNNING, null, null,
                0, null, null, null);

        JobState updated = jobManager.getJob(claimed.id());
        assertEquals(JobStatus.RUNNING, updated.status());
        assertNotNull(updated.startedAt());
    }

    @Test
    void jobCompletedFromWorker() {
        JobState claimed = submitAndClaim("job");

        jobManager.handleStatusUpdate(claimed.id(), JobStatus.RUNNING, null, null,
                0, null, null, null);
        jobManager.handleStatusUpdate(claimed.id(), null, null, null,
                0, "extract", TaskStatus.RUNNING, null);
        jobManager.handleStatusUpdate(claimed.id(), null, null, null,
                0, "extract", TaskStatus.COMPLETED, null);
        jobManager.handleStatusUpdate(claimed.id(), JobStatus.COMPLETED, null, null,
                0, null, null, null);

        JobState updated = jobManager.getJob(claimed.id());
        assertEquals(JobStatus.COMPLETED, updated.status());
        assertNotNull(updated.completedAt());
    }

    @Test
    void jobFailedFromWorker() {
        JobState claimed = submitAndClaim("job");

        jobManager.handleStatusUpdate(claimed.id(), JobStatus.RUNNING, null, null,
                0, null, null, null);
        jobManager.handleStatusUpdate(claimed.id(), JobStatus.FAILED,
                FailureReason.PROCESS_EXITED, "exit code 1",
                0, null, null, null);

        JobState updated = jobManager.getJob(claimed.id());
        assertEquals(JobStatus.FAILED, updated.status());
        assertEquals(FailureReason.PROCESS_EXITED, updated.failureReason());
        assertEquals("exit code 1", updated.failureDetail());
    }

    @Test
    void jobKilled() {
        JobState claimed = submitAndClaim("job");

        jobManager.handleStatusUpdate(claimed.id(), JobStatus.RUNNING, null, null,
                0, null, null, null);
        jobManager.handleStatusUpdate(claimed.id(), JobStatus.KILLED,
                FailureReason.PROCESS_TIMEOUT, "PT10M",
                0, null, null, null);

        JobState updated = jobManager.getJob(claimed.id());
        assertEquals(JobStatus.KILLED, updated.status());
        assertEquals(FailureReason.PROCESS_TIMEOUT, updated.failureReason());
        assertEquals("PT10M", updated.failureDetail());
        assertNotNull(updated.completedAt());
    }

    @Test
    void taskFailedWithError() {
        JobState claimed = submitAndClaim("job");

        jobManager.handleStatusUpdate(claimed.id(), JobStatus.RUNNING, null, null,
                0, null, null, null);
        jobManager.handleStatusUpdate(claimed.id(), null, null, null,
                0, "extract", TaskStatus.RUNNING, null);
        jobManager.handleStatusUpdate(claimed.id(), null, null, null,
                0, "extract", TaskStatus.FAILED, "out of memory");

        JobState updated = jobManager.getJob(claimed.id());
        assertEquals(TaskStatus.FAILED, updated.taskStates().get(0).status());
        assertEquals("out of memory", updated.taskStates().get(0).errorMessage());
    }

    @Test
    void lateStatusIgnoredForTerminalJob() {
        JobState claimed = submitAndClaim("job");

        jobManager.handleStatusUpdate(claimed.id(), JobStatus.FAILED,
                FailureReason.PROCESS_EXITED, "exit code 1",
                0, null, null, null);

        // Late task status should be ignored
        jobManager.handleStatusUpdate(claimed.id(), null, null, null,
                0, "extract", TaskStatus.RUNNING, null);

        JobState updated = jobManager.getJob(claimed.id());
        assertEquals(JobStatus.FAILED, updated.status());
        assertTrue(updated.taskStates().isEmpty());
    }

    @Test
    void taskNameUpdated() {
        JobState claimed = submitAndClaim("job");

        jobManager.handleStatusUpdate(claimed.id(), null, null, null,
                0, "extract", TaskStatus.RUNNING, null);

        JobState updated = jobManager.getJob(claimed.id());
        assertEquals("extract", updated.taskStates().get(0).taskName());
    }

    @Test
    void claimedJobStaysStartingWithoutUpdates() {
        JobState claimed = submitAndClaim("idle");

        JobState current = jobManager.getJob(claimed.id());
        assertEquals(JobStatus.STARTING, current.status());
        assertTrue(current.taskStates().isEmpty());
    }

    @Test
    void testFailJobsForWorker() {
        JobState claimed = submitAndClaim("hb");

        int count = jobManager.failJobsForWorker("worker-1", FailureReason.HEARTBEAT_LOST);

        assertEquals(1, count);
        JobState updated = jobManager.getJob(claimed.id());
        assertEquals(JobStatus.FAILED, updated.status());
        assertEquals(FailureReason.HEARTBEAT_LOST, updated.failureReason());
        assertNotNull(updated.completedAt());
    }

    @Test
    void testFailJobsForWorkerSkipsTerminal() {
        JobState claimed = submitAndClaim("done");
        jobManager.handleStatusUpdate(claimed.id(), JobStatus.RUNNING, null, null,
                0, null, null, null);
        jobManager.handleStatusUpdate(claimed.id(), JobStatus.COMPLETED, null, null,
                0, null, null, null);

        int count = jobManager.failJobsForWorker("worker-1", FailureReason.HEARTBEAT_LOST);

        assertEquals(0, count);
        assertEquals(JobStatus.COMPLETED, jobManager.getJob(claimed.id()).status());
    }

    @Test
    void testFailJobsForWorkerMultipleJobs() {
        Job job1 = new Job("job-a", "img:latest", null, 0);
        Job job2 = new Job("job-b", "img:latest", null, 0);
        jobManager.submit("id-a", job1);
        jobManager.submit("id-b", job2);
        jobManager.claimNextJob("worker-1");
        jobManager.claimNextJob("worker-1");

        // Move one to RUNNING
        jobManager.handleStatusUpdate("id-b", JobStatus.RUNNING, null, null,
                0, null, null, null);

        int count = jobManager.failJobsForWorker("worker-1", FailureReason.HEARTBEAT_LOST);

        assertEquals(2, count);
        assertEquals(JobStatus.FAILED, jobManager.getJob("id-a").status());
        assertEquals(JobStatus.FAILED, jobManager.getJob("id-b").status());
    }

    private JobState submitAndClaim(String name) {
        Job job = new Job(name, "my-job:latest", null, 0);
        jobManager.submit("job-" + name, job);
        return jobManager.claimNextJob("worker-1").orElseThrow();
    }
}
