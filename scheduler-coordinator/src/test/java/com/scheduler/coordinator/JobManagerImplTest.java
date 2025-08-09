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
        Job job = new Job("test-job", "/path/to/jar", "com.example.Main", 5);

        JobState execution = jobManager.submit(job);

        assertEquals("test-job", execution.job().name());
        assertEquals(JobStatus.QUEUED, execution.status());
        assertNotNull(execution.id());
        assertNotNull(execution.createdAt());
        assertTrue(execution.taskStates().isEmpty());
    }

    @Test
    void getJob() {
        Job job = new Job("test-job", "/jar", null, 0);
        JobState submitted = jobManager.submit(job);

        JobState result = jobManager.getJob(submitted.id());

        assertEquals(submitted.id(), result.id());
    }

    @Test
    void getJobNotFound() {
        assertThrows(JobNotFoundException.class, () -> jobManager.getJob("nonexistent"));
    }

    @Test
    void claimNextJob() {
        Job job = new Job("test-job", "/jar", null, 0);
        jobManager.submit(job);

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
    void taskRunningTransitionsJobToRunning() {
        JobState claimed = submitAndClaim("job");

        jobManager.updateTaskStatus(claimed.id(), 0, "task-1", TaskStatus.RUNNING, null);

        JobState updated = jobManager.getJob(claimed.id());
        assertEquals(JobStatus.RUNNING, updated.status());
        assertNotNull(updated.startedAt());
        assertEquals(TaskStatus.RUNNING, updated.taskStates().get(0).status());
    }

    @Test
    void allTasksCompletedTransitionsJobToCompleted() {
        JobState claimed = submitAndClaim("job");

        jobManager.updateTaskStatus(claimed.id(), 0, "task-1", TaskStatus.RUNNING, null);
        jobManager.updateTaskStatus(claimed.id(), 0, "task-1", TaskStatus.COMPLETED, null);
        jobManager.updateTaskStatus(claimed.id(), 1, "task-2", TaskStatus.RUNNING, null);
        jobManager.updateTaskStatus(claimed.id(), 1, "task-2", TaskStatus.COMPLETED, null);
        jobManager.finalizeJob(claimed.id());

        JobState updated = jobManager.getJob(claimed.id());
        assertEquals(JobStatus.COMPLETED, updated.status());
        assertNotNull(updated.completedAt());
    }

    @Test
    void taskFailedTransitionsJobToFailed() {
        JobState claimed = submitAndClaim("job");

        jobManager.updateTaskStatus(claimed.id(), 0, "task-1", TaskStatus.RUNNING, null);
        jobManager.updateTaskStatus(claimed.id(), 0, "task-1", TaskStatus.FAILED, "out of memory");

        JobState updated = jobManager.getJob(claimed.id());
        assertEquals(JobStatus.FAILED, updated.status());
        assertEquals("out of memory", updated.errorMessage());
        assertEquals(TaskStatus.FAILED, updated.taskStates().get(0).status());
    }

    @Test
    void finalizeJobNoOpsIfAlreadyTerminal() {
        JobState claimed = submitAndClaim("job");

        jobManager.updateTaskStatus(claimed.id(), 0, "task-1", TaskStatus.RUNNING, null);
        jobManager.updateTaskStatus(claimed.id(), 0, "task-1", TaskStatus.FAILED, "boom");
        jobManager.finalizeJob(claimed.id());

        JobState updated = jobManager.getJob(claimed.id());
        assertEquals(JobStatus.FAILED, updated.status());
        assertEquals("boom", updated.errorMessage());
    }

    private JobState submitAndClaim(String name) {
        Job job = new Job(name, "/jar", null, 0);
        jobManager.submit(job);
        return jobManager.claimNextJob("worker-1").orElseThrow();
    }
}
