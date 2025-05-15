package com.scheduler.coordinator;

import com.scheduler.core.*;
import com.scheduler.core.exception.JobNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
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
        Job job = new Job("test-job", "/path/to/jar", "com.example.Main", List.of(
                new Task("task-1"),
                new Task("task-2")
        ), 5);

        JobExecution execution = jobManager.submit(job);

        assertEquals("test-job", execution.job().name());
        assertEquals(JobStatus.QUEUED, execution.status());
        assertNotNull(execution.id());
        assertNotNull(execution.createdAt());
        assertEquals(2, execution.taskExecutions().size());
        assertEquals(0, execution.taskExecutions().get(0).taskIndex());
        assertEquals(1, execution.taskExecutions().get(1).taskIndex());
    }

    @Test
    void submitEmptyTasks() {
        assertThrows(IllegalArgumentException.class, () ->
                new Job("test-job", "/path/to/jar", null, List.of(), 0));
    }

    @Test
    void getJob() {
        Job job = new Job("test-job", "/jar", null, List.of(new Task("t")), 0);
        JobExecution submitted = jobManager.submit(job);

        JobExecution result = jobManager.getJob(submitted.id());

        assertEquals(submitted.id(), result.id());
    }

    @Test
    void getJobNotFound() {
        assertThrows(JobNotFoundException.class, () -> jobManager.getJob("nonexistent"));
    }

    @Test
    void claimNextJob() {
        Job job = new Job("test-job", "/jar", null, List.of(new Task("t")), 0);
        jobManager.submit(job);

        Optional<JobExecution> claimed = jobManager.claimNextJob("worker-1");

        assertTrue(claimed.isPresent());
        assertEquals(JobStatus.STARTING, claimed.get().status());
    }

    @Test
    void claimNextJobEmpty() {
        Optional<JobExecution> claimed = jobManager.claimNextJob("worker-1");

        assertTrue(claimed.isEmpty());
    }

    @Test
    void taskRunningTransitionsJobToRunning() {
        JobExecution claimed = submitAndClaim("job", "task-1", "task-2");

        jobManager.updateTaskStatus(claimed.id(), 0, TaskStatus.RUNNING, null);

        JobExecution updated = jobManager.getJob(claimed.id());
        assertEquals(JobStatus.RUNNING, updated.status());
        assertNotNull(updated.startedAt());
        assertEquals(TaskStatus.RUNNING, updated.taskExecutions().get(0).status());
    }

    @Test
    void allTasksCompletedTransitionsJobToCompleted() {
        JobExecution claimed = submitAndClaim("job", "task-1", "task-2");

        jobManager.updateTaskStatus(claimed.id(), 0, TaskStatus.RUNNING, null);
        jobManager.updateTaskStatus(claimed.id(), 0, TaskStatus.COMPLETED, null);
        jobManager.updateTaskStatus(claimed.id(), 1, TaskStatus.RUNNING, null);
        jobManager.updateTaskStatus(claimed.id(), 1, TaskStatus.COMPLETED, null);

        JobExecution updated = jobManager.getJob(claimed.id());
        assertEquals(JobStatus.COMPLETED, updated.status());
        assertNotNull(updated.completedAt());
    }

    @Test
    void taskFailedTransitionsJobToFailed() {
        JobExecution claimed = submitAndClaim("job", "task-1", "task-2");

        jobManager.updateTaskStatus(claimed.id(), 0, TaskStatus.RUNNING, null);
        jobManager.updateTaskStatus(claimed.id(), 0, TaskStatus.FAILED, "out of memory");

        JobExecution updated = jobManager.getJob(claimed.id());
        assertEquals(JobStatus.FAILED, updated.status());
        assertEquals("out of memory", updated.errorMessage());
        assertEquals(TaskStatus.FAILED, updated.taskExecutions().get(0).status());
        assertEquals(TaskStatus.SKIPPED, updated.taskExecutions().get(1).status());
    }

    @Test
    void invalidTaskIndex() {
        JobExecution claimed = submitAndClaim("job", "task-1");

        assertThrows(IllegalArgumentException.class, () ->
                jobManager.updateTaskStatus(claimed.id(), 5, TaskStatus.RUNNING, null));
    }

    private JobExecution submitAndClaim(String name, String... taskNames) {
        List<Task> tasks = java.util.Arrays.stream(taskNames).map(Task::new).toList();
        Job job = new Job(name, "/jar", null, tasks, 0);
        jobManager.submit(job);
        return jobManager.claimNextJob("worker-1").orElseThrow();
    }
}
