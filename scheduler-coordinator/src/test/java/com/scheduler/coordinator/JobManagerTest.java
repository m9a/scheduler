package com.scheduler.coordinator;

import com.scheduler.core.Job;
import com.scheduler.core.JobStatus;
import com.scheduler.core.ResourceRequirements;
import com.scheduler.core.WorkerInfo;
import com.scheduler.core.exception.JobNotFoundException;
import com.scheduler.proto.job.StatusUpdate;
import com.scheduler.proto.v1.FailureReason;
import com.scheduler.proto.v1.JobState;
import com.scheduler.proto.v1.TaskState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class JobManagerTest {

    private JobManager jobManager;

    @BeforeEach
    void setUp() {
        jobManager = new JobManager();
    }

    @Test
    void submit() {
        Job job = new Job("test-job", "registry.example.com/job:v1", null, 5, null, null);

        JobStatus execution = jobManager.submit("job-1", job);

        assertEquals("job-1", execution.id());
        assertEquals("test-job", execution.job().name());
        assertEquals(JobState.JOB_STATE_QUEUED, execution.state());
        assertNotNull(execution.createdAt());
        assertTrue(execution.taskStatuses().isEmpty());
    }

    @Test
    void getJob() {
        Job job = new Job("test-job", "my-job:latest", null, 0, null, null);
        JobStatus submitted = jobManager.submit("job-1", job);

        JobStatus result = jobManager.getJob(submitted.id());

        assertEquals(submitted.id(), result.id());
    }

    @Test
    void getJobNotFound() {
        assertThrows(JobNotFoundException.class, () -> jobManager.getJob("nonexistent"));
    }

    @Test
    void claimNextJob() {
        Job job = new Job("test-job", "my-job:latest", null, 0, null, null);
        jobManager.submit("job-1", job);

        Optional<JobStatus> claimed = jobManager.claimNextJob(DEFAULT_WORKER);

        assertTrue(claimed.isPresent());
        assertEquals(JobState.JOB_STATE_STARTING, claimed.get().state());
    }

    @Test
    void claimNextJobEmpty() {
        Optional<JobStatus> claimed = jobManager.claimNextJob(DEFAULT_WORKER);

        assertTrue(claimed.isEmpty());
    }

    @Test
    void taskRunningTransitionsTaskOnly() {
        JobStatus claimed = submitAndClaim("job");

        jobManager.handleStatusUpdate(taskUpdate(claimed.id(), 0, "extract", TaskState.TASK_STATE_RUNNING, null));

        JobStatus updated = jobManager.getJob(claimed.id());
        // Job stays STARTING — the coordinator never infers RUNNING; the worker sends it.
        assertEquals(JobState.JOB_STATE_STARTING, updated.state());
        assertEquals(TaskState.TASK_STATE_RUNNING, updated.taskStatuses().get(0).state());
    }

    @Test
    void jobRunningFromWorker() {
        JobStatus claimed = submitAndClaim("job");

        jobManager.handleStatusUpdate(jobUpdate(claimed.id(), JobState.JOB_STATE_RUNNING, null, null));

        JobStatus updated = jobManager.getJob(claimed.id());
        assertEquals(JobState.JOB_STATE_RUNNING, updated.state());
        assertNotNull(updated.startedAt());
    }

    @Test
    void jobCompletedFromWorker() {
        JobStatus claimed = submitAndClaim("job");

        jobManager.handleStatusUpdate(jobUpdate(claimed.id(), JobState.JOB_STATE_RUNNING, null, null));
        jobManager.handleStatusUpdate(taskUpdate(claimed.id(), 0, "extract", TaskState.TASK_STATE_RUNNING, null));
        jobManager.handleStatusUpdate(taskUpdate(claimed.id(), 0, "extract", TaskState.TASK_STATE_COMPLETED, null));
        jobManager.handleStatusUpdate(jobUpdate(claimed.id(), JobState.JOB_STATE_COMPLETED, null, null));

        JobStatus updated = jobManager.getJob(claimed.id());
        assertEquals(JobState.JOB_STATE_COMPLETED, updated.state());
        assertNotNull(updated.completedAt());
    }

    @Test
    void jobFailedFromWorker() {
        JobStatus claimed = submitAndClaim("job");

        jobManager.handleStatusUpdate(jobUpdate(claimed.id(), JobState.JOB_STATE_RUNNING, null, null));
        jobManager.handleStatusUpdate(jobUpdate(claimed.id(), JobState.JOB_STATE_FAILED,
                FailureReason.FAILURE_REASON_PROCESS_EXITED, "exit code 1"));

        JobStatus updated = jobManager.getJob(claimed.id());
        assertEquals(JobState.JOB_STATE_FAILED, updated.state());
        assertEquals(FailureReason.FAILURE_REASON_PROCESS_EXITED, updated.failureReason());
        assertEquals("exit code 1", updated.failureDetail());
    }

    @Test
    void jobKilled() {
        JobStatus claimed = submitAndClaim("job");

        jobManager.handleStatusUpdate(jobUpdate(claimed.id(), JobState.JOB_STATE_RUNNING, null, null));
        jobManager.handleStatusUpdate(jobUpdate(claimed.id(), JobState.JOB_STATE_KILLED,
                FailureReason.FAILURE_REASON_PROCESS_TIMEOUT, "PT10M"));

        JobStatus updated = jobManager.getJob(claimed.id());
        assertEquals(JobState.JOB_STATE_KILLED, updated.state());
        assertEquals(FailureReason.FAILURE_REASON_PROCESS_TIMEOUT, updated.failureReason());
        assertEquals("PT10M", updated.failureDetail());
        assertNotNull(updated.completedAt());
    }

    @Test
    void taskFailedWithError() {
        JobStatus claimed = submitAndClaim("job");

        jobManager.handleStatusUpdate(jobUpdate(claimed.id(), JobState.JOB_STATE_RUNNING, null, null));
        jobManager.handleStatusUpdate(taskUpdate(claimed.id(), 0, "extract", TaskState.TASK_STATE_RUNNING, null));
        jobManager.handleStatusUpdate(taskUpdate(claimed.id(), 0, "extract", TaskState.TASK_STATE_FAILED, "out of memory"));

        JobStatus updated = jobManager.getJob(claimed.id());
        assertEquals(TaskState.TASK_STATE_FAILED, updated.taskStatuses().get(0).state());
        assertEquals("out of memory", updated.taskStatuses().get(0).errorMessage());
    }

    @Test
    void lateStatusIgnoredForTerminalJob() {
        JobStatus claimed = submitAndClaim("job");

        jobManager.handleStatusUpdate(jobUpdate(claimed.id(), JobState.JOB_STATE_FAILED,
                FailureReason.FAILURE_REASON_PROCESS_EXITED, "exit code 1"));

        // Late task status should be ignored
        jobManager.handleStatusUpdate(taskUpdate(claimed.id(), 0, "extract", TaskState.TASK_STATE_RUNNING, null));

        JobStatus updated = jobManager.getJob(claimed.id());
        assertEquals(JobState.JOB_STATE_FAILED, updated.state());
        assertTrue(updated.taskStatuses().isEmpty());
    }

    @Test
    void taskNameUpdated() {
        JobStatus claimed = submitAndClaim("job");

        jobManager.handleStatusUpdate(taskUpdate(claimed.id(), 0, "extract", TaskState.TASK_STATE_RUNNING, null));

        JobStatus updated = jobManager.getJob(claimed.id());
        assertEquals("extract", updated.taskStatuses().get(0).taskName());
    }

    @Test
    void claimedJobStaysStartingWithoutUpdates() {
        JobStatus claimed = submitAndClaim("idle");

        JobStatus current = jobManager.getJob(claimed.id());
        assertEquals(JobState.JOB_STATE_STARTING, current.state());
        assertTrue(current.taskStatuses().isEmpty());
    }

    @Test
    void testFailJobsForWorker() {
        JobStatus claimed = submitAndClaim("hb");

        int count = jobManager.failJobsForWorker("worker-1", FailureReason.FAILURE_REASON_HEARTBEAT_LOST);

        assertEquals(1, count);
        JobStatus updated = jobManager.getJob(claimed.id());
        assertEquals(JobState.JOB_STATE_FAILED, updated.state());
        assertEquals(FailureReason.FAILURE_REASON_HEARTBEAT_LOST, updated.failureReason());
        assertNotNull(updated.completedAt());
    }

    @Test
    void testFailJobsForWorkerSkipsTerminal() {
        JobStatus claimed = submitAndClaim("done");
        jobManager.handleStatusUpdate(jobUpdate(claimed.id(), JobState.JOB_STATE_RUNNING, null, null));
        jobManager.handleStatusUpdate(jobUpdate(claimed.id(), JobState.JOB_STATE_COMPLETED, null, null));

        int count = jobManager.failJobsForWorker("worker-1", FailureReason.FAILURE_REASON_HEARTBEAT_LOST);

        assertEquals(0, count);
        assertEquals(JobState.JOB_STATE_COMPLETED, jobManager.getJob(claimed.id()).state());
    }

    @Test
    void testFailJobsForWorkerMultipleJobs() {
        Job job1 = new Job("job-a", "img:latest", null, 0, null, null);
        Job job2 = new Job("job-b", "img:latest", null, 0, null, null);
        jobManager.submit("id-a", job1);
        jobManager.submit("id-b", job2);
        jobManager.claimNextJob(DEFAULT_WORKER);
        jobManager.claimNextJob(DEFAULT_WORKER);

        // Move one to RUNNING
        jobManager.handleStatusUpdate(jobUpdate("id-b", JobState.JOB_STATE_RUNNING, null, null));

        int count = jobManager.failJobsForWorker("worker-1", FailureReason.FAILURE_REASON_HEARTBEAT_LOST);

        assertEquals(2, count);
        assertEquals(JobState.JOB_STATE_FAILED, jobManager.getJob("id-a").state());
        assertEquals(JobState.JOB_STATE_FAILED, jobManager.getJob("id-b").state());
    }

    @Test
    void testClaimGpuJobMatchesGpuWorker() {
        Job job = new Job("gpu-job", "img:latest", null, 0, null,
                new ResourceRequirements(0, 0, true, Set.of()));
        jobManager.submit("job-gpu", job);

        WorkerInfo gpuWorker = worker("gpu-worker", 8192, 8, true, Set.of());
        Optional<JobStatus> claimed = jobManager.claimNextJob(gpuWorker);

        assertTrue(claimed.isPresent());
        assertEquals("job-gpu", claimed.get().id());
    }

    @Test
    void testClaimGpuJobSkipsNonGpuWorker() {
        Job gpuJob = new Job("gpu-job", "img:latest", null, 0, null,
                new ResourceRequirements(0, 0, true, Set.of()));
        Job cpuJob = new Job("cpu-job", "img:latest", null, 0, null, null);
        jobManager.submit("job-gpu", gpuJob);
        jobManager.submit("job-cpu", cpuJob);

        WorkerInfo cpuWorker = worker("cpu-worker", 8192, 8, false, Set.of());
        Optional<JobStatus> claimed = jobManager.claimNextJob(cpuWorker);

        assertTrue(claimed.isPresent());
        assertEquals("job-cpu", claimed.get().id());
    }

    @Test
    void testClaimNonGpuJobSkipsGpuWorker() {
        // GPU workers are reserved: a CPU-only job must not occupy a GPU worker.
        Job cpuJob = new Job("cpu-job", "img:latest", null, 0, null, null);
        jobManager.submit("job-cpu", cpuJob);

        WorkerInfo gpuWorker = worker("gpu-worker", 8192, 8, true, Set.of());
        Optional<JobStatus> claimed = jobManager.claimNextJob(gpuWorker);

        assertTrue(claimed.isEmpty());
    }

    @Test
    void testClaimMatchesCapabilities() {
        Job job = new Job("cap-job", "img:latest", null, 0, null,
                new ResourceRequirements(0, 0, false, Set.of("avx512")));
        jobManager.submit("job-cap", job);

        WorkerInfo plainWorker = worker("plain", 8192, 8, false, Set.of());
        assertTrue(jobManager.claimNextJob(plainWorker).isEmpty());

        WorkerInfo capableWorker = worker("capable", 8192, 8, false, Set.of("avx512"));
        assertTrue(jobManager.claimNextJob(capableWorker).isPresent());
    }

    @Test
    void testClaimDefaultRequirementsMatchAdequateWorker() {
        Job job = new Job("any-job", "img:latest", null, 0, null, null);
        jobManager.submit("job-any", job);

        WorkerInfo worker = worker("w", 8192, 8, false, Set.of());
        Optional<JobStatus> claimed = jobManager.claimNextJob(worker);

        assertTrue(claimed.isPresent());
    }

    @Test
    void testClaimMemoryInsufficient() {
        Job job = new Job("big-job", "img:latest", null, 0, null,
                new ResourceRequirements(4096, 0, false, Set.of()));
        jobManager.submit("job-big", job);

        WorkerInfo smallWorker = worker("small", 2048, 8, false, Set.of());
        Optional<JobStatus> claimed = jobManager.claimNextJob(smallWorker);

        assertTrue(claimed.isEmpty());
    }

    // -- helpers --

    private static StatusUpdate jobUpdate(String jobId, JobState state, FailureReason reason, String detail) {
        StatusUpdate.Builder builder = StatusUpdate.newBuilder().setJobId(jobId).setJobState(state);
        if (reason != null) {
            builder.setFailureReason(reason);
        }
        if (detail != null) {
            builder.setFailureDetail(detail);
        }
        return builder.build();
    }

    private static StatusUpdate taskUpdate(String jobId, int taskIndex, String taskName,
                                           TaskState state, String error) {
        StatusUpdate.Builder builder = StatusUpdate.newBuilder()
                .setJobId(jobId).setTaskIndex(taskIndex).setTaskName(taskName).setTaskState(state);
        if (error != null) {
            builder.setErrorMessage(error);
        }
        return builder.build();
    }

    private static WorkerInfo worker(String id, int memoryMb, int cpuCores, boolean gpu, Set<String> capabilities) {
        return new WorkerInfo(id, "localhost", memoryMb, cpuCores, gpu, capabilities,
                Instant.now(), Instant.now());
    }

    private static final WorkerInfo DEFAULT_WORKER = worker("worker-1", 8192, 8, false, Set.of());

    private JobStatus submitAndClaim(String name) {
        Job job = new Job(name, "my-job:latest", null, 0, null, null);
        jobManager.submit("job-" + name, job);
        return jobManager.claimNextJob(DEFAULT_WORKER).orElseThrow();
    }
}
