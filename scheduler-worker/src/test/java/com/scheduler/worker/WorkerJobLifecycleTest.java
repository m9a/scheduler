package com.scheduler.worker;

import com.scheduler.coordinator.JobManager;
import com.scheduler.coordinator.client.ClientHandler;
import com.scheduler.coordinator.worker.WorkerHandler;
import com.scheduler.proto.v1.*;
import com.scheduler.proto.job.StatusUpdate;
import com.scheduler.proto.client.*;
import com.scheduler.worker.persistence.InMemoryWorkerStatusStore;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * In-process integration test wiring a real coordinator (gRPC on ephemeral port)
 * with a WorkerAgent subclass that overrides spawnJobProcess to simulate
 * container behavior without Docker. Tests cover failure paths: crashes,
 * timeouts, task failures, and worker recovery.
 */
class WorkerJobLifecycleTest {

    @FunctionalInterface
    interface SpawnBehavior {
        int execute(JobDetails details, String workerAgentUrl)
                throws IOException, InterruptedException;
    }

    @FunctionalInterface
    interface AttachBehavior {
        int execute(String jobId) throws IOException, InterruptedException;
    }

    static class TestableWorkerAgent extends WorkerAgent {
        final InMemoryWorkerStatusStore statusStore;
        private volatile SpawnBehavior spawnBehavior;
        private volatile AttachBehavior attachBehavior;
        // When set, boot recovery sees this instead of asking docker.
        volatile JobLauncher.ContainerState containerStateStub;

        TestableWorkerAgent(WorkerConfig config) throws IOException {
            this(config, new InMemoryWorkerStatusStore());
        }

        TestableWorkerAgent(WorkerConfig config, InMemoryWorkerStatusStore store) throws IOException {
            super(config, null, store);
            this.statusStore = store;
        }

        void setSpawnBehavior(SpawnBehavior behavior) {
            this.spawnBehavior = behavior;
        }

        void setAttachBehavior(AttachBehavior behavior) {
            this.attachBehavior = behavior;
        }

        @Override
        int spawnJobProcess(JobDetails details, Path inputDir, Path outputDir, Path logFile,
                            Map<String, String> params) throws IOException, InterruptedException {
            return spawnBehavior.execute(details, workerAgentUrl());
        }

        @Override
        int attachJobProcess(String jobId, Path logFile) throws IOException, InterruptedException {
            return attachBehavior.execute(jobId);
        }

        @Override
        public JobLauncher.ContainerState containerState(String jobId) {
            return containerStateStub != null ? containerStateStub : super.containerState(jobId);
        }
    }

    private Server server;
    private ManagedChannel clientChannel;
    private ClientServiceGrpc.ClientServiceBlockingStub clientStub;
    private JobManager jobManager;
    private WorkerHandler workerHandler;
    private TestableWorkerAgent worker;
    private Thread workerThread;

    @BeforeEach
    void setUp() throws Exception {
        jobManager = TestJobManager.create();
        workerHandler = TestJobManager.workerHandler(jobManager);

        server = ServerBuilder.forPort(0)
                .addService(new ClientHandler(jobManager, null))
                .addService(workerHandler)
                .build()
                .start();

        clientChannel = ManagedChannelBuilder
                .forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();
        clientStub = ClientServiceGrpc.newBlockingStub(clientChannel);

        worker = new TestableWorkerAgent(testConfig(server.getPort()));
    }

    @AfterEach
    void tearDown() throws Exception {
        worker.stop();
        if (workerThread != null) {
            workerThread.interrupt();
            workerThread.join(5000);
        }
        worker.close();
        clientChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    void processExitZero() throws Exception {
        worker.setSpawnBehavior((details, url) -> {
            postTaskState(url, details.jobId(), 0, "extract", "RUNNING", null);
            postTaskState(url, details.jobId(), 0, "extract", "COMPLETED", null);
            return 0;
        });

        String jobId = submitJob("exit-zero");
        startWorker();

        Job job = pollUntilTerminal(jobId, 10, TimeUnit.SECONDS);
        assertEquals(JobState.JOB_STATE_COMPLETED, job.getState());
        assertTrue(job.getStartedAtMillis() > 0, "startedAt should be set");
        assertTrue(job.getCompletedAtMillis() > 0, "completedAt should be set");
    }

    @Test
    void processExitNonZero() throws Exception {
        worker.setSpawnBehavior((details, url) -> {
            postTaskState(url, details.jobId(), 0, "extract", "RUNNING", null);
            return 1;
        });

        String jobId = submitJob("exit-nonzero");
        startWorker();

        Job job = pollUntilTerminal(jobId, 10, TimeUnit.SECONDS);
        assertEquals(JobState.JOB_STATE_FAILED, job.getState());
        assertEquals(FailureReason.FAILURE_REASON_PROCESS_EXITED, job.getFailureReason());
        assertEquals("exit code 1", job.getFailureDetail());
        assertTrue(job.getErrorMessage().contains("non-zero code"));
        // The container died mid-task; the task keeps its last reported state
        // (RUNNING) — only the job goes FAILED.
        assertEquals(1, job.getTasksCount());
        assertEquals(TaskState.TASK_STATE_RUNNING, job.getTasks(0).getState());
    }

    @Test
    void processExitNonZeroNoUpdates() throws Exception {
        worker.setSpawnBehavior((details, url) -> 42);

        String jobId = submitJob("no-updates");
        startWorker();

        Job job = pollUntilTerminal(jobId, 10, TimeUnit.SECONDS);
        assertEquals(JobState.JOB_STATE_FAILED, job.getState());
        assertEquals(0, job.getTasksCount());
    }

    @Test
    void taskFailed() throws Exception {
        worker.setSpawnBehavior((details, url) -> {
            postTaskState(url, details.jobId(), 0, "extract", "RUNNING", null);
            postTaskState(url, details.jobId(), 0, "extract", "FAILED", "out of memory");
            return 1;
        });

        String jobId = submitJob("task-fail");
        startWorker();

        Job job = pollUntilTerminal(jobId, 10, TimeUnit.SECONDS);
        assertEquals(JobState.JOB_STATE_FAILED, job.getState());
        assertEquals(TaskState.TASK_STATE_FAILED, job.getTasks(0).getState());
    }

    // A task reports FAILED but the container still exits 0 — the job must be
    // FAILED, not COMPLETED. Any task failure fails the whole job.
    @Test
    void taskFailedExitZero() throws Exception {
        worker.setSpawnBehavior((details, url) -> {
            postTaskState(url, details.jobId(), 0, "extract", "RUNNING", null);
            postTaskState(url, details.jobId(), 0, "extract", "FAILED", "boom");
            return 0;  // container exits clean despite the failed task
        });

        String jobId = submitJob("task-fail-exit-zero");
        startWorker();

        Job job = pollUntilTerminal(jobId, 10, TimeUnit.SECONDS);
        assertEquals(JobState.JOB_STATE_FAILED, job.getState());
        assertEquals(FailureReason.FAILURE_REASON_PROCESS_EXITED, job.getFailureReason());
        assertEquals(TaskState.TASK_STATE_FAILED, job.getTasks(0).getState());
    }

    // A worker goes silent mid-job; the coordinator's heartbeat monitor must
    // detect it and fail the job as HEARTBEAT_LOST.
    @Test
    void heartbeatLost() throws Exception {
        // Tear down the default setup — we need a coordinator with the heartbeat monitor enabled
        worker.stop();
        if (workerThread != null) {
            workerThread.interrupt();
            workerThread.join(5000);
        }
        worker.close();
        clientChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        server.shutdown().awaitTermination(5, TimeUnit.SECONDS);

        // Stand up a coordinator with aggressive heartbeat settings (2s timeout, 500ms scan)
        JobManager jobManager = TestJobManager.create();
        WorkerHandler workerHandler = TestJobManager.workerHandler(jobManager);
        workerHandler.startHeartbeatMonitor(Duration.ofSeconds(2), Duration.ofMillis(500));

        server = ServerBuilder.forPort(0)
                .addService(new ClientHandler(jobManager, null))
                .addService(workerHandler)
                .build()
                .start();

        clientChannel = ManagedChannelBuilder
                .forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();
        clientStub = ClientServiceGrpc.newBlockingStub(clientChannel);

        worker = new TestableWorkerAgent(testConfig(server.getPort()));

        // Block the spawn so the job stays in-flight while we kill the worker
        CountDownLatch spawnBlocked = new CountDownLatch(1);
        worker.setSpawnBehavior((details, url) -> {
            spawnBlocked.await();
            return 0;
        });

        String jobId = submitJob("heartbeat-test");
        startWorker();

        // Wait for the job to be claimed (status becomes STARTING)
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            GetJobStatusResponse resp = clientStub.getJobStatus(
                    GetJobStatusRequest.newBuilder().setJobId(jobId).build());
            if (resp.getJob().getState() != JobState.JOB_STATE_QUEUED) {
                break;
            }
            Thread.sleep(100);
        }

        // Shut down the worker's heartbeat sender without interrupting the spawn thread,
        // simulating a worker that goes silent while a job is in-flight.
        worker.close();

        // The coordinator's heartbeat monitor should detect the dead worker and fail the job
        Job job = pollUntilTerminal(jobId, 10, TimeUnit.SECONDS);
        assertEquals(JobState.JOB_STATE_FAILED, job.getState());
        assertEquals(FailureReason.FAILURE_REASON_HEARTBEAT_LOST, job.getFailureReason());
        assertTrue(job.getErrorMessage().contains("heartbeat"),
                "Expected reason to mention heartbeat, got: " + job.getErrorMessage());

        // Unblock the spawn and let the worker thread finish
        spawnBlocked.countDown();
        workerThread.join(5000);

        // Recreate worker so tearDown doesn't NPE
        worker = new TestableWorkerAgent(testConfig(server.getPort()));
        worker.setSpawnBehavior((details, url) -> 0);
        workerThread = null;

        workerHandler.shutdownHeartbeatMonitor();
    }

    // Durable status store: job + task rows written while the job is in flight,
    // all rows dropped once the coordinator acks the terminal delivery.
    @Test
    void statusStoreAck() throws Exception {
        CountDownLatch midJob = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        worker.setSpawnBehavior((details, url) -> {
            postTaskState(url, details.jobId(), 0, "extract", "RUNNING", null);
            midJob.countDown();
            release.await();
            postTaskState(url, details.jobId(), 0, "extract", "COMPLETED", null);
            return 0;
        });

        String jobId = submitJob("store-write-through");
        startWorker();

        assertTrue(midJob.await(10, TimeUnit.SECONDS), "job should reach mid-flight");
        awaitTrue(() -> worker.statusStore.loadAllJobs().size() == 2, 5, TimeUnit.SECONDS,
                "job entry + task entry should be persisted mid-flight");
        List<StatusUpdate> rows = worker.statusStore.loadAllJobs();
        // Task entry first (RUNNING, job stamped RUNNING), then the job entry (STARTING at claim).
        assertEquals(TaskState.TASK_STATE_RUNNING, rows.get(0).getTaskState());
        assertEquals(JobState.JOB_STATE_RUNNING, rows.get(0).getJobState());
        assertEquals(JobState.JOB_STATE_STARTING, rows.get(1).getJobState());

        release.countDown();
        Job job = pollUntilTerminal(jobId, 10, TimeUnit.SECONDS);
        assertEquals(JobState.JOB_STATE_COMPLETED, job.getState());
        awaitTrue(() -> worker.statusStore.loadAllJobs().isEmpty(), 5, TimeUnit.SECONDS,
                "coordinator's close ack should drop the job's rows");
    }

    // A failed job must not wedge the worker: the next job still runs.
    @Test
    void nextJobAfterFailure() throws Exception {
        AtomicBoolean first = new AtomicBoolean(true);
        worker.setSpawnBehavior((details, url) -> {
            if (first.compareAndSet(true, false)) {
                return 1;
            }
            postTaskState(url, details.jobId(), 0, "extract", "RUNNING", null);
            postTaskState(url, details.jobId(), 0, "extract", "COMPLETED", null);
            return 0;
        });

        String jobId1 = submitJob("fail-first");
        String jobId2 = submitJob("succeed-second");
        startWorker();

        Job job1 = pollUntilTerminal(jobId1, 10, TimeUnit.SECONDS);
        Job job2 = pollUntilTerminal(jobId2, 10, TimeUnit.SECONDS);
        assertEquals(JobState.JOB_STATE_FAILED, job1.getState());
        assertEquals(JobState.JOB_STATE_COMPLETED, job2.getState());
    }

    // Coordinator → worker push path end-to-end: the worker subscribes (so a push
    // is deliverable), and a drain command actually reaches and changes it.
    @Test
    void drainCommand() throws Exception {
        worker.setSpawnBehavior((details, url) -> 0);  // no job submitted; worker stays idle
        startWorker();

        String workerId = awaitWorkerId();
        // Probe with a no-op drain(false) until it reports deliverable — that's the
        // signal the worker has opened the system command stream.
        awaitTrue(() -> workerHandler.drain(workerId, false), 5, TimeUnit.SECONDS,
                "worker should subscribe to the system command stream");

        assertTrue(workerHandler.drain(workerId, true), "drain push should be deliverable");
        awaitTrue(worker::isDraining, 5, TimeUnit.SECONDS, "drain command should reach the worker");
    }

    // A worker dies mid-job; the job's rows stay in the store and the container keeps
    // running. The restarted worker must re-attach, resume the wait, and report the
    // terminal state; the coordinator's ack then drops the rows.
    @Test
    void reattach() throws Exception {
        CountDownLatch claimed = new CountDownLatch(1);
        CountDownLatch blockForever = new CountDownLatch(1);
        worker.setSpawnBehavior((details, url) -> {
            claimed.countDown();
            blockForever.await();
            return 0;
        });

        String jobId = submitJob("reattach");
        startWorker();
        assertTrue(claimed.await(10, TimeUnit.SECONDS), "job should be claimed");
        awaitTrue(() -> !worker.statusStore.loadAllJobs().isEmpty(), 5, TimeUnit.SECONDS,
                "job entry should be persisted before the crash");

        // "Crash": close the worker. Its spawn thread stays blocked (daemon); the
        // store — standing in for the sqlite file — keeps the job's rows.
        InMemoryWorkerStatusStore store = worker.statusStore;
        worker.close();

        worker = new TestableWorkerAgent(testConfig(server.getPort()), store);
        worker.containerStateStub = JobLauncher.ContainerState.RUNNING;
        worker.setSpawnBehavior((details, url) -> 0);
        CountDownLatch reattached = new CountDownLatch(1);
        worker.setAttachBehavior(jid -> {
            reattached.countDown();
            return 0;
        });
        startWorker();

        assertTrue(reattached.await(10, TimeUnit.SECONDS), "restarted worker should re-attach");
        Job job = pollUntilTerminal(jobId, 10, TimeUnit.SECONDS);
        assertEquals(JobState.JOB_STATE_COMPLETED, job.getState());
        awaitTrue(() -> store.loadAllJobs().isEmpty(), 5, TimeUnit.SECONDS,
                "ack of the re-attached terminal should drop the job's rows");
    }

    // A worker dies mid-job and the container is gone on restart. Best-effort
    // recovery: the job is FAILED / NOT_FOUND_ON_RECOVERY and the rows are acked.
    @Test
    void absentOnRecovery() throws Exception {
        Job job = restartWithContainerState(JobLauncher.ContainerState.ABSENT);
        assertEquals(JobState.JOB_STATE_FAILED, job.getState());
        assertEquals(FailureReason.FAILURE_REASON_NOT_FOUND_ON_RECOVERY, job.getFailureReason());
        assertEquals("container absent on worker restart", job.getFailureDetail());
    }

    // Same crash, but the container finished while the worker was down. Still the
    // coarse best-effort FAILED — the user reads the checkpoint to judge progress.
    @Test
    void exitedOnRecovery() throws Exception {
        Job job = restartWithContainerState(JobLauncher.ContainerState.EXITED);
        assertEquals(JobState.JOB_STATE_FAILED, job.getState());
        assertEquals(FailureReason.FAILURE_REASON_NOT_FOUND_ON_RECOVERY, job.getFailureReason());
        assertEquals("container exited on worker restart", job.getFailureDetail());
    }

    // Worker dies mid-job for longer than the heartbeat timeout, so the coordinator
    // already failed the job (HEARTBEAT_LOST). The restarted worker must discard it:
    // container stopped, no re-attach, coordinator verdict untouched, rows dropped.
    @Test
    void deadOnRecovery() throws Exception {
        CountDownLatch claimed = new CountDownLatch(1);
        CountDownLatch blockForever = new CountDownLatch(1);
        worker.setSpawnBehavior((details, url) -> {
            claimed.countDown();
            blockForever.await();
            return 0;
        });

        String jobId = submitJob("dead-on-recovery");
        startWorker();
        assertTrue(claimed.await(10, TimeUnit.SECONDS), "job should be claimed");
        awaitTrue(() -> !worker.statusStore.loadAllJobs().isEmpty(), 5, TimeUnit.SECONDS,
                "job entry should be persisted before the crash");
        String workerId = awaitWorkerId();

        InMemoryWorkerStatusStore store = worker.statusStore;
        worker.close();
        // The heartbeat monitor's action, applied directly: the worker is gone too long.
        jobManager.failJobsForWorker(workerId, FailureReason.FAILURE_REASON_HEARTBEAT_LOST);

        worker = new TestableWorkerAgent(testConfig(server.getPort()), store);
        // The container survived the worker — but the job is dead on the coordinator,
        // so it must be discarded, not re-attached (no attach behavior is set: a
        // re-attach attempt would NPE and fail the test).
        worker.containerStateStub = JobLauncher.ContainerState.RUNNING;
        worker.setSpawnBehavior((details, url) -> 0);
        startWorker();

        awaitTrue(() -> store.loadAllJobs().isEmpty(), 5, TimeUnit.SECONDS,
                "close ack should drop the dead job's rows");
        Job job = pollUntilTerminal(jobId, 5, TimeUnit.SECONDS);
        assertEquals(JobState.JOB_STATE_FAILED, job.getState());
        assertEquals(FailureReason.FAILURE_REASON_HEARTBEAT_LOST, job.getFailureReason(),
                "the coordinator's verdict must not be overwritten by the discard report");
    }

    /**
     * Shared crash-and-restart scaffold: worker 1 claims a job and dies mid-run;
     * worker 2 boots on the same store with the stubbed container state. Returns
     * the job's terminal view from the coordinator after recovery, asserting the
     * store was acked empty.
     */
    private Job restartWithContainerState(JobLauncher.ContainerState state) throws Exception {
        CountDownLatch claimed = new CountDownLatch(1);
        CountDownLatch blockForever = new CountDownLatch(1);
        worker.setSpawnBehavior((details, url) -> {
            claimed.countDown();
            blockForever.await();
            return 0;
        });

        String jobId = submitJob("recovery-" + state);
        startWorker();
        assertTrue(claimed.await(10, TimeUnit.SECONDS), "job should be claimed");
        awaitTrue(() -> !worker.statusStore.loadAllJobs().isEmpty(), 5, TimeUnit.SECONDS,
                "job entry should be persisted before the crash");

        InMemoryWorkerStatusStore store = worker.statusStore;
        worker.close();

        worker = new TestableWorkerAgent(testConfig(server.getPort()), store);
        worker.containerStateStub = state;
        worker.setSpawnBehavior((details, url) -> 0);
        startWorker();

        Job job = pollUntilTerminal(jobId, 10, TimeUnit.SECONDS);
        awaitTrue(() -> store.loadAllJobs().isEmpty(), 5, TimeUnit.SECONDS,
                "ack of the recovery terminal should drop the job's rows");
        return job;
    }

    // -- helpers --

    private String awaitWorkerId() throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            String id = worker.workerId();
            if (id != null) {
                return id;
            }
            Thread.sleep(50);
        }
        fail("worker did not register within timeout");
        return null;
    }

    private void awaitTrue(BooleanSupplier condition, long timeout, TimeUnit unit, String message)
            throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        fail(message);
    }

    private static WorkerConfig testConfig(int coordinatorPort) throws IOException {
        Path configPath = Path.of(WorkerJobLifecycleTest.class.getClassLoader()
                .getResource("config.yaml").getPath());
        WorkerConfig config = WorkerConfig.load(configPath);
        config.getCoordinator().setPort(coordinatorPort);
        return config;
    }

    private String submitJob(String name) {
        SubmitJobResponse response = clientStub.submitJob(SubmitJobRequest.newBuilder()
                .setName(name)
                .setArtifactUri("test-image:latest")
                .build());
        return response.getJob().getId();
    }

    private void startWorker() {
        workerThread = new Thread(worker::run);
        workerThread.setDaemon(true);
        workerThread.start();
    }

    /** Polls until the job reaches the given (possibly transient) status — for observing TIMEOUT. */
    private Job pollUntilStatus(String jobId, JobState expected, long timeout, TimeUnit unit)
            throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        Job job = null;
        while (System.nanoTime() < deadline) {
            job = clientStub.getJobStatus(
                    GetJobStatusRequest.newBuilder().setJobId(jobId).build()).getJob();
            if (job.getState() == expected) {
                return job;
            }
            Thread.sleep(50);
        }
        fail("Job did not reach " + expected + " within timeout. Last status: "
                + (job != null ? job.getState() : "null"));
        return null;
    }

    private Job pollUntilTerminal(String jobId, long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        Job job = null;

        while (System.nanoTime() < deadline) {
            GetJobStatusResponse response = clientStub.getJobStatus(
                    GetJobStatusRequest.newBuilder().setJobId(jobId).build());
            job = response.getJob();
            JobState state = job.getState();

            if (state == JobState.JOB_STATE_COMPLETED || state == JobState.JOB_STATE_FAILED
                    || state == JobState.JOB_STATE_KILLED) {
                return job;
            }
            Thread.sleep(100);
        }

        fail("Job did not reach terminal status within timeout. Last status: " + (job != null ? job.getState() : "null"));
        return null;
    }

    private static void postTaskState(String workerUrl, String jobId, int taskIndex,
                                        String taskName, String status, String errorMessage) {
        try {
            TaskState protoStatus = switch (status) {
                case "RUNNING" -> TaskState.TASK_STATE_RUNNING;
                case "COMPLETED" -> TaskState.TASK_STATE_COMPLETED;
                case "FAILED" -> TaskState.TASK_STATE_FAILED;
                default -> TaskState.TASK_STATE_UNSPECIFIED;
            };

            StatusUpdate.Builder builder =
                    StatusUpdate.newBuilder()
                            .setJobId(jobId)
                            .setTaskIndex(taskIndex)
                            .setTaskName(taskName)
                            .setTaskState(protoStatus);
            if (errorMessage != null) {
                builder.setErrorMessage(errorMessage);
            }

            byte[] proto = builder.build().toByteArray();
            byte[] framed = new byte[proto.length + 1];
            framed[0] = JobCallbackHandler.TYPE_TAG_STATUS;
            System.arraycopy(proto, 0, framed, 1, proto.length);

            WebSocket ws = HttpClient.newHttpClient().newWebSocketBuilder()
                    .buildAsync(URI.create(workerUrl), new WebSocket.Listener() {})
                    .join();
            ws.sendBinary(ByteBuffer.wrap(framed), true).join();
            // Brief pause to let the server process the message before closing
            Thread.sleep(50);
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to send task status", e);
        }
    }
}
