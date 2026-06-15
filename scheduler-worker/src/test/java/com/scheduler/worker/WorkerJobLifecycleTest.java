package com.scheduler.worker;

import com.scheduler.coordinator.JobManager;
import com.scheduler.coordinator.client.ClientHandler;
import com.scheduler.coordinator.worker.WorkerHandler;
import com.scheduler.proto.v1.*;
import com.scheduler.proto.job.StatusUpdate;
import com.scheduler.proto.client.*;
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
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
        // onTimeout simulates the launcher's deadline hook: call it, then return -1,
        // to behave like a container that hit the execution timeout.
        int execute(JobDetails details, String workerAgentUrl, Runnable onTimeout)
                throws IOException, InterruptedException;
    }

    static class TestableWorkerAgent extends WorkerAgent {
        private volatile SpawnBehavior spawnBehavior;

        TestableWorkerAgent(WorkerConfig config) throws IOException {
            super(config, null, Duration.ofSeconds(10));
        }

        void setSpawnBehavior(SpawnBehavior behavior) {
            this.spawnBehavior = behavior;
        }

        @Override
        int spawnJobProcess(JobDetails details, Path inputDir, Path outputDir, Path logFile,
                            Map<String, String> params, Runnable onTimeout) throws IOException, InterruptedException {
            return spawnBehavior.execute(details, workerAgentUrl(), onTimeout);
        }
    }

    private Server server;
    private ManagedChannel clientChannel;
    private ClientServiceGrpc.ClientServiceBlockingStub clientStub;
    private TestableWorkerAgent worker;
    private Thread workerThread;

    @BeforeEach
    void setUp() throws Exception {
        JobManager jobManager = new JobManager();

        server = ServerBuilder.forPort(0)
                .addService(new ClientHandler(jobManager, null))
                .addService(new WorkerHandler(jobManager))
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
        worker.setSpawnBehavior((details, url, onTimeout) -> {
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
        worker.setSpawnBehavior((details, url, onTimeout) -> {
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
        worker.setSpawnBehavior((details, url, onTimeout) -> 42);

        String jobId = submitJob("no-updates");
        startWorker();

        Job job = pollUntilTerminal(jobId, 10, TimeUnit.SECONDS);
        assertEquals(JobState.JOB_STATE_FAILED, job.getState());
        assertEquals(0, job.getTasksCount());
    }

    @Test
    void processTimeout() throws Exception {
        // Hold the spawn between the deadline hook and the kill confirmation so
        // the test can observe the intermediate TIMEOUT state.
        CountDownLatch killConfirmed = new CountDownLatch(1);
        worker.setSpawnBehavior((details, url, onTimeout) -> {
            onTimeout.run();
            killConfirmed.await();
            return -1;
        });

        String jobId = submitJob("timeout");
        startWorker();

        Job job = pollUntilStatus(jobId, JobState.JOB_STATE_TIMEOUT, 10, TimeUnit.SECONDS);
        assertEquals(FailureReason.FAILURE_REASON_PROCESS_TIMEOUT, job.getFailureReason());

        killConfirmed.countDown();
        job = pollUntilTerminal(jobId, 10, TimeUnit.SECONDS);
        assertEquals(JobState.JOB_STATE_KILLED, job.getState());
        assertEquals(FailureReason.FAILURE_REASON_PROCESS_TIMEOUT, job.getFailureReason());
        assertTrue(job.getErrorMessage().contains("timed out"));
        assertEquals(0, job.getTasksCount());
    }

    @Test
    void taskFailed() throws Exception {
        worker.setSpawnBehavior((details, url, onTimeout) -> {
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

    @Test
    void testWorkerHeartbeatLost() throws Exception {
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
        JobManager jobManager = new JobManager();
        WorkerHandler workerHandler = new WorkerHandler(jobManager);
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
        worker.setSpawnBehavior((details, url, onTimeout) -> {
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
        worker.setSpawnBehavior((details, url, onTimeout) -> 0);
        workerThread = null;

        workerHandler.shutdownHeartbeatMonitor();
    }

    @Test
    void workerContinuesAfterFailure() throws Exception {
        AtomicBoolean first = new AtomicBoolean(true);
        worker.setSpawnBehavior((details, url, onTimeout) -> {
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

    // -- helpers --

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
            framed[0] = JobCallbackServer.TYPE_TAG_STATUS;
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
