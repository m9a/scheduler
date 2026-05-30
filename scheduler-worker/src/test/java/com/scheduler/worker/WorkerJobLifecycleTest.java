package com.scheduler.worker;

import com.scheduler.coordinator.JobManagerImpl;
import com.scheduler.coordinator.client.UserRequestHandler;
import com.scheduler.coordinator.worker.WorkerHandler;
import com.scheduler.proto.v1.*;
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
        int execute(JobDetails details, String workerAgentUrl) throws IOException, InterruptedException;
    }

    static class TestableWorkerAgent extends WorkerAgent {
        private volatile SpawnBehavior spawnBehavior;

        TestableWorkerAgent(String coordinatorHost, int coordinatorPort) throws IOException {
            super(coordinatorHost, coordinatorPort, "localhost", 1, null, Duration.ofSeconds(10));
        }

        void setSpawnBehavior(SpawnBehavior behavior) {
            this.spawnBehavior = behavior;
        }

        @Override
        int spawnJobProcess(JobDetails details, Path inputDir, Path outputDir, Path logFile,
                            Map<String, String> params) throws IOException, InterruptedException {
            return spawnBehavior.execute(details, workerAgentUrl());
        }
    }

    private Server server;
    private ManagedChannel clientChannel;
    private ClientServiceGrpc.ClientServiceBlockingStub clientStub;
    private TestableWorkerAgent worker;
    private Thread workerThread;

    @BeforeEach
    void setUp() throws Exception {
        JobManagerImpl jobManager = new JobManagerImpl();

        server = ServerBuilder.forPort(0)
                .addService(new UserRequestHandler(jobManager, null))
                .addService(new WorkerHandler(jobManager))
                .build()
                .start();

        clientChannel = ManagedChannelBuilder
                .forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();
        clientStub = ClientServiceGrpc.newBlockingStub(clientChannel);

        worker = new TestableWorkerAgent("localhost", server.getPort());
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
            postTaskStatus(url, details.jobId(), 0, "extract", "RUNNING", null);
            postTaskStatus(url, details.jobId(), 0, "extract", "COMPLETED", null);
            return 0;
        });

        String jobId = submitJob("exit-zero");
        startWorker();

        Job job = pollUntilTerminal(jobId, 10, TimeUnit.SECONDS);
        assertEquals(JobStatus.JOB_STATUS_COMPLETED, job.getStatus());
        assertTrue(job.getStartedAtMillis() > 0, "startedAt should be set");
        assertTrue(job.getCompletedAtMillis() > 0, "completedAt should be set");
    }

    @Test
    void processExitNonZero() throws Exception {
        worker.setSpawnBehavior((details, url) -> {
            postTaskStatus(url, details.jobId(), 0, "extract", "RUNNING", null);
            return 1;
        });

        String jobId = submitJob("exit-nonzero");
        startWorker();

        Job job = pollUntilTerminal(jobId, 10, TimeUnit.SECONDS);
        assertEquals(JobStatus.JOB_STATUS_FAILED, job.getStatus());
        assertEquals(FailureReason.FAILURE_REASON_PROCESS_EXITED, job.getFailureReason());
        assertEquals("exit code 1", job.getFailureDetail());
        assertTrue(job.getErrorMessage().contains("non-zero code"));
    }

    @Test
    void processExitNonZeroNoUpdates() throws Exception {
        worker.setSpawnBehavior((details, url) -> 42);

        String jobId = submitJob("no-updates");
        startWorker();

        Job job = pollUntilTerminal(jobId, 10, TimeUnit.SECONDS);
        assertEquals(JobStatus.JOB_STATUS_FAILED, job.getStatus());
        assertEquals(0, job.getTasksCount());
    }

    @Test
    void processTimeout() throws Exception {
        worker.setSpawnBehavior((details, url) -> -1);

        String jobId = submitJob("timeout");
        startWorker();

        Job job = pollUntilTerminal(jobId, 10, TimeUnit.SECONDS);
        assertEquals(JobStatus.JOB_STATUS_KILLED, job.getStatus());
        assertEquals(FailureReason.FAILURE_REASON_PROCESS_TIMEOUT, job.getFailureReason());
        assertTrue(job.getErrorMessage().contains("timed out"));
        assertEquals(0, job.getTasksCount());
    }

    @Test
    void taskFailed() throws Exception {
        worker.setSpawnBehavior((details, url) -> {
            postTaskStatus(url, details.jobId(), 0, "extract", "RUNNING", null);
            postTaskStatus(url, details.jobId(), 0, "extract", "FAILED", "out of memory");
            return 1;
        });

        String jobId = submitJob("task-fail");
        startWorker();

        Job job = pollUntilTerminal(jobId, 10, TimeUnit.SECONDS);
        assertEquals(JobStatus.JOB_STATUS_FAILED, job.getStatus());
        assertEquals(TaskStatus.TASK_STATUS_FAILED, job.getTasks(0).getStatus());
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
        JobManagerImpl jobManager = new JobManagerImpl();
        WorkerHandler workerHandler = new WorkerHandler(jobManager);
        workerHandler.startHeartbeatMonitor(Duration.ofSeconds(2), Duration.ofMillis(500));

        server = ServerBuilder.forPort(0)
                .addService(new UserRequestHandler(jobManager, null))
                .addService(workerHandler)
                .build()
                .start();

        clientChannel = ManagedChannelBuilder
                .forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();
        clientStub = ClientServiceGrpc.newBlockingStub(clientChannel);

        worker = new TestableWorkerAgent("localhost", server.getPort());

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
            if (resp.getJob().getStatus() != JobStatus.JOB_STATUS_QUEUED) {
                break;
            }
            Thread.sleep(100);
        }

        // Shut down the worker's heartbeat sender without interrupting the spawn thread,
        // simulating a worker that goes silent while a job is in-flight.
        worker.close();

        // The coordinator's heartbeat monitor should detect the dead worker and fail the job
        Job job = pollUntilTerminal(jobId, 10, TimeUnit.SECONDS);
        assertEquals(JobStatus.JOB_STATUS_FAILED, job.getStatus());
        assertEquals(FailureReason.FAILURE_REASON_HEARTBEAT_LOST, job.getFailureReason());
        assertTrue(job.getErrorMessage().contains("heartbeat"),
                "Expected reason to mention heartbeat, got: " + job.getErrorMessage());

        // Unblock the spawn and let the worker thread finish
        spawnBlocked.countDown();
        workerThread.join(5000);

        // Recreate worker so tearDown doesn't NPE
        worker = new TestableWorkerAgent("localhost", server.getPort());
        worker.setSpawnBehavior((details, url) -> 0);
        workerThread = null;

        workerHandler.shutdownHeartbeatMonitor();
    }

    @Test
    void workerContinuesAfterFailure() throws Exception {
        AtomicBoolean first = new AtomicBoolean(true);
        worker.setSpawnBehavior((details, url) -> {
            if (first.compareAndSet(true, false)) {
                return 1;
            }
            postTaskStatus(url, details.jobId(), 0, "extract", "RUNNING", null);
            postTaskStatus(url, details.jobId(), 0, "extract", "COMPLETED", null);
            return 0;
        });

        String jobId1 = submitJob("fail-first");
        String jobId2 = submitJob("succeed-second");
        startWorker();

        Job job1 = pollUntilTerminal(jobId1, 10, TimeUnit.SECONDS);
        Job job2 = pollUntilTerminal(jobId2, 10, TimeUnit.SECONDS);
        assertEquals(JobStatus.JOB_STATUS_FAILED, job1.getStatus());
        assertEquals(JobStatus.JOB_STATUS_COMPLETED, job2.getStatus());
    }

    // -- helpers --

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

    private Job pollUntilTerminal(String jobId, long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        Job job = null;

        while (System.nanoTime() < deadline) {
            GetJobStatusResponse response = clientStub.getJobStatus(
                    GetJobStatusRequest.newBuilder().setJobId(jobId).build());
            job = response.getJob();
            JobStatus status = job.getStatus();

            if (status == JobStatus.JOB_STATUS_COMPLETED || status == JobStatus.JOB_STATUS_FAILED
                    || status == JobStatus.JOB_STATUS_KILLED) {
                return job;
            }
            Thread.sleep(100);
        }

        fail("Job did not reach terminal status within timeout. Last status: " + (job != null ? job.getStatus() : "null"));
        return null;
    }

    private static void postTaskStatus(String workerUrl, String jobId, int taskIndex,
                                        String taskName, String status, String errorMessage) {
        try {
            TaskStatus protoStatus = switch (status) {
                case "RUNNING" -> TaskStatus.TASK_STATUS_RUNNING;
                case "COMPLETED" -> TaskStatus.TASK_STATUS_COMPLETED;
                case "FAILED" -> TaskStatus.TASK_STATUS_FAILED;
                default -> TaskStatus.TASK_STATUS_UNSPECIFIED;
            };

            com.scheduler.proto.job.StatusUpdate.Builder builder =
                    com.scheduler.proto.job.StatusUpdate.newBuilder()
                            .setJobId(jobId)
                            .setTaskIndex(taskIndex)
                            .setTaskName(taskName)
                            .setTaskStatus(protoStatus);
            if (errorMessage != null) {
                builder.setErrorMessage(errorMessage);
            }

            byte[] proto = builder.build().toByteArray();
            byte[] framed = new byte[proto.length + 1];
            framed[0] = WorkerAgent.TYPE_TAG_STATUS;
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
