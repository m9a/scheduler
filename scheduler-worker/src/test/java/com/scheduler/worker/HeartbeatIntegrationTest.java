package com.scheduler.worker;

import com.scheduler.client.SchedulerClient;
import com.scheduler.coordinator.JobManager;
import com.scheduler.coordinator.client.ClientHandler;
import com.scheduler.coordinator.worker.WorkerHandler;
import com.scheduler.proto.v1.FailureReason;
import com.scheduler.proto.v1.Job;
import com.scheduler.proto.v1.JobState;
import com.scheduler.worker.persistence.InMemoryWorkerStatusStore;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Failure-detection tests for the two liveness paths — worker heartbeat loss and
 * job stall. Deliberately <b>infra-free</b>: an in-process coordinator and a
 * {@link TestableWorkerAgent} whose spawn is overridden, so there is no Docker
 * container, MinIO, registry, or compose stack. (They live here, not in
 * {@code IntegrationTest}, precisely so they don't inherit its Docker
 * {@code @BeforeAll}.) The job-stall case does shell out to {@code docker stop}
 * for a non-existent container on kill, which fails harmlessly if a Docker daemon
 * is present and is a no-op otherwise.
 */
class HeartbeatIntegrationTest {

    /**
     * Worker dies → coordinator's heartbeat monitor detects it → job marked FAILED
     * with HEARTBEAT_LOST → client sees the failure via GetJobStatus.
     */
    @Test
    void testWorkerHeartbeatLost() throws Exception {
        // Coordinator with aggressive heartbeat settings (2s timeout, 500ms scan).
        JobManager jobManager = TestJobManager.create();
        WorkerHandler workerHandler = TestJobManager.workerHandler(jobManager);
        workerHandler.startHeartbeatMonitor(Duration.ofSeconds(2), Duration.ofMillis(500));

        Server server = ServerBuilder.forPort(0)
                .addService(new ClientHandler(jobManager, null))
                .addService(workerHandler)
                .build()
                .start();

        SchedulerClient client = SchedulerClient.builder()
                .host("localhost")
                .port(server.getPort())
                .deadline(Duration.ofSeconds(30))
                .maxRetries(0)
                .build();

        // Worker that blocks the spawn so the job stays in-flight while we kill heartbeats.
        TestableWorkerAgent worker = new TestableWorkerAgent(testConfig(server.getPort()));
        CountDownLatch spawnBlocked = new CountDownLatch(1);
        worker.setSpawnBehavior((details, url) -> {
            spawnBlocked.await();
            return 0;
        });

        Thread workerThread = null;
        try {
            Job submitted = client.submitJob("heartbeat-loss-test", "test-image:latest", Map.of());
            String jobId = submitted.getId();

            workerThread = new Thread(worker::run);
            workerThread.setDaemon(true);
            workerThread.start();

            // Wait for the job to be claimed (no longer QUEUED).
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < deadline) {
                if (client.getJobStatus(jobId).getState() != JobState.JOB_STATE_QUEUED) {
                    break;
                }
                Thread.sleep(100);
            }

            // Kill the worker — heartbeats stop while spawn is still blocked.
            worker.close();

            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            Job job = null;
            while (System.nanoTime() < deadline) {
                job = client.getJobStatus(jobId);
                if (job.getState() == JobState.JOB_STATE_FAILED) {
                    break;
                }
                Thread.sleep(100);
            }

            assertNotNull(job);
            assertEquals(JobState.JOB_STATE_FAILED, job.getState());
            assertEquals(FailureReason.FAILURE_REASON_HEARTBEAT_LOST, job.getFailureReason());
            assertTrue(job.getErrorMessage().contains("heartbeat"),
                    "Expected error_message to mention heartbeat, got: " + job.getErrorMessage());
            assertTrue(job.getCompletedAtMillis() > 0, "Expected completedAt to be set");

            spawnBlocked.countDown();
            workerThread.join(5000);
        } finally {
            workerHandler.shutdownHeartbeatMonitor();
            client.close();
            server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /**
     * Job goes silent → the worker's JobLivenessMonitor flags it unresponsive →
     * the worker kills it and reports KILLED / UNRESPONSIVE → client sees it. The
     * job-side counterpart to {@link #testWorkerHeartbeatLost}.
     */
    @Test
    void testJobStallUnresponsive() throws Exception {
        JobManager jobManager = TestJobManager.create();
        WorkerHandler workerHandler = TestJobManager.workerHandler(jobManager);
        Server server = ServerBuilder.forPort(0)
                .addService(new ClientHandler(jobManager, null))
                .addService(workerHandler)
                .build()
                .start();

        SchedulerClient client = SchedulerClient.builder()
                .host("localhost")
                .port(server.getPort())
                .deadline(Duration.ofSeconds(30))
                .maxRetries(0)
                .build();

        // Aggressive liveness: 1s startup grace, 1s tick, 1 missed ping → unresponsive.
        WorkerConfig config = testConfig(server.getPort());
        config.getLiveness().setStartupTimeoutSeconds(1);
        config.getLiveness().setPingIntervalSeconds(1);
        config.getLiveness().setMaxMissedPings(1);
        config.getLiveness().setAutoKill(true);
        config.getLiveness().setShutdownGraceSeconds(1);
        TestableWorkerAgent worker = new TestableWorkerAgent(config);

        // Silent "container": runs a few seconds without ever sending activity, long
        // enough for the monitor to flag it. Exit 0 — unresponsiveness takes precedence.
        worker.setSpawnBehavior((details, url) -> {
            Thread.sleep(3000);
            return 0;
        });

        Thread workerThread = null;
        try {
            Job submitted = client.submitJob("job-stall-test", "test-image:latest", Map.of());
            String jobId = submitted.getId();

            workerThread = new Thread(worker::run);
            workerThread.setDaemon(true);
            workerThread.start();

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            Job job = null;
            while (System.nanoTime() < deadline) {
                job = client.getJobStatus(jobId);
                if (job.getState() == JobState.JOB_STATE_KILLED) {
                    break;
                }
                Thread.sleep(100);
            }

            assertNotNull(job);
            assertEquals(JobState.JOB_STATE_KILLED, job.getState());
            assertEquals(FailureReason.FAILURE_REASON_UNRESPONSIVE, job.getFailureReason());
        } finally {
            worker.close();
            if (workerThread != null) {
                workerThread.join(5000);
            }
            client.close();
            server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static WorkerConfig testConfig(int coordinatorPort) throws IOException {
        Path configPath = Path.of(HeartbeatIntegrationTest.class.getClassLoader()
                .getResource("config.yaml").getPath());
        WorkerConfig config = WorkerConfig.load(configPath);
        config.getCoordinator().setPort(coordinatorPort);
        return config;
    }

    @FunctionalInterface
    interface SpawnBehavior {
        int execute(JobDetails details, String workerAgentUrl)
                throws IOException, InterruptedException;
    }

    /** WorkerAgent whose spawn is test-controlled instead of launching a container. */
    static class TestableWorkerAgent extends WorkerAgent {
        private volatile SpawnBehavior spawnBehavior;

        TestableWorkerAgent(WorkerConfig config) throws IOException {
            super(config, null, new InMemoryWorkerStatusStore());
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
}
