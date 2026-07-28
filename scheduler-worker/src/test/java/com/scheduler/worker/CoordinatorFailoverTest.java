package com.scheduler.worker;

import com.scheduler.client.SchedulerClient;
import com.scheduler.coordinator.JobManager;
import com.scheduler.coordinator.client.ClientHandler;
import com.scheduler.coordinator.worker.WorkerHandler;
import com.scheduler.proto.job.StatusUpdate;
import com.scheduler.proto.v1.Job;
import com.scheduler.proto.v1.JobState;
import com.scheduler.proto.v1.TaskState;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Coordinator restart while the worker stays up. The worker detects the dropped
 * command streams, and when the new coordinator (same store, recovered) is back,
 * re-registers with reconciliation. Infra-free: in-process coordinator, stubbed
 * container (see {@link HeartbeatIntegrationTest.TestableWorkerAgent}).
 */
class CoordinatorFailoverTest {

    // Job finishes while the coordinator is down: the terminal report gets no
    // ack, so the rows stay. The restarted coordinator (same job store) learns
    // the outcome from the replayed rows at re-register and acks them.
    @Test
    void testCoordinatorRestartMidJob() throws Exception {
        Path jobDb = Files.createTempFile("coord-failover", ".db");
        JobManager jobManager = TestJobManager.create(jobDb);
        WorkerHandler workerHandler = TestJobManager.workerHandler(jobManager);
        Server server = ServerBuilder.forPort(0)
                .addService(new ClientHandler(jobManager, null))
                .addService(workerHandler)
                .build()
                .start();
        int port = server.getPort();

        HeartbeatIntegrationTest.TestableWorkerAgent worker =
                new HeartbeatIntegrationTest.TestableWorkerAgent(testConfig(port));
        CountDownLatch claimed = new CountDownLatch(1);
        CountDownLatch finishJob = new CountDownLatch(1);
        worker.setSpawnBehavior((details, url) -> {
            // Task states reported while the first coordinator is still up —
            // they land in the worker's store and its live view.
            postTaskState(url, details.jobId(), "train", TaskState.TASK_STATE_RUNNING);
            postTaskState(url, details.jobId(), "train", TaskState.TASK_STATE_COMPLETED);
            claimed.countDown();
            finishJob.await();
            return 0;
        });

        Server restarted = null;
        SchedulerClient client = null;
        Thread workerThread = new Thread(worker::run);
        workerThread.setDaemon(true);
        try {
            SchedulerClient submitClient = SchedulerClient.builder()
                    .host("localhost").port(port)
                    .deadline(Duration.ofSeconds(30)).maxRetries(0).build();
            String jobId = submitClient.submitJob("coordinator-failover", "test-image:latest", Map.of()).getId();
            workerThread.start();
            assertTrue(claimed.await(10, TimeUnit.SECONDS), "job should be claimed");
            submitClient.close();

            // Coordinator "crashes" — shutdownNow, not graceful: a graceful stop
            // would let in-flight streams (and the terminal ack) drain first.
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);

            // The job finishes while the coordinator is down: terminal report
            // gets no ack, rows stay in the store.
            finishJob.countDown();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (System.nanoTime() < deadline
                    && worker.statusStore.loadAllJobs().isEmpty()) {
                Thread.sleep(100);
            }
            assertFalse(worker.statusStore.loadAllJobs().isEmpty(),
                    "rows must survive while the coordinator is down");

            // Coordinator restarts on the same port, recovering from the same
            // job store — the standard boot path.
            JobManager recovered = TestJobManager.create(jobDb);
            recovered.recover();
            restarted = ServerBuilder.forPort(port)
                    .addService(new ClientHandler(recovered, null))
                    .addService(TestJobManager.workerHandler(recovered))
                    .build()
                    .start();

            // The worker's resubscribe loop re-registers with reconciliation:
            // the replayed rows deliver the outcome, the ack drops them.
            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (System.nanoTime() < deadline && !worker.statusStore.loadAllJobs().isEmpty()) {
                Thread.sleep(200);
            }
            assertTrue(worker.statusStore.loadAllJobs().isEmpty(),
                    "reconciliation ack should drop the finished job's rows");

            client = SchedulerClient.builder()
                    .host("localhost").port(port)
                    .deadline(Duration.ofSeconds(30)).maxRetries(0).build();
            Job job = client.getJobStatus(jobId);
            assertEquals(JobState.JOB_STATE_COMPLETED, job.getState(),
                    "the restarted coordinator must learn the real outcome, not fail the job");
            assertEquals(TaskState.TASK_STATE_COMPLETED, job.getTasks(0).getState());
        } finally {
            worker.close();
            workerThread.join(5000);
            if (client != null) {
                client.close();
            }
            if (restarted != null) {
                restarted.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            }
        }
    }

    // Job outlives the restart: after the worker reconnects, a task update must
    // reach the new coordinator live (through the re-opened streams), and the
    // terminal ack must clear the store — no rows left waiting for a next trigger.
    @Test
    void testStreamsReopenAfterCoordinatorRestart() throws Exception {
        Path jobDb = Files.createTempFile("coord-failover-streams", ".db");
        JobManager jobManager = TestJobManager.create(jobDb);
        WorkerHandler workerHandler = TestJobManager.workerHandler(jobManager);
        Server server = ServerBuilder.forPort(0)
                .addService(new ClientHandler(jobManager, null))
                .addService(workerHandler)
                .build()
                .start();
        int port = server.getPort();

        HeartbeatIntegrationTest.TestableWorkerAgent worker =
                new HeartbeatIntegrationTest.TestableWorkerAgent(testConfig(port));
        CountDownLatch claimed = new CountDownLatch(1);
        CountDownLatch finishJob = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicReference<String> workerUrl = new java.util.concurrent.atomic.AtomicReference<>();
        worker.setSpawnBehavior((details, url) -> {
            workerUrl.set(url);
            postTaskState(url, details.jobId(), "train", TaskState.TASK_STATE_RUNNING);
            claimed.countDown();
            finishJob.await();
            return 0;
        });

        Server restarted = null;
        SchedulerClient client = null;
        Thread workerThread = new Thread(worker::run);
        workerThread.setDaemon(true);
        try {
            SchedulerClient submitClient = SchedulerClient.builder()
                    .host("localhost").port(port)
                    .deadline(Duration.ofSeconds(30)).maxRetries(0).build();
            String jobId = submitClient.submitJob("failover-streams", "test-image:latest", Map.of()).getId();
            workerThread.start();
            assertTrue(claimed.await(10, TimeUnit.SECONDS), "job should be claimed");
            String workerId = worker.workerId();
            submitClient.close();

            // Coordinator crashes mid-job; the job keeps running.
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);

            JobManager recovered = TestJobManager.create(jobDb);
            recovered.recover();
            WorkerHandler restartedHandler = TestJobManager.workerHandler(recovered);
            restarted = ServerBuilder.forPort(port)
                    .addService(new ClientHandler(recovered, null))
                    .addService(restartedHandler)
                    .build()
                    .start();

            // Reconnected = the command stream is deliverable again (the retry
            // task re-registers before resubscribing, so this implies both).
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (System.nanoTime() < deadline && !restartedHandler.drain(workerId, false)) {
                Thread.sleep(200);
            }
            assertTrue(restartedHandler.drain(workerId, false), "worker should reconnect to the new coordinator");

            // A task update after the reconnect must arrive live, mid-job —
            // proof the per-job streams were re-opened, not just the register.
            postTaskState(workerUrl.get(), jobId, "train", TaskState.TASK_STATE_COMPLETED);
            client = SchedulerClient.builder()
                    .host("localhost").port(port)
                    .deadline(Duration.ofSeconds(30)).maxRetries(0).build();
            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            Job live = null;
            while (System.nanoTime() < deadline) {
                live = client.getJobStatus(jobId);
                if (live.getTasksCount() > 0
                        && live.getTasks(0).getState() == TaskState.TASK_STATE_COMPLETED) {
                    break;
                }
                Thread.sleep(200);
            }
            assertNotNull(live);
            assertEquals(TaskState.TASK_STATE_COMPLETED, live.getTasks(0).getState(),
                    "task update after reconnect must flow through the re-opened stream");
            assertEquals(JobState.JOB_STATE_RUNNING, live.getState(), "job must still be running");

            // Job ends: the terminal report and its ack ride the fresh stream.
            finishJob.countDown();
            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < deadline && !worker.statusStore.loadAllJobs().isEmpty()) {
                Thread.sleep(200);
            }
            assertTrue(worker.statusStore.loadAllJobs().isEmpty(),
                    "terminal ack on the re-opened stream should drop the rows");
            assertEquals(JobState.JOB_STATE_COMPLETED, client.getJobStatus(jobId).getState());
        } finally {
            worker.close();
            workerThread.join(5000);
            if (client != null) {
                client.close();
            }
            if (restarted != null) {
                restarted.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            }
        }
    }

    /** Sends one framed task-status update to the worker's WebSocket, like the SDK does. */
    private static void postTaskState(String workerUrl, String jobId, String taskName, TaskState state) {
        try {
            byte[] proto = StatusUpdate.newBuilder()
                    .setJobId(jobId)
                    .setTaskIndex(0)
                    .setTaskName(taskName)
                    .setTaskState(state)
                    .build().toByteArray();
            byte[] framed = new byte[proto.length + 1];
            framed[0] = JobCallbackHandler.TYPE_TAG_STATUS;
            System.arraycopy(proto, 0, framed, 1, proto.length);
            WebSocket ws = HttpClient.newHttpClient().newWebSocketBuilder()
                    .buildAsync(URI.create(workerUrl), new WebSocket.Listener() {})
                    .join();
            ws.sendBinary(ByteBuffer.wrap(framed), true).join();
            Thread.sleep(100);  // let the worker relay before the socket closes
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static WorkerConfig testConfig(int coordinatorPort) throws IOException {
        Path configPath = Path.of(CoordinatorFailoverTest.class.getClassLoader()
                .getResource("config.yaml").getPath());
        WorkerConfig config = WorkerConfig.load(configPath);
        config.getCoordinator().setPort(coordinatorPort);
        return config;
    }
}
