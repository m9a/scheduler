package com.scheduler.worker;

import com.scheduler.coordinator.JobManagerImpl;
import com.scheduler.coordinator.client.UserRequestHandler;
import com.scheduler.coordinator.worker.WorkerHandler;
import com.scheduler.proto.v1.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration test with a real coordinator. Exercises the full
 * submission flow:
 *
 * <pre>
 * Client ──SubmitJob──► Coordinator ◄──RegisterWorker/PullJob── WorkerAgent
 *                          │                                        │
 *                     JobManagerImpl                          spawn child JVM
 *                          │                                        │
 * Client ──GetJobStatus──► │ ◄──ReportTaskStatus────────────────────┘
 * </pre>
 *
 * Unlike {@link WorkerAgentIntegrationTest}, which stubs the coordinator,
 * this test wires real {@link UserRequestHandler} and {@link WorkerHandler}
 * sharing the same {@link JobManagerImpl}.
 */
class EndToEndIntegrationTest {

    private Server coordinatorServer;
    private ManagedChannel clientChannel;
    private ClientServiceGrpc.ClientServiceBlockingStub clientStub;

    @BeforeEach
    void setUp() throws Exception {
        JobManagerImpl jobManager = new JobManagerImpl();

        coordinatorServer = ServerBuilder.forPort(0)
                .addService(new UserRequestHandler(jobManager))
                .addService(new WorkerHandler(jobManager))
                .build()
                .start();

        clientChannel = ManagedChannelBuilder
                .forAddress("localhost", coordinatorServer.getPort())
                .usePlaintext()
                .build();
        clientStub = ClientServiceGrpc.newBlockingStub(clientChannel);
    }

    @AfterEach
    void tearDown() throws Exception {
        clientChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        coordinatorServer.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
     * Submit a job via the client API, let a real WorkerAgent pull and execute it,
     * then poll GetJobStatus until the coordinator reports COMPLETED.
     *
     * <pre>
     * Client                    Coordinator                     WorkerAgent              Job Process
     *   │                          │                               │                        │
     *   ├── SubmitJob(SampleJob) ──►  JobManager.submit()          │                        │
     *   │◄── jobId, QUEUED ────────┤                               │                        │
     *   │                          │◄── RegisterWorker ────────────┤                        │
     *   │                          │◄── PullJob ───────────────────┤                        │
     *   │                          │── Job(SampleJob_Harness) ────►│                        │
     *   │                          │                               ├── spawn child JVM ────►│
     *   │                          │                               │◄── HTTP POST status ───┤
     *   │                          │◄── gRPC ReportTaskStatus ─────┤                        │
     *   │                          │   (×4: 2 tasks × RUNNING+COMPLETED)                    │
     *   │── GetJobStatus(jobId) ──►│                               │                        │
     *   │◄── COMPLETED ────────────┤                               │                        │
     * </pre>
     */
    @Test
    void submitExecuteComplete() throws Exception {
        String classpath = System.getProperty("java.class.path");

        // 1. Client submits a job
        SubmitJobResponse submitResponse = clientStub.submitJob(SubmitJobRequest.newBuilder()
                .setName("sample-job")
                .setJarPath(classpath)
                .setMainClass("com.scheduler.worker.SampleJob_Harness")
                .build());

        String jobId = submitResponse.getJob().getId();
        assertFalse(jobId.isEmpty(), "Expected a job ID");
        assertEquals(JobStatus.JOB_STATUS_QUEUED, submitResponse.getJob().getStatus());

        // 2. Start a WorkerAgent — it registers, pulls the job, spawns the process
        try (WorkerAgent agent = new WorkerAgent("localhost", coordinatorServer.getPort(), "test-host", 1)) {
            Thread workerThread = new Thread(agent::run);
            workerThread.start();

            // 3. Poll GetJobStatus until COMPLETED or timeout
            JobStatus finalStatus = pollUntilTerminal(jobId, 15, TimeUnit.SECONDS);

            agent.stop();
            workerThread.interrupt();
            workerThread.join(5000);

            // 4. Assert final state
            assertEquals(JobStatus.JOB_STATUS_COMPLETED, finalStatus);

            Job finalJob = clientStub.getJobStatus(GetJobStatusRequest.newBuilder()
                    .setJobId(jobId).build()).getJob();
            assertTrue(finalJob.getCompletedAtMillis() > 0, "Expected completedAt to be set");
            assertTrue(finalJob.getStartedAtMillis() > 0, "Expected startedAt to be set");
        }
    }

    private JobStatus pollUntilTerminal(String jobId, long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        JobStatus status = JobStatus.JOB_STATUS_UNSPECIFIED;

        while (System.nanoTime() < deadline) {
            GetJobStatusResponse response = clientStub.getJobStatus(
                    GetJobStatusRequest.newBuilder().setJobId(jobId).build());
            status = response.getJob().getStatus();

            if (status == JobStatus.JOB_STATUS_COMPLETED || status == JobStatus.JOB_STATUS_FAILED) {
                return status;
            }
            Thread.sleep(200);
        }

        fail("Job did not reach terminal status within timeout. Last status: " + status);
        return status; // unreachable
    }
}
