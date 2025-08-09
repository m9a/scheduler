package com.scheduler.worker;

import com.scheduler.proto.coordinator.*;
import com.scheduler.proto.v1.Job;
import com.scheduler.proto.v1.TaskStatus;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for {@link WorkerAgent}'s main loop. Uses a stub gRPC server
 * (inline {@link WorkerServiceGrpc.WorkerServiceImplBase}) that implements just
 * enough of WorkerService to let the agent register, pull one job, and stream
 * status updates back.
 *
 * <p>Verifies the full agent lifecycle:
 * <pre>
 * agent.run() → register → pullJob → spawn SampleJob process →
 * SampleJob runs tasks → status POSTed via HTTP to agent →
 * agent forwards to fake server via gRPC stream → test asserts
 * </pre>
 */
class WorkerAgentIntegrationTest {

    private static final String COORDINATOR_HOST = "localhost";

    /**
     * Full agent lifecycle without a real coordinator:
     *
     * 1. Stub gRPC server serves one {@link SampleJob} on the first pullJob, then empty
     * 2. {@code WorkerAgent.run()} registers, pulls the job, spawns it as a child JVM
     * 3. SampleJob (child JVM) runs 2 tasks, reports status via HTTP back to the agent
     * 4. Agent forwards each status update to the stub server via gRPC client stream
     * 5. Test collects the proto-level updates and asserts on them
     */
    @Test
    void endToEndSuccess() throws Exception {
        String classpath = System.getProperty("java.class.path");
        List<ReportTaskStatusRequest> updates = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch streamDone = new CountDownLatch(1);

        // Stub coordinator: implements the gRPC WorkerService that the agent connects to.
        // Returns a workerId on register, serves one job on pullJob,
        // and collects status updates from reportTaskStatus.
        Server coordinator = ServerBuilder.forPort(0)
                .addService(new WorkerServiceGrpc.WorkerServiceImplBase() {
                    private final AtomicBoolean jobServed = new AtomicBoolean(false);

                    @Override
                    public void registerWorker(RegisterWorkerRequest request,
                                               StreamObserver<RegisterWorkerResponse> responseObserver) {
                        responseObserver.onNext(RegisterWorkerResponse.newBuilder()
                                .setWorkerId("test-worker-1")
                                .build());
                        responseObserver.onCompleted();
                    }

                    @Override
                    public void pullJob(PullJobRequest request,
                                        StreamObserver<PullJobResponse> responseObserver) {
                        PullJobResponse.Builder response = PullJobResponse.newBuilder();
                        if (jobServed.compareAndSet(false, true)) {
                            response.setJob(Job.newBuilder()
                                    .setId("job-1")
                                    .setName("sample-job")
                                    .setJarPath(classpath)
                                    .setMainClass("com.scheduler.worker.SampleJob_Harness")
                                    .build());
                        }
                        responseObserver.onNext(response.build());
                        responseObserver.onCompleted();
                    }

                    @Override
                    public StreamObserver<ReportTaskStatusRequest> reportTaskStatus(
                            StreamObserver<ReportTaskStatusResponse> responseObserver) {
                        return new StreamObserver<>() {
                            @Override
                            public void onNext(ReportTaskStatusRequest request) {
                                updates.add(request);
                            }

                            @Override
                            public void onError(Throwable t) {
                                streamDone.countDown();
                            }

                            @Override
                            public void onCompleted() {
                                responseObserver.onNext(ReportTaskStatusResponse.getDefaultInstance());
                                responseObserver.onCompleted();
                                streamDone.countDown();
                            }
                        };
                    }
                })
                .build()
                .start();

        try (WorkerAgent agent = new WorkerAgent(COORDINATOR_HOST, coordinator.getPort(), "test-host", 1)) {
            Thread workerThread = new Thread(agent::run);
            workerThread.start();

            // Wait for the gRPC stream to close (signals job execution is done)
            assertTrue(streamDone.await(15, TimeUnit.SECONDS), "Job did not complete in time");

            agent.stop();
            workerThread.interrupt();
            workerThread.join(5000);

            // Agent registered successfully
            assertEquals("test-worker-1", agent.workerId());

            // 2 tasks × (RUNNING + COMPLETED) = 4 status updates
            assertEquals(4, updates.size());

            // step-1: taskIndex=0
            assertEquals(0, updates.get(0).getTaskIndex());
            assertEquals(TaskStatus.TASK_STATUS_RUNNING, updates.get(0).getStatus());
            assertEquals(0, updates.get(1).getTaskIndex());
            assertEquals(TaskStatus.TASK_STATUS_COMPLETED, updates.get(1).getStatus());

            // step-2: taskIndex=1
            assertEquals(1, updates.get(2).getTaskIndex());
            assertEquals(TaskStatus.TASK_STATUS_RUNNING, updates.get(2).getStatus());
            assertEquals(1, updates.get(3).getTaskIndex());
            assertEquals(TaskStatus.TASK_STATUS_COMPLETED, updates.get(3).getStatus());

            // All updates belong to the same job
            assertTrue(updates.stream().allMatch(u -> "job-1".equals(u.getJobId())));
        } finally {
            coordinator.shutdown();
        }
    }
}
