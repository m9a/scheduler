package com.scheduler.worker;

import com.scheduler.proto.v1.*;
import com.scheduler.sdk.TaskStatusUpdate;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * The worker's main agent. Connects to the coordinator via gRPC to register,
 * pull jobs, and stream task status updates.
 *
 * <pre>
 * WorkerAgent ──gRPC──► Coordinator (WorkerHandler)
 *   register()            RegisterWorker
 *   pullJob()             PullJob
 *   openTaskStatusStream() ReportTaskStatus
 * </pre>
 */
public class WorkerAgent implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WorkerAgent.class);

    private final ManagedChannel channel;
    private final WorkerServiceGrpc.WorkerServiceBlockingStub blockingStub;
    private final WorkerServiceGrpc.WorkerServiceStub asyncStub;

    public WorkerAgent(String coordinatorHost, int coordinatorPort) {
        this.channel = ManagedChannelBuilder.forAddress(coordinatorHost, coordinatorPort)
                .usePlaintext()
                .build();
        this.blockingStub = WorkerServiceGrpc.newBlockingStub(channel);
        this.asyncStub = WorkerServiceGrpc.newStub(channel);
    }

    public String register(String hostname, int port, int capacity) {
        log.info("Registering with coordinator: hostname={}, port={}, capacity={}", hostname, port, capacity);
        RegisterWorkerResponse response = blockingStub.registerWorker(RegisterWorkerRequest.newBuilder()
                .setHostname(hostname)
                .setPort(port)
                .setCapacity(capacity)
                .build());
        log.info("Registered with coordinator: workerId={}", response.getWorkerId());
        return response.getWorkerId();
    }

    public Optional<Job> pullJob(String workerId) {
        log.debug("Pulling job from coordinator: workerId={}", workerId);
        PullJobResponse response = blockingStub.pullJob(PullJobRequest.newBuilder()
                .setWorkerId(workerId)
                .build());
        if (response.hasJob()) {
            log.info("Pulled job from coordinator: jobId={}, name={}", response.getJob().getId(), response.getJob().getName());
            return Optional.of(response.getJob());
        }
        log.debug("No jobs available from coordinator: workerId={}", workerId);
        return Optional.empty();
    }

    /**
     * Opens a client-streaming RPC for reporting task status updates.
     * Returns a {@link TaskStatusReporter} that the caller uses to send
     * updates and close the stream when the job finishes.
     */
    public TaskStatusReporter openTaskStatusStream() {
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<ReportTaskStatusResponse> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(ReportTaskStatusResponse response) {}

            @Override
            public void onError(Throwable t) {
                log.error("ReportTaskStatus stream error: {}", t.getMessage());
                done.countDown();
            }

            @Override
            public void onCompleted() {
                done.countDown();
            }
        };

        StreamObserver<ReportTaskStatusRequest> requestObserver = asyncStub.reportTaskStatus(responseObserver);
        return new TaskStatusReporter(requestObserver, done);
    }

    @Override
    public void close() throws Exception {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
}
