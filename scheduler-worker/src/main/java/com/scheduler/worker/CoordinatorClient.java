package com.scheduler.worker;

import com.scheduler.proto.job.Report;
import com.scheduler.proto.job.StatusUpdate;
import com.scheduler.proto.worker.HeartbeatRequest;
import com.scheduler.proto.worker.JobLiveness;
import com.scheduler.proto.worker.PullJobRequest;
import com.scheduler.proto.worker.PullJobResponse;
import com.scheduler.proto.worker.RegisterWorkerRequest;
import com.scheduler.proto.worker.RegisterWorkerResponse;
import com.scheduler.proto.worker.StatusUpdateResponse;
import com.scheduler.proto.worker.WorkerServiceGrpc;
import com.scheduler.proto.v1.Job;
import com.scheduler.proto.v1.ResourceRequirements;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * <b>Worker → Coordinator leg.</b> The single owner of the gRPC channel to the
 * coordinator — every RPC the worker sends goes through this class, nothing else
 * talks to the coordinator. Called by {@link WorkerAgent} (register/pull/heartbeat,
 * telemetry forwarding) and, for per-job status, via the
 * {@link CoordinatorStatusStream} this class opens. The other end is the
 * coordinator's {@code WorkerHandler}.
 *
 * <pre>
 * WorkerAgent ──► CoordinatorClient ──gRPC──► Coordinator (WorkerHandler)
 *   register()           RegisterWorker
 *   pullJob()            PullJob
 *   startHeartbeat()     Heartbeat            (5s loop, daemon thread)
 *   forwardTelemetry()   ReportTelemetry      (unary, lossy by design)
 *   openStatusStream()   ReportStatus         (client stream, one per job)
 * </pre>
 */
class CoordinatorClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CoordinatorClient.class);
    private static final long HEARTBEAT_INTERVAL_MS = 5000;

    private final ManagedChannel channel;
    private final WorkerServiceGrpc.WorkerServiceBlockingStub blockingStub;
    private final WorkerServiceGrpc.WorkerServiceStub asyncStub;
    private ScheduledExecutorService heartbeatExecutor;

    CoordinatorClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.blockingStub = WorkerServiceGrpc.newBlockingStub(channel);
        this.asyncStub = WorkerServiceGrpc.newStub(channel);
    }

    /** Registers this worker and its resources; returns the coordinator-assigned worker ID. */
    String register(String hostname, int memoryMb, int cpuCores, boolean gpu, Set<String> capabilities) {
        log.info("Registering with coordinator: hostname={}, memory={}, cpu={}, gpu={}, capabilities={}",
                hostname, memoryMb, cpuCores, gpu, capabilities);
        RegisterWorkerResponse response = blockingStub.registerWorker(RegisterWorkerRequest.newBuilder()
                .setHostname(hostname)
                // Same shape as what jobs require — see ResourceRequirements in common.proto.
                .setResources(ResourceRequirements.newBuilder()
                        .setMemoryMb(memoryMb)
                        .setCpuCores(cpuCores)
                        .setGpu(gpu)
                        .addAllCapabilities(capabilities))
                .build());
        log.info("Registered with coordinator: workerId={}", response.getWorkerId());
        return response.getWorkerId();
    }

    Optional<Job> pullJob(String workerId) {
        log.debug("Pulling job from coordinator: workerId={}", workerId);
        PullJobResponse response = blockingStub.pullJob(PullJobRequest.newBuilder()
                .setWorkerId(workerId)
                .build());
        if (response.hasJob()) {
            log.info("Pulled job from coordinator: jobId={}, name={}",
                    response.getJob().getId(), response.getJob().getName());
            return Optional.of(response.getJob());
        }
        log.debug("No jobs available from coordinator: workerId={}", workerId);
        return Optional.empty();
    }

    /** Starts the liveness loop — the coordinator fails this worker's jobs if these stop. */
    void startHeartbeat(String workerId) {
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "heartbeat-sender");
            t.setDaemon(true);
            return t;
        });
        heartbeatExecutor.scheduleAtFixedRate(() -> {
            try {
                blockingStub.heartbeat(HeartbeatRequest.newBuilder()
                        .setWorkerId(workerId)
                        .build());
                log.debug("Sent heartbeat: workerId={}", workerId);
            } catch (Exception e) {
                log.warn("Failed to send heartbeat: workerId={}, error={}", workerId, e.getMessage());
            }
        }, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /** Forwards job-emitted telemetry to the coordinator. Lossy by design — log and move on. */
    void forwardTelemetry(Report report) {
        try {
            blockingStub.reportTelemetry(report);
        } catch (Exception e) {
            log.warn("Failed to forward telemetry for job={}, taskIndex={}: {}",
                    report.getJobId(), report.getTaskIndex(), e.getMessage());
        }
    }

    /** Reports a job's worker-tracked last-activity time. Lossy — log and move on. */
    void reportLiveness(String jobId, long lastActivityAtMillis) {
        try {
            blockingStub.reportLiveness(JobLiveness.newBuilder()
                    .setJobId(jobId)
                    .setLastActivityAtMillis(lastActivityAtMillis)
                    .build());
        } catch (Exception e) {
            log.debug("Failed to report liveness for job={}: {}", jobId, e.getMessage());
        }
    }

    /**
     * Opens the per-job client-streaming pipe for status updates.
     *
     * <p>Two observers are involved:
     * <ul>
     *   <li>{@code responseObserver} — handles what comes back from the coordinator.
     *       Since ReportStatus returns a single response only after the stream closes,
     *       onNext is empty; onCompleted/onError count down the latch so the caller
     *       (via {@link CoordinatorStatusStream#awaitCompletion}) can wait for
     *       acknowledgment.</li>
     *   <li>{@code requestObserver} — the send side. Each {@code onNext()} pushes a status
     *       update to the coordinator. Wrapped inside {@link CoordinatorStatusStream}
     *       for conversion to proto.</li>
     * </ul>
     */
    CoordinatorStatusStream openStatusStream(String jobId) {
        CountDownLatch done = new CountDownLatch(1);

        // Receive side: coordinator sends a single response when the stream closes.
        // The latch lets callers block until the coordinator has acknowledged.
        StreamObserver<StatusUpdateResponse> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(StatusUpdateResponse response) {}

            @Override
            public void onError(Throwable t) {
                log.error("ReportStatus stream error: {}", t.getMessage());
                done.countDown();
            }

            @Override
            public void onCompleted() {
                done.countDown();
            }
        };

        // Send side: each onNext() pushes a status update to the coordinator
        StreamObserver<StatusUpdate> requestObserver =
                asyncStub.reportStatus(responseObserver);

        return new CoordinatorStatusStream(requestObserver, done);
    }

    @Override
    public void close() throws InterruptedException {
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdown();
        }
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
}
