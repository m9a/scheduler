package com.scheduler.worker;

import com.scheduler.proto.job.Report;
import com.scheduler.proto.job.StatusUpdate;
import com.scheduler.proto.worker.HeartbeatRequest;
import com.scheduler.proto.worker.PullJobRequest;
import com.scheduler.proto.worker.PullJobResponse;
import com.scheduler.proto.worker.RegisterWorkerRequest;
import com.scheduler.proto.worker.RegisterWorkerResponse;
import com.scheduler.proto.worker.ReportTelemetryResponse;
import com.scheduler.proto.worker.StatusUpdateResponse;
import com.scheduler.proto.worker.SubscribeRequest;
import com.scheduler.proto.worker.SystemCommand;
import com.scheduler.proto.worker.JobCommand;
import com.scheduler.proto.worker.WorkerServiceGrpc;
import com.scheduler.proto.v1.Job;
import com.scheduler.proto.v1.ResourceRequirements;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * <b>Worker → Coordinator leg.</b> Owns the single gRPC channel to the
 * coordinator. Every RPC the worker sends goes through this class.
 * {@link WorkerAgent} calls it; the other end is the coordinator's
 * {@code WorkerHandler}.
 *
 * <pre>
 * WorkerAgent ──► CoordinatorClient ──gRPC──► Coordinator (WorkerHandler)
 *   register()           RegisterWorker
 *   pullJob()            PullJob
 *   startHeartbeat()     Heartbeat            (5s loop, daemon thread)
 *   openTelemetryStream() ReportTelemetry     (client stream, one per job, lossy)
 *   openStatusStream()   ReportStatus         (client stream, one per job)
 *   subscribeSystemCommands() SystemCommands  (server stream, coordinator → worker push)
 *   subscribeJobCommands()    JobCommands     (server stream, coordinator → worker push)
 * </pre>
 */
class CoordinatorClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CoordinatorClient.class);

    // Fixed delay before re-opening a dropped command stream — keeps reconnect simple.
    private static final long RESUBSCRIBE_DELAY_MS = 2000;

    private final ManagedChannel channel;
    private final WorkerServiceGrpc.WorkerServiceBlockingStub blockingStub;
    private final WorkerServiceGrpc.WorkerServiceStub asyncStub;
    private ScheduledExecutorService heartbeatExecutor;
    // Reschedules dropped command-stream subscriptions; created on first subscribe.
    private ScheduledExecutorService commandExecutor;
    // Set by close() on worker shutdown. Stops pending resubscribes from firing —
    // unlike a stream close, which triggers one.
    private volatile boolean shuttingDown;

    CoordinatorClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.blockingStub = WorkerServiceGrpc.newBlockingStub(channel);
        this.asyncStub = WorkerServiceGrpc.newStub(channel);
    }

    /**
     * Registers this worker: stable id, resources, and the jobs it still holds
     * from before a restart. Returns the ids the worker must kill — the
     * coordinator already marked those jobs dead (heartbeat lost), but their
     * containers may still run here.
     */
    Set<String> register(String workerId, String hostname, int memoryMb, int cpuCores, boolean gpu,
                         Set<String> capabilities, List<StatusUpdate> knownJobs) {
        log.info("Registering with coordinator: workerId={}, hostname={}, memory={}, cpu={}, gpu={}, capabilities={}, knownJobs={}",
                workerId, hostname, memoryMb, cpuCores, gpu, capabilities, knownJobs.size());
        RegisterWorkerResponse response = blockingStub.registerWorker(RegisterWorkerRequest.newBuilder()
                .setWorkerId(workerId)
                .setHostname(hostname)
                // Same shape as what jobs require — see ResourceRequirements in common.proto.
                .setResources(ResourceRequirements.newBuilder()
                        .setMemoryMb(memoryMb)
                        .setCpuCores(cpuCores)
                        .setGpu(gpu)
                        .addAllCapabilities(capabilities))
                .addAllKnownJobs(knownJobs)
                .build());
        Set<String> jobIdsToKill = new HashSet<>(response.getJobIdsToKillList());
        log.info("Registered with coordinator: workerId={}{}", response.getWorkerId(),
                jobIdsToKill.isEmpty() ? "" : ", jobsToKill=" + jobIdsToKill);
        return jobIdsToKill;
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
    void startHeartbeat(String workerId, long intervalMs) {
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
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Subscribes to the coordinator's system-command push stream (drain).
     * The call returns immediately; {@code handler} runs on a gRPC thread per
     * pushed command. A dropped stream re-subscribes after a fixed delay.
     */
    void subscribeSystemCommands(String workerId, Consumer<SystemCommand> handler) {
        ensureCommandExecutor();
        subscribe("system", workerId, asyncStub::systemCommands, handler);
    }

    /** Job-command counterpart of {@link #subscribeSystemCommands} (cancel, preempt). */
    void subscribeJobCommands(String workerId, Consumer<JobCommand> handler) {
        ensureCommandExecutor();
        subscribe("job", workerId, asyncStub::jobCommands, handler);
    }

    private synchronized void ensureCommandExecutor() {
        if (commandExecutor == null) {
            commandExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "command-resubscriber");
                t.setDaemon(true);
                return t;
            });
        }
    }

    private <T> void subscribe(String name, String workerId,
                               BiConsumer<SubscribeRequest, StreamObserver<T>> rpc, Consumer<T> handler) {
        if (shuttingDown) {
            return;
        }
        StreamObserver<T> observer = new StreamObserver<>() {
            @Override
            public void onNext(T command) {
                try {
                    handler.accept(command);
                } catch (Exception e) {
                    log.error("Error handling {} command: {}", name, e.getMessage(), e);
                }
            }

            @Override
            public void onError(Throwable t) {
                resubscribe(name, workerId, rpc, handler, t.getMessage());
            }

            @Override
            public void onCompleted() {
                resubscribe(name, workerId, rpc, handler, "stream closed by coordinator");
            }
        };
        log.info("Subscribing to {} command stream: workerId={}", name, workerId);
        rpc.accept(SubscribeRequest.newBuilder().setWorkerId(workerId).build(), observer);
    }

    private <T> void resubscribe(String name, String workerId,
                                 BiConsumer<SubscribeRequest, StreamObserver<T>> rpc, Consumer<T> handler, String why) {
        if (shuttingDown) {
            return;
        }
        log.warn("{} command stream lost ({}); resubscribing in {}ms", name, why, RESUBSCRIBE_DELAY_MS);
        commandExecutor.schedule(() -> subscribe(name, workerId, rpc, handler),
                RESUBSCRIBE_DELAY_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Opens the per-job client-streaming pipe for telemetry. The coordinator sends
     * one response when the stream closes. onError/onCompleted release the latch,
     * so {@link CoordinatorTelemetryStream#awaitCompletion} can block on the ack.
     */
    CoordinatorTelemetryStream openTelemetryStream(String jobId) {
        CountDownLatch done = new CountDownLatch(1);

        StreamObserver<ReportTelemetryResponse> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(ReportTelemetryResponse response) {}

            @Override
            public void onError(Throwable t) {
                log.warn("ReportTelemetry stream error for job={}: {}", jobId, t.getMessage());
                done.countDown();
            }

            @Override
            public void onCompleted() {
                done.countDown();
            }
        };

        StreamObserver<Report> requestObserver = asyncStub.reportTelemetry(responseObserver);
        return new CoordinatorTelemetryStream(requestObserver, done);
    }

    /**
     * Opens the per-job client-streaming pipe for status updates.
     *
     * <p>Two observers:
     * <ul>
     *   <li>{@code responseObserver} — the receive side. ReportStatus sends one
     *       response only when the stream closes, so onNext is empty.
     *       onCompleted/onError count down the latch; the caller waits on it via
     *       {@link CoordinatorStatusStream#awaitCompletion}.</li>
     *   <li>{@code requestObserver} — the send side. Each {@code onNext()} pushes
     *       one status update. {@link CoordinatorStatusStream} wraps it.</li>
     * </ul>
     */
    CoordinatorStatusStream openStatusStream(String jobId) {
        CountDownLatch done = new CountDownLatch(1);
        // True once the coordinator's close-ack response arrives — proof it applied
        // every update. A stream error before that must NOT count as acked: the
        // caller keeps the job's store rows in that case.
        AtomicBoolean acked = new AtomicBoolean(false);

        // Receive side: the coordinator sends one response + onCompleted when the
        // worker closes the stream. The latch lets callers block until then.
        StreamObserver<StatusUpdateResponse> responseObserver = new StreamObserver<>() {

            @Override
            public void onNext(StatusUpdateResponse response) {
                // This response IS the ack — the coordinator sends it only after
                // applying every update on the stream.
                acked.set(true);
                log.info("Received status close ack from coordinator: jobId={}", jobId);
            }

            @Override
            public void onError(Throwable t) {
                if (acked.get()) {
                    // The ack got through before the stream broke — updates are
                    // safe on the coordinator, only the close was untidy.
                    log.warn("ReportStatus stream error after ack: jobId={}, error={}", jobId, t.getMessage());
                } else {
                    log.error("ReportStatus stream error before ack — status rows will be kept: jobId={}, error={}",
                            jobId, t.getMessage());
                }
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

        return new CoordinatorStatusStream(requestObserver, done, acked);
    }

    @Override
    public void close() throws InterruptedException {
        shuttingDown = true;  // stop any pending re-subscribe from firing
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdown();
        }
        if (commandExecutor != null) {
            commandExecutor.shutdown();
        }
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
}
