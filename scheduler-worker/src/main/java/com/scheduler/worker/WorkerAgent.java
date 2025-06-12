package com.scheduler.worker;

import com.scheduler.proto.v1.*;
import com.scheduler.sdk.TaskStatusUpdate;
import com.sun.net.httpserver.HttpServer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * The worker's main agent. Has two roles:
 *
 * <p><b>Inbound</b> — local HTTP server receiving task status updates from
 * {@link com.scheduler.sdk.JobRunner} (running inside the job process, a child JVM
 * spawned by {@link JobExecutor}):
 * <pre>
 * Job process (child JVM)
 *   └─ JobRunner ──HTTP POST /task-status──► WorkerAgent
 * </pre>
 *
 * <p><b>Outbound</b> — gRPC client forwarding state to the coordinator:
 * <pre>
 * WorkerAgent ──gRPC──► Coordinator (WorkerHandler)
 *   register()            RegisterWorker
 *   pullJob()             PullJob
 *   openTaskStatusStream() ReportTaskStatus
 * </pre>
 */
public class WorkerAgent implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WorkerAgent.class);

    // -- outbound: gRPC to coordinator --
    private final ManagedChannel channel;
    private final WorkerServiceGrpc.WorkerServiceBlockingStub blockingStub;
    private final WorkerServiceGrpc.WorkerServiceStub asyncStub;

    // -- inbound: HTTP from JobRunner (in job process) --
    private final HttpServer taskStatusServer;

    private final String hostname;
    private final int capacity;
    private String workerId;

    public WorkerAgent(String coordinatorHost, int coordinatorPort, String hostname, int capacity) throws IOException {
        this.channel = ManagedChannelBuilder.forAddress(coordinatorHost, coordinatorPort)
                .usePlaintext()
                .build();
        this.blockingStub = WorkerServiceGrpc.newBlockingStub(channel);
        this.asyncStub = WorkerServiceGrpc.newStub(channel);
        this.hostname = hostname;
        this.capacity = capacity;
        this.taskStatusServer = HttpServer.create(new InetSocketAddress(0), 0);
        this.taskStatusServer.start();
        log.info("Task status server listening on port {}", taskStatusPort());
    }

    public void start() {
        workerId = register(hostname, capacity);
        log.info("Worker started: workerId={}, hostname={}, capacity={}", workerId, hostname, capacity);
    }

    public String workerId() {
        return workerId;
    }

    // ── inbound: task status from JobRunner (in job process) → HTTP → here ─

    /**
     * Returns the port the task status HTTP server is listening on.
     * Passed to the job process as {@code -Dscheduler.callback.url}.
     */
    public int taskStatusPort() {
        return taskStatusServer.getAddress().getPort();
    }

    /**
     * Registers a handler for task status updates received from JobRunner
     * (running in the job process). JobRunner POSTs {@link TaskStatusUpdate}
     * JSON to {@code /task-status} on this server.
     */
    public void onTaskStatus(Consumer<TaskStatusUpdate> handler) {
        taskStatusServer.createContext("/task-status", exchange -> {
            try {
                if (!"POST".equals(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }

                String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                TaskStatusUpdate update = TaskStatusUpdate.fromJson(body);
                log.info("Received status from job process: jobId={}, taskIndex={}, taskName={}, status={}",
                        update.jobId(), update.taskIndex(), update.taskName(), update.status());

                handler.accept(update);
                exchange.sendResponseHeaders(200, -1);
            } catch (Exception e) {
                log.error("Failed to handle task status update: {}", e.getMessage(), e);
                exchange.sendResponseHeaders(500, -1);
            } finally {
                exchange.close();
            }
        });
    }

    // ── outbound: gRPC to coordinator ────────────────────────────────────────

    public String register(String hostname, int capacity) {
        log.info("Registering with coordinator: hostname={}, capacity={}", hostname, capacity);
        RegisterWorkerResponse response = blockingStub.registerWorker(RegisterWorkerRequest.newBuilder()
                .setHostname(hostname)
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
     * Opens a client-streaming RPC for reporting task status updates to the coordinator.
     * Returns a {@link TaskStatusReporter} that converts SDK updates to proto and streams them.
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

    // ── lifecycle ────────────────────────────────────────────────────────────

    @Override
    public void close() throws Exception {
        taskStatusServer.stop(0);
        log.info("Task status server stopped");
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public static void main(String[] args) throws IOException {
        String coordinatorHost = args.length > 0 ? args[0] : "localhost";
        int coordinatorPort = args.length > 1 ? Integer.parseInt(args[1]) : 9090;
        String hostname = args.length > 2 ? args[2] : "localhost";
        int capacity = args.length > 3 ? Integer.parseInt(args[3]) : 1;

        WorkerAgent agent = new WorkerAgent(coordinatorHost, coordinatorPort, hostname, capacity);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down worker");
            try {
                agent.close();
            } catch (Exception e) {
                log.error("Error shutting down worker: {}", e.getMessage());
            }
        }));

        agent.start();
    }
}
