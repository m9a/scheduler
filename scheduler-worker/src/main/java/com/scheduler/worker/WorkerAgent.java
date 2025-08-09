package com.scheduler.worker;

import com.scheduler.proto.coordinator.*;
import com.scheduler.proto.v1.Job;
import com.scheduler.sdk.ExecutionPayload;
import com.scheduler.sdk.TaskStatusUpdate;
import com.sun.net.httpserver.HttpServer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
/**
 * The worker's main agent. Has two roles:
 *
 * <p><b>Inbound</b> — local HTTP server receiving task status updates from
 * {@link com.scheduler.sdk.JobProcess} (running inside the job process, a child JVM
 * spawned by {@link #spawnJobProcess}):
 * <pre>
 * Job process (child JVM)
 *   └─ JobProcess ──HTTP POST /task-status──► WorkerAgent
 * </pre>
 *
 * <p><b>Outbound</b> — gRPC client forwarding state to the coordinator:
 * <pre>
 * WorkerAgent ──gRPC──► Coordinator (WorkerHandler)
 *   register()            RegisterWorker
 *   pullJob()             PullJob
 *   openTaskStatusStream() ReportTaskStatus
 * </pre>
 *
 * <p><b>Main loop</b> ({@link #run()}):
 * <pre>
 * register → loop {
 *   pullJob → open gRPC stream → wire onTaskStatus → spawn job process → wait → close stream
 * }
 * </pre>
 */
public class WorkerAgent implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WorkerAgent.class);
    private static final long POLL_INTERVAL_MS = 5000;

    // -- outbound: gRPC to coordinator --
    private final ManagedChannel channel;
    private final WorkerServiceGrpc.WorkerServiceBlockingStub blockingStub;
    private final WorkerServiceGrpc.WorkerServiceStub asyncStub;

    // -- inbound: HTTP from JobProcess (in job process) --
    private final HttpServer taskStatusServer;

    private final String hostname;
    private final int capacity;
    private volatile boolean running;
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
        log.info("Task status server listening on {}", workerAgentUrl());
    }

    /**
     * Registers with the coordinator and enters the main loop:
     * pull job → execute → report status → repeat.
     * Blocks until {@link #stop()} is called or the thread is interrupted.
     */
    public void run() {
        workerId = register(hostname, capacity);
        log.info("Worker started: workerId={}, hostname={}, capacity={}", workerId, hostname, capacity);

        running = true;
        while (running) {
            Optional<Job> job = pullJob(workerId);
            if (job.isPresent()) {
                executeJob(job.get());
            } else {
                try {
                    Thread.sleep(POLL_INTERVAL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        log.info("Worker loop stopped: workerId={}", workerId);
    }

    /**
     * Registers with the coordinator without entering the main loop.
     * Useful for tests that need to control the lifecycle manually.
     */
    public void start() {
        workerId = register(hostname, capacity);
        log.info("Worker started: workerId={}, hostname={}, capacity={}", workerId, hostname, capacity);
    }

    public void stop() {
        running = false;
    }

    public String workerId() {
        return workerId;
    }

    // ── main loop: execute a single job ──────────────────────────────────────

    private void executeJob(Job job) {
        log.info("Executing job: jobId={}, name={}, jarPath={}", job.getId(), job.getName(), job.getJarPath());

        TaskStatusReporter reporter = openTaskStatusStream();
        onTaskStatus(reporter::report);

        ExecutionPayload executionPayload = new ExecutionPayload(workerAgentUrl(), job.getId(), Map.of());
        JobDetails details = new JobDetails(
                job.getId(),
                job.getJarPath(),
                job.getMainClass().isEmpty() ? null : job.getMainClass(),
                executionPayload.encode()
        );

        try {
            int exitCode = spawnJobProcess(details);
            if (exitCode != 0) {
                log.warn("Job process exited with non-zero code: jobId={}, exitCode={}", job.getId(), exitCode);
            }
        } catch (IOException | InterruptedException e) {
            log.error("Failed to execute job: jobId={}, error={}", job.getId(), e.getMessage(), e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        } finally {
            reporter.complete();
            try {
                reporter.awaitCompletion(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            log.info("Finished job: jobId={}, name={}", job.getId(), job.getName());
        }
    }

    /**
     * Spawns a child JVM for the job JAR, reads its stdout/stderr, and waits for exit.
     * The child process runs JobProcess.run(tasks) which POSTs status updates back
     * to this agent's HTTP server.
     */
    int spawnJobProcess(JobDetails details) throws IOException, InterruptedException {
        List<String> command = buildCommand(details);
        log.info("Starting job process: {}", String.join(" ", command));

        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .start();

        // process.getInputStream() is the parent's view of the child's stdout/stderr
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                log.info("[job:{}] {}", details.jobId(), line);
            }
        }

        int exitCode = process.waitFor();
        log.info("Job process finished: jobId={}, exitCode={}", details.jobId(), exitCode);
        return exitCode;
    }

    static List<String> buildCommand(JobDetails details) {
        List<String> command = new ArrayList<>();
        command.add("java");
        if (details.mainClass() != null) {
            command.add("-cp");
            command.add(details.jarPath());
            command.add(details.mainClass());
        } else {
            command.add("-jar");
            command.add(details.jarPath());
        }
        if (details.payload() != null) {
            command.add(details.payload());
        }
        return command;
    }

    // ── inbound: task status from JobProcess (in job process) → HTTP → here ─

    /**
     * Returns the URL that job processes use to POST task status updates back to this agent.
     * Passed to the job process as part of {@link ExecutionPayload}.
     */
    public String workerAgentUrl() {
        return "http://localhost:" + taskStatusServer.getAddress().getPort();
    }

    /**
     * Registers a handler for task status updates received from JobProcess
     * (running in the job process). JobProcess POSTs {@link TaskStatusUpdate}
     * JSON to {@code /task-status} on this server.
     */
    @FunctionalInterface
    public interface TaskStatusHandler {
        void handle(TaskStatusUpdate update);
    }

    public void onTaskStatus(TaskStatusHandler handler) {
        taskStatusServer.createContext("/task-status", exchange -> {
            try {
                if (!"POST".equals(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }

                String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                TaskStatusUpdate update = TaskStatusUpdate.fromJson(body);
                log.info("Received status from JobProcess: jobId={}, taskIndex={}, taskName={}, status={}",
                        update.jobId(), update.taskIndex(), update.taskName(), update.status());

                handler.handle(update);
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
     * Opens a bidirectional pipe to the coordinator for streaming task status updates.
     *
     * <p>Two observers are involved:
     * <ul>
     *   <li>{@code responseObserver} — handles what comes back from the coordinator.
     *       Since ReportTaskStatus returns a single response only after the stream closes,
     *       onNext is empty; onCompleted/onError count down the latch so the caller
     *       (via {@link TaskStatusReporter#awaitCompletion}) can wait for acknowledgment.</li>
     *   <li>{@code requestObserver} — the send side. Each {@code onNext()} pushes a status
     *       update to the coordinator. Wrapped inside {@link TaskStatusReporter} for
     *       SDK-to-proto conversion.</li>
     * </ul>
     */
    public TaskStatusReporter openTaskStatusStream() {
        CountDownLatch done = new CountDownLatch(1);

        // Receive side: coordinator sends a single response when the stream closes.
        // The latch lets callers block until the coordinator has acknowledged.
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

        // Send side: each onNext() pushes a task status update to the coordinator
        StreamObserver<ReportTaskStatusRequest> requestObserver = asyncStub.reportTaskStatus(responseObserver);
        return new TaskStatusReporter(requestObserver, done);
    }

    // ── lifecycle ────────────────────────────────────────────────────────────

    @Override
    public void close() throws Exception {
        stop();
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

        agent.run();
    }
}
