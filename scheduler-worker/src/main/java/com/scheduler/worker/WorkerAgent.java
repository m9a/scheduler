package com.scheduler.worker;

import com.scheduler.core.FailureReason;
import com.scheduler.core.InputFile;
import com.scheduler.core.ObjectStore;
import com.scheduler.proto.coordinator.*;
import com.scheduler.proto.v1.Job;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * The worker's main agent. Owns the full job lifecycle: start, monitor, kill, report.
 * The coordinator is a passive state store that applies whatever this agent sends.
 *
 * <p><b>Inbound</b> — local WebSocket server receiving task status updates from the
 * job process (a child process spawned by {@link #spawnJobProcess}). Each binary frame
 * starts with a one-byte type tag ({@code 0x01} = status) followed by a proto
 * {@link com.scheduler.proto.job.StatusUpdate} payload.
 * <pre>
 * Job process (child)
 *   └─ JobProcess/job_runner ──WebSocket──► WorkerAgent
 * </pre>
 *
 * <p><b>Outbound</b> — gRPC client forwarding state to the coordinator:
 * <pre>
 * WorkerAgent ──gRPC──► Coordinator (WorkerHandler)
 *   register()            RegisterWorker
 *   pullJob()             PullJob
 *   openStatusStream()    ReportStatus
 * </pre>
 *
 * <p><b>Main loop</b> ({@link #run()}):
 * <pre>
 * register → loop {
 *   pullJob → stage input files → open gRPC stream
 *   → report STARTING → spawn job process → report RUNNING on first task
 *   → wait → report COMPLETED/FAILED/KILLED → upload outputs → close stream
 * }
 * </pre>
 */
public class WorkerAgent implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WorkerAgent.class);
    private static final long POLL_INTERVAL_MS = 5000;
    private static final long HEARTBEAT_INTERVAL_MS = 5000;
    private static final Duration DEFAULT_JOB_EXECUTION_TIMEOUT = Duration.ofMinutes(10);

    // -- outbound: gRPC to coordinator --
    private final ManagedChannel channel;
    private final WorkerServiceGrpc.WorkerServiceBlockingStub blockingStub;
    private final WorkerServiceGrpc.WorkerServiceStub asyncStub;

    // -- inbound: WebSocket from JobProcess (in job process) --
    private final JobWebSocketServer wsServer;

    // -- file I/O --
    private final ObjectStore objectStore;

    private final String hostname;
    private final int memory;
    private final int cpu;
    private final boolean gpu;
    private final Set<String> capabilities;
    private final Duration jobExecutionTimeout;
    private final String dockerNetwork;
    private final String mlflowTrackingUri;
    private volatile boolean running;
    private String workerId;
    private ScheduledExecutorService heartbeatExecutor;

    public WorkerAgent(WorkerConfig config, ObjectStore objectStore,
                       Duration jobExecutionTimeout) throws IOException {
        this.channel = ManagedChannelBuilder.forAddress(
                        config.getCoordinator().getHost(), config.getCoordinator().getPort())
                .usePlaintext()
                .build();
        this.blockingStub = WorkerServiceGrpc.newBlockingStub(channel);
        this.asyncStub = WorkerServiceGrpc.newStub(channel);
        this.hostname = config.getHostname();
        this.memory = config.getResources().getMemory();
        this.cpu = config.getResources().getCpu();
        this.gpu = config.getResources().isGpu();
        this.objectStore = objectStore;
        this.jobExecutionTimeout = jobExecutionTimeout;
        this.dockerNetwork = config.getDocker().getNetwork();
        this.mlflowTrackingUri = config.getMlflow().getTrackingUri();

        this.capabilities = config.getResources().getCapabilities() == null
                ? Set.of()
                : Set.copyOf(config.getResources().getCapabilities());

        // Bind to all NICs so Docker containers on the bridge network can reach
        // this server via the host's real hostname (passed in workerAgentUrl).
        this.wsServer = new JobWebSocketServer(new InetSocketAddress("0.0.0.0", config.getPort()));
        this.wsServer.start();
        try {
            this.wsServer.awaitReady();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted waiting for WebSocket server to start", e);
        }
        log.info("WebSocket server listening on {}", workerAgentUrl());
    }


    /**
     * Registers with the coordinator and enters the main loop:
     * pull job → execute → report status → repeat.
     * Blocks until {@link #stop()} is called or the thread is interrupted.
     *
     * <p><b>Limitation:</b> runs one job at a time because {@code executeJob} blocks
     * on {@code process.waitFor()}. To run multiple jobs concurrently, submit
     * {@code executeJob} calls to a bounded thread pool. That also requires
     * multiplexing the task status HTTP handler by jobId instead of replacing
     * it per job.
     */
    public void run() {
        workerId = register(hostname);
        log.info("Worker running: workerId={}, hostname={}", workerId, hostname);

        startHeartbeat();
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

    public void stop() {
        running = false;
    }

    public String workerId() {
        return workerId;
    }

    // ── main loop: execute a single job ──────────────────────────────────────

    private void executeJob(Job job) {
        log.info("Executing job: jobId={}, name={}, artifactUri={}", job.getId(), job.getName(), job.getArtifactUri());

        Path inputDir = Path.of("/tmp/jobs", job.getId(), "input");
        Path outputDir = Path.of("/tmp/jobs", job.getId(), "output");

        try {
            Files.createDirectories(inputDir);
            Files.createDirectories(outputDir);

            stageInputFiles(job);

            StatusReporter reporter = openStatusStream(job.getId());

            // claimNextJob already transitions job to STARTING on the coordinator.
            // The worker reports RUNNING on the first RUNNING task update from the SDK.
            JobUpdateHandler updateHandler = new JobUpdateHandler(job.getId(), reporter);
            onStatusUpdate(updateHandler);

            ExecutionPayload executionPayload = new ExecutionPayload(workerAgentUrl(), job.getId(), job.getParamsMap());
            int jobMemoryMb = job.hasResources() ? job.getResources().getMemoryMb() : 0;
            int jobCpuCores = job.hasResources() ? job.getResources().getCpuCores() : 0;
            JobDetails details = new JobDetails(
                    job.getId(),
                    job.getArtifactUri(),
                    executionPayload.encode(),
                    jobMemoryMb,
                    jobCpuCores
            );

            Path logFile = Path.of("/tmp/jobs", job.getId(), "stdout.log");

            try {
                int exitCode = spawnJobProcess(details, inputDir, outputDir, logFile, job.getParamsMap());
                // Ensure the coordinator has seen RUNNING before we send a terminal
                // status — fast jobs can exit before the async task-status updates
                // are delivered, causing an illegal STARTING → COMPLETED transition.
                updateHandler.ensureRunning();
                if (exitCode == -1) {
                    reporter.report(StatusUpdate.jobUpdate(job.getId(), "KILLED",
                            FailureReason.PROCESS_TIMEOUT, jobExecutionTimeout.toString()));
                } else if (exitCode == 0) {
                    reporter.report(StatusUpdate.jobUpdate(job.getId(), "COMPLETED", null, null));
                } else {
                    log.warn("Job process exited with non-zero code: jobId={}, exitCode={}", job.getId(), exitCode);
                    reporter.report(StatusUpdate.jobUpdate(job.getId(), "FAILED",
                            FailureReason.PROCESS_EXITED, "exit code " + exitCode));
                }
            } catch (IOException | InterruptedException e) {
                log.error("Failed to execute job: jobId={}, error={}", job.getId(), e.getMessage(), e);
                reporter.report(StatusUpdate.jobUpdate(job.getId(), "FAILED",
                        FailureReason.PROCESS_START_FAILED, e.getMessage()));
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
            }

            uploadOutputs(job.getId(), outputDir, logFile);
        } catch (IOException e) {
            log.error("Failed to set up file staging for jobId={}: {}", job.getId(), e.getMessage(), e);
        } finally {
            cleanupTempDirs(job.getId());
            log.info("Finished job: jobId={}, name={}", job.getId(), job.getName());
        }
    }

    private void stageInputFiles(Job job) {
        if (objectStore == null) {
            return;
        }
        for (com.scheduler.proto.v1.InputFile inputFile : job.getInputFilesList()) {
            String uri = inputFile.getUri();
            Path dest = Path.of("/tmp/jobs", job.getId(), "input", inputFile.getName());
            log.info("Downloading input file: jobId={}, name={}, uri={}", job.getId(), inputFile.getName(), uri);
            objectStore.getObject(uri, dest);
        }
    }

    private void uploadOutputs(String jobId, Path outputDir, Path logFile) {
        if (objectStore == null) {
            return;
        }
        try (Stream<Path> files = Files.walk(outputDir)) {
            files.filter(Files::isRegularFile).forEach(file -> {
                String key = "jobs/" + jobId + "/output/" + outputDir.relativize(file);
                log.info("Uploading output file: jobId={}, key={}", jobId, key);
                objectStore.putObject(key, file);
            });
        } catch (IOException e) {
            log.error("Failed to upload output files for jobId={}: {}", jobId, e.getMessage(), e);
        }

        if (Files.exists(logFile)) {
            String key = "jobs/" + jobId + "/logs/stdout.log";
            log.info("Uploading stdout log: jobId={}, key={}", jobId, key);
            objectStore.putObject(key, logFile);
        }
    }

    private void cleanupTempDirs(String jobId) {
        Path jobDir = Path.of("/tmp/jobs", jobId);
        try (Stream<Path> walk = Files.walk(jobDir)) {
            walk.sorted(java.util.Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException e) {
                            log.warn("Failed to delete temp file {}: {}", p, e.getMessage());
                        }
                    });
        } catch (IOException e) {
            log.warn("Failed to clean up temp dir for jobId={}: {}", jobId, e.getMessage());
        }
    }

    /**
     * Spawns a child process for the job artifact with volume mounts for input/output,
     * tees stdout/stderr to a log file and the logger, then waits for exit.
     */
    int spawnJobProcess(JobDetails details, Path inputDir, Path outputDir, Path logFile,
                        Map<String, String> params) throws IOException, InterruptedException {
        List<String> command = buildCommand(details, inputDir, outputDir, params, dockerNetwork, mlflowTrackingUri);
        log.info("Starting job process: {}", String.join(" ", command));

        ProcessBuilder pb = new ProcessBuilder(command)
                .redirectErrorStream(true);

        Process process = pb.start();

        // Tee stdout/stderr to both log file and logger
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
             BufferedWriter logWriter = Files.newBufferedWriter(logFile)) {
            String line;
            while ((line = reader.readLine()) != null) {
                log.info("[job:{}] {}", details.jobId(), line);
                logWriter.write(line);
                logWriter.newLine();
            }
        }

        boolean finished = process.waitFor(jobExecutionTimeout.toMillis(), TimeUnit.MILLISECONDS);
        if (!finished) {
            log.warn("Job process timed out after {}: jobId={}", jobExecutionTimeout, details.jobId());
            process.destroyForcibly();
            // destroyForcibly kills the `docker run` CLI process but the container
            // keeps running. Force-remove it by name so nothing is left dangling.
            removeContainer(details.jobId());
            return -1;
        }

        int exitCode = process.exitValue();
        log.info("Job process finished: jobId={}, exitCode={}", details.jobId(), exitCode);
        return exitCode;
    }

    private static void removeContainer(String jobId) {
        String containerName = "job-" + jobId;
        try {
            Process rm = new ProcessBuilder("docker", "rm", "-f", "-v", containerName)
                    .redirectErrorStream(true)
                    .start();
            rm.getInputStream().readAllBytes();
            rm.waitFor(10, TimeUnit.SECONDS);
            log.info("Removed container: {}", containerName);
        } catch (Exception e) {
            log.warn("Failed to remove container {}: {}", containerName, e.getMessage());
        }
    }

    static List<String> buildCommand(JobDetails details, Path inputDir, Path outputDir,
                                      Map<String, String> params, String dockerNetwork,
                                      String mlflowTrackingUri) {
        List<String> command = new ArrayList<>();
        command.add("docker");
        command.add("run");
        command.add("--rm");
        command.add("--name");
        command.add("job-" + details.jobId());

        if (dockerNetwork != null) {
            command.add("--network");
            command.add(dockerNetwork);
        }

        command.add("-v");
        command.add(inputDir.toAbsolutePath() + ":/workspace/input:ro");
        command.add("-v");
        command.add(outputDir.toAbsolutePath() + ":/workspace/output");

        String containerPort = params.get("containerPort");
        if (containerPort != null) {
            command.add("-p");
            command.add("0:" + containerPort);
        }

        if (details.memoryMb() > 0) {
            command.add("--memory");
            command.add(details.memoryMb() + "m");
        }
        if (details.cpuCores() > 0) {
            command.add("--cpus");
            command.add(String.valueOf(details.cpuCores()));
        }

        command.add("-e");
        command.add("EXECUTION_PAYLOAD=" + details.payload());

        if (mlflowTrackingUri != null) {
            command.add("-e");
            command.add("MLFLOW_TRACKING_URI=" + mlflowTrackingUri);
        }

        command.add(details.artifactUri());
        return command;
    }

    // ── inbound: task status from JobProcess (in job process) → WebSocket → here ─

    /**
     * Returns the WebSocket URL that job processes use to send status updates.
     * Uses the real hostname (not localhost) so Docker containers on the bridge network
     * can route back to the worker.
     */
    public String workerAgentUrl() {
        return "ws://" + hostname + ":" + wsServer.getPort();
    }

    // WebSocket type tag — must match the SDK's framing constant.
    static final byte TYPE_TAG_STATUS = 0x01;

    /** Receives task status updates parsed from WebSocket binary frames. */
    @FunctionalInterface
    public interface StatusHandler {
        void handle(StatusUpdate update);
    }

    /**
     * Forwards all task updates to the coordinator. When a task reports RUNNING,
     * also sends a job-level RUNNING update — the coordinator ignores duplicates
     * if the job is already RUNNING.
     *
     * <p>Tracks whether RUNNING was sent so the worker can ensure the job
     * transitions through RUNNING before reporting a terminal state. Without
     * this, fast-completing jobs can race: the process exits before the async
     * gRPC stream delivers the RUNNING update, causing an illegal
     * STARTING → COMPLETED transition on the coordinator.
     */
    private static class JobUpdateHandler implements StatusHandler {
        private final String jobId;
        private final StatusReporter reporter;
        private volatile boolean runningSent;

        JobUpdateHandler(String jobId, StatusReporter reporter) {
            this.jobId = jobId;
            this.reporter = reporter;
        }

        @Override
        public void handle(StatusUpdate update) {
            if ("RUNNING".equals(update.taskStatus()) && !runningSent) {
                reporter.report(StatusUpdate.jobUpdate(jobId, "RUNNING", null, null));
                runningSent = true;
            }
            reporter.report(update);
        }

        /**
         * Ensures the coordinator has seen RUNNING before a terminal status is
         * sent. Called by executeJob just before reporting COMPLETED/FAILED/KILLED.
         */
        void ensureRunning() {
            if (!runningSent) {
                reporter.report(StatusUpdate.jobUpdate(jobId, "RUNNING", null, null));
                runningSent = true;
            }
        }
    }

    public void onStatusUpdate(StatusHandler handler) {
        wsServer.setStatusHandler(handler);
    }

    /**
     * WebSocket server that receives task-status messages from job processes over
     * a single persistent connection. Each binary frame starts with a one-byte
     * type tag ({@code 0x01} = status) followed by the proto payload.
     */
    private static class JobWebSocketServer extends WebSocketServer {

        private volatile StatusHandler statusHandler;
        private final CountDownLatch ready = new CountDownLatch(1);

        JobWebSocketServer(InetSocketAddress address) {
            super(address);
            setReuseAddr(true);
        }

        /** Blocks until the server thread has bound the socket and is accepting connections. */
        void awaitReady() throws InterruptedException {
            ready.await();
        }

        void setStatusHandler(StatusHandler handler) {
            this.statusHandler = handler;
        }

        @Override
        public void onOpen(WebSocket conn, ClientHandshake handshake) {
            log.info("WebSocket connection opened: remote={}", conn.getRemoteSocketAddress());
        }

        @Override
        public void onMessage(WebSocket conn, String message) {
            log.warn("Received unexpected text WebSocket message from {} — expected binary proto",
                    conn.getRemoteSocketAddress());
        }

        @Override
        public void onMessage(WebSocket conn, ByteBuffer buffer) {
            try {
                if (buffer.remaining() < 2) {
                    log.warn("Received too-short binary message ({} bytes) from {}",
                            buffer.remaining(), conn.getRemoteSocketAddress());
                    return;
                }

                byte typeTag = buffer.get();
                byte[] payload = new byte[buffer.remaining()];
                buffer.get(payload);

                if (typeTag == TYPE_TAG_STATUS) {
                    com.scheduler.proto.job.StatusUpdate proto =
                            com.scheduler.proto.job.StatusUpdate.parseFrom(payload);
                    StatusUpdate update = StatusUpdate.fromProto(proto);
                    log.info("Received status from JobProcess: jobId={}, taskIndex={}, taskName={}, status={}",
                            update.jobId(), update.taskIndex(), update.taskName(), update.taskStatus());
                    StatusHandler handler = statusHandler;
                    if (handler != null) {
                        handler.handle(update);
                    } else {
                        log.warn("No status handler registered, dropping update: jobId={}", update.jobId());
                    }
                } else {
                    log.warn("Unknown type tag 0x{} from {}",
                            String.format("%02x", typeTag), conn.getRemoteSocketAddress());
                }
            } catch (Exception e) {
                log.error("Failed to handle WebSocket message: {}", e.getMessage(), e);
            }
        }

        @Override
        public void onClose(WebSocket conn, int code, String reason, boolean remote) {
            log.info("WebSocket connection closed: remote={}, code={}, reason={}",
                    conn.getRemoteSocketAddress(), code, reason);
        }

        @Override
        public void onError(WebSocket conn, Exception ex) {
            log.error("WebSocket error: remote={}, error={}",
                    conn != null ? conn.getRemoteSocketAddress() : "null", ex.getMessage(), ex);
        }

        @Override
        public void onStart() {
            log.info("WebSocket server started on port {}", getPort());
            ready.countDown();
        }
    }

    // ── outbound: gRPC to coordinator ────────────────────────────────────────

    public String register(String hostname) {
        log.info("Registering with coordinator: hostname={}, memory={}, cpu={}, gpu={}, capabilities={}",
                hostname, memory, cpu, gpu, capabilities);
        RegisterWorkerResponse response = blockingStub.registerWorker(RegisterWorkerRequest.newBuilder()
                .setHostname(hostname)
                .setMemoryMb(memory)
                .setCpuCores(cpu)
                .setGpu(gpu)
                .addAllCapabilities(capabilities)
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
     * Opens a client-streaming pipe to the coordinator for streaming status updates.
     *
     * <p>Two observers are involved:
     * <ul>
     *   <li>{@code responseObserver} — handles what comes back from the coordinator.
     *       Since ReportStatus returns a single response only after the stream closes,
     *       onNext is empty; onCompleted/onError count down the latch so the caller
     *       (via {@link StatusReporter#awaitCompletion}) can wait for acknowledgment.</li>
     *   <li>{@code requestObserver} — the send side. Each {@code onNext()} pushes a status
     *       update to the coordinator. Wrapped inside {@link StatusReporter} for
     *       conversion to proto.</li>
     * </ul>
     */
    public StatusReporter openStatusStream(String jobId) {
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
        StreamObserver<com.scheduler.proto.job.StatusUpdate> requestObserver =
                asyncStub.reportStatus(responseObserver);

        return new StatusReporter(requestObserver, done);
    }

    // ── lifecycle ────────────────────────────────────────────────────────────

    private void startHeartbeat() {
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

    @Override
    public void close() throws Exception {
        stop();
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdown();
        }
        wsServer.stop();
        log.info("WebSocket server stopped");
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2 || !args[0].equals("--config")) {
            System.err.println("Usage: java -jar worker.jar --config <path>");
            System.exit(1);
        }

        WorkerConfig config = WorkerConfig.load(Path.of(args[1]));
        log.info("Loaded config from {}", args[1]);

        ObjectStore objectStore = createObjectStore(config.getMinio());

        WorkerAgent agent = new WorkerAgent(config, objectStore, DEFAULT_JOB_EXECUTION_TIMEOUT);
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

    private static ObjectStore createObjectStore(WorkerConfig.Minio minio) {
        S3Client s3 = S3Client.builder()
                .endpointOverride(URI.create(minio.getEndpoint()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(minio.getAccessKey(), minio.getSecretKey())))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .build();

        return new ObjectStore(s3, minio.getBucket());
    }
}
