package com.scheduler.worker;

import com.scheduler.core.ObjectStore;
import com.scheduler.proto.job.StatusUpdate;
import com.scheduler.proto.v1.FailureReason;
import com.scheduler.proto.v1.Job;
import com.scheduler.proto.v1.JobState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The worker's main agent — the orchestrator that wires the two communication
 * legs together and drives the job lifecycle. The coordinator is a passive
 * state store that applies whatever this agent sends.
 *
 * <p>Each leg is owned by one class; this class only connects them:
 * <pre>
 *                Job → Worker leg                Worker → Coordinator leg
 *                ────────────────                ────────────────────────
 *  Job container ──WebSocket──► JobCallbackServer            CoordinatorClient ──gRPC──► Coordinator
 *    [0x01] task status              │                            ▲   register / pullJob / heartbeat
 *    [0x03] telemetry                │                            │   forwardTelemetry (as-is)
 *                                    ▼                            │
 *           (status handler stamps job RUNNING) ──► CoordinatorStatusStream (per job)
 *
 *  WorkerAgent also runs the job itself: JobLauncher (docker run + file staging)
 *  and observes it from outside: WorkerMetrics (docker stats → /metrics).
 * </pre>
 *
 * <p><b>Main loop</b> ({@link #run()}): register → pull job → execute → repeat.
 * Per job ({@link #executeJob}): stage inputs → open status stream → wire handler
 * → spawn container → wait → report COMPLETED/FAILED/KILLED (TIMEOUT → KILLED
 * on deadline) → upload outputs.
 */
public class WorkerAgent implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WorkerAgent.class);
    private static final long POLL_INTERVAL_MS = 5000;
    private static final Duration DEFAULT_JOB_EXECUTION_TIMEOUT = Duration.ofMinutes(10);
    // Fixed Prometheus port, matching metrics/prometheus.yml in the scheduler repo.
    static final int METRICS_PORT = 9092;

    // Worker → Coordinator leg: all gRPC to the coordinator goes through this.
    private final CoordinatorClient coordinator;

    // Job → Worker leg: WebSocket server receiving status/telemetry from job containers.
    private final JobCallbackServer jobCallbacks;

    // Job execution: docker run + object-store file staging. No communication.
    private final JobLauncher launcher;

    // Observes job containers from outside (docker stats / nvidia-smi → /metrics).
    private final WorkerMetrics metrics;

    // Resources advertised to the coordinator at registration.
    private final String hostname;
    private final int memory;
    private final int cpu;
    private final boolean gpu;
    private final Set<String> capabilities;

    private final Duration jobExecutionTimeout;
    private volatile boolean running;
    private String workerId;

    public WorkerAgent(WorkerConfig config, ObjectStore objectStore,
                       Duration jobExecutionTimeout) throws IOException {
        this.coordinator = new CoordinatorClient(
                config.getCoordinator().getHost(), config.getCoordinator().getPort());
        this.hostname = config.getHostname();
        this.memory = config.getResources().getMemory();
        this.cpu = config.getResources().getCpu();
        this.gpu = config.getResources().isGpu();
        this.capabilities = config.getResources().getCapabilities() == null
                ? Set.of()
                : Set.copyOf(config.getResources().getCapabilities());
        this.jobExecutionTimeout = jobExecutionTimeout;
        this.launcher = new JobLauncher(objectStore, jobExecutionTimeout,
                config.getDocker().getNetwork(), config.getMlflow().getTrackingUri());

        // Bind to all NICs so Docker containers on the bridge network can reach
        // this server via the host's real hostname (passed in workerAgentUrl).
        this.jobCallbacks = new JobCallbackServer(new InetSocketAddress("0.0.0.0", config.getPort()));
        this.jobCallbacks.start();
        try {
            this.jobCallbacks.awaitReady();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted waiting for WebSocket server to start", e);
        }
        // Telemetry needs no per-job state — forward each Report to the coordinator
        // as-is, so this handler is wired once for the worker's lifetime. The status
        // handler, by contrast, is swapped in per job (see executeJob).
        this.jobCallbacks.setReportHandler(coordinator::forwardTelemetry);
        log.info("WebSocket server listening on {}", workerAgentUrl());

        // Always serve /metrics on the fixed port — whether anything scrapes it is
        // decided by the control-plane config (the `metrics` compose profile), not
        // per-worker config. A failed bind (e.g. a second worker on this host) must
        // never take the worker down, but say why.
        this.metrics = new WorkerMetrics(config.getResources().isGpu());
        try {
            metrics.start(METRICS_PORT);
        } catch (IOException e) {
            log.warn("Metrics endpoint disabled — could not bind :{}: {}", METRICS_PORT, e.getMessage());
        }
    }

    /**
     * Registers with the coordinator and enters the main loop:
     * pull job → execute → report status → repeat.
     * Blocks until {@link #stop()} is called or the thread is interrupted.
     *
     * <p><b>Limitation:</b> runs one job at a time because {@code executeJob} blocks
     * on the container process. To run multiple jobs concurrently, submit
     * {@code executeJob} calls to a bounded thread pool. That also requires
     * multiplexing the status handler by jobId instead of replacing it per job.
     */
    public void run() {
        workerId = coordinator.register(hostname, memory, cpu, gpu, capabilities);
        log.info("Worker running: workerId={}, hostname={}", workerId, hostname);

        coordinator.startHeartbeat(workerId);
        running = true;
        while (running) {
            Optional<Job> job = coordinator.pullJob(workerId);
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

    /**
     * Returns the WebSocket URL job processes use to reach {@link JobCallbackServer}.
     * Uses the real hostname (not localhost) so Docker containers on the bridge
     * network can route back to the worker; passed in via EXECUTION_PAYLOAD.
     */
    public String workerAgentUrl() {
        return "ws://" + hostname + ":" + jobCallbacks.getPort();
    }

    /** Registers the per-job handler for task status arriving on the Job → Worker leg. */
    public void onStatusUpdate(JobCallbackServer.StatusHandler handler) {
        jobCallbacks.setStatusHandler(handler);
    }

    // ── per-job orchestration ────────────────────────────────────────────────

    private void executeJob(Job job) {
        log.info("Executing job: jobId={}, name={}, artifactUri={}", job.getId(), job.getName(), job.getArtifactUri());

        Path inputDir = Path.of("/tmp/jobs", job.getId(), "input");
        Path outputDir = Path.of("/tmp/jobs", job.getId(), "output");
        Path logFile = Path.of("/tmp/jobs", job.getId(), "stdout.log");

        try {
            Files.createDirectories(inputDir);
            Files.createDirectories(outputDir);
            launcher.stageInputFiles(job);
            runJobContainer(job, inputDir, outputDir, logFile);
            launcher.uploadOutputs(job.getId(), outputDir, logFile);
        } catch (IOException e) {
            log.error("Failed to set up file staging for jobId={}: {}", job.getId(), e.getMessage(), e);
        } finally {
            launcher.cleanupTempDirs(job.getId());
            log.info("Finished job: jobId={}, name={}", job.getId(), job.getName());
        }
    }

    /**
     * Opens the per-job status stream, runs the container, reports the one terminal
     * job update its outcome implies, then finalizes the stream. The status handler
     * stamps job RUNNING onto each task update the container sends — the SDK only
     * knows task state; the coordinator moves STARTING → RUNNING on the first and
     * de-dupes the rest.
     */
    private void runJobContainer(Job job, Path inputDir, Path outputDir, Path logFile) {
        CoordinatorStatusStream statusStream = coordinator.openStatusStream(job.getId());
        onStatusUpdate(update -> statusStream.report(
                update.toBuilder().setJobState(JobState.JOB_STATE_RUNNING).build()));

        metrics.jobStarted(job.getId(), job.getName());
        StatusUpdate terminal = awaitContainerOutcome(job, inputDir, outputDir, logFile, statusStream);
        try {
            statusStream.report(terminal);
        } finally {
            metrics.jobFinished(job.getId(), job.getName(), outcomeLabel(terminal.getJobState()));
            statusStream.complete();
            awaitStreamClose(statusStream);
        }
    }

    /**
     * Runs the container to exit and maps the result to its terminal job update:
     * exit 0 → COMPLETED, deadline kill (-1) → KILLED, any other exit → FAILED, a
     * spawn failure → FAILED. On the deadline the launcher's onTimeout hook reports
     * TIMEOUT first (kill in flight); KILLED follows here. A task still
     * mid-execution keeps its last reported state — the worker does not fail it.
     */
    private StatusUpdate awaitContainerOutcome(Job job, Path inputDir, Path outputDir, Path logFile,
                                               CoordinatorStatusStream statusStream) {
        String jobId = job.getId();
        try {
            int exitCode = spawnJobProcess(jobDetails(job), inputDir, outputDir, logFile, job.getParamsMap(), () ->
                    statusStream.report(jobUpdate(jobId, JobState.JOB_STATE_TIMEOUT,
                            FailureReason.FAILURE_REASON_PROCESS_TIMEOUT, jobExecutionTimeout.toString())));
            return switch (exitCode) {
                case -1 -> jobUpdate(jobId, JobState.JOB_STATE_KILLED,
                        FailureReason.FAILURE_REASON_PROCESS_TIMEOUT, jobExecutionTimeout.toString());
                case 0 -> jobUpdate(jobId, JobState.JOB_STATE_COMPLETED, null, null);
                default -> {
                    log.warn("Job process exited with non-zero code: jobId={}, exitCode={}", jobId, exitCode);
                    yield jobUpdate(jobId, JobState.JOB_STATE_FAILED,
                            FailureReason.FAILURE_REASON_PROCESS_EXITED, "exit code " + exitCode);
                }
            };
        } catch (IOException | InterruptedException e) {
            log.error("Failed to execute job: jobId={}, error={}", jobId, e.getMessage(), e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return jobUpdate(jobId, JobState.JOB_STATE_FAILED,
                    FailureReason.FAILURE_REASON_PROCESS_START_FAILED, e.getMessage());
        }
    }

    private JobDetails jobDetails(Job job) {
        ExecutionPayload payload = new ExecutionPayload(workerAgentUrl(), job.getId(), job.getParamsMap());
        int memoryMb = job.hasResources() ? job.getResources().getMemoryMb() : 0;
        int cpuCores = job.hasResources() ? job.getResources().getCpuCores() : 0;
        return new JobDetails(job.getId(), job.getArtifactUri(), payload.encode(), memoryMb, cpuCores);
    }

    /** Prometheus outcome label for a terminal job state. */
    private static String outcomeLabel(JobState terminalState) {
        return switch (terminalState) {
            case JOB_STATE_COMPLETED -> "completed";
            case JOB_STATE_KILLED -> "killed";
            default -> "failed";
        };
    }

    private void awaitStreamClose(CoordinatorStatusStream statusStream) {
        try {
            statusStream.awaitCompletion(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Builds a job-level {@link StatusUpdate} proto (no task section). */
    private static StatusUpdate jobUpdate(String jobId, JobState state, FailureReason reason, String detail) {
        StatusUpdate.Builder builder = StatusUpdate.newBuilder().setJobId(jobId).setJobState(state);
        if (reason != null) {
            builder.setFailureReason(reason);
        }
        if (detail != null) {
            builder.setFailureDetail(detail);
        }
        return builder.build();
    }

    /**
     * Runs the job container via {@link JobLauncher} and blocks until exit.
     * {@code onTimeout} fires when the execution deadline is hit, before the
     * kill starts (see {@link JobLauncher#spawn}). Package-private override
     * seam: tests subclass the agent and replace this to simulate job
     * processes without docker.
     */
    int spawnJobProcess(JobDetails details, Path inputDir, Path outputDir, Path logFile,
                        Map<String, String> params, Runnable onTimeout) throws IOException, InterruptedException {
        return launcher.spawn(details, inputDir, outputDir, logFile, params, onTimeout);
    }

    // ── lifecycle ────────────────────────────────────────────────────────────

    @Override
    public void close() throws Exception {
        stop();
        metrics.stop();
        jobCallbacks.stop();
        log.info("WebSocket server stopped");
        coordinator.close();
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
