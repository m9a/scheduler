package com.scheduler.worker;

import com.scheduler.core.ObjectStore;
import com.scheduler.proto.job.Report;
import com.scheduler.proto.job.StatusUpdate;
import com.scheduler.worker.persistence.SqliteWorkerStatusStore;
import com.scheduler.worker.persistence.WorkerStatusStore;
import com.scheduler.proto.v1.FailureReason;
import com.scheduler.proto.v1.Job;
import com.scheduler.proto.v1.JobState;
import com.scheduler.proto.v1.TaskState;
import com.scheduler.proto.worker.JobCommand;
import com.scheduler.proto.worker.SystemCommand;
import com.scheduler.worker.JobLauncher.ContainerState;
import com.scheduler.worker.WorkerRecovery.JobToReconcile;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The worker's main agent. It connects the two communication legs and drives
 * the job lifecycle.
 *
 * <pre>
 *                Job → Worker leg                Worker → Coordinator leg
 *                ────────────────                ────────────────────────
 *  Job container ──WebSocket──► JobCallbackHandler            CoordinatorClient ──gRPC──► Coordinator
 *    [0x01] task status              │                            ▲   register / pullJob / heartbeat
 *    [0x03] telemetry                │                            │   CoordinatorTelemetryStream (per job)
 *                                    ▼                            │
 *           (status handler stamps job RUNNING) ──► CoordinatorStatusStream (per job)
 * </pre>
 *
 * <p><b>What it owns:</b>
 * <ul>
 *   <li>{@link JobCallbackHandler} — receives status/telemetry from job containers.</li>
 *   <li>{@link CoordinatorClient} — all gRPC to the coordinator.</li>
 *   <li>{@link JobLauncher} — runs the job: docker run + file staging.</li>
 *   <li>{@link WorkerMetrics} — observes containers from outside (docker stats → /metrics).</li>
 *   <li>{@link WorkerStatusStore} — durable copy of every update sent; read back on boot.</li>
 * </ul>
 *
 * <p><b>Boot</b> ({@link #run()}):
 * <ul>
 *   <li>Resolve the stable worker id from the checkpoint file.</li>
 *   <li>Run boot recovery: which jobs was I running, are their containers alive?</li>
 *   <li>Register with those jobs; the coordinator answers which ones to kill.</li>
 *   <li>Reconcile: kill, re-attach, or fail each in-flight job.</li>
 *   <li>Enter the loop: pull job → execute → repeat.</li>
 * </ul>
 *
 * <p><b>Per job</b> ({@link #executeJob}): stage inputs → open streams → wire
 * handlers → run container → wait → report terminal state → upload outputs.
 *
 * <p>The coordinator is a passive state store. It applies what this agent sends.
 */
public class WorkerAgent implements AutoCloseable, ContainerInspector {

    private static final Logger log = LoggerFactory.getLogger(WorkerAgent.class);

    // Worker → Coordinator leg: all gRPC to the coordinator goes through this.
    private final CoordinatorClient coordinatorClient;

    // Job → Worker leg: WebSocket server receiving status/telemetry from job containers.
    private final JobCallbackHandler jobCallbacks;

    // Job execution: docker run + object-store file staging. No communication.
    private final JobLauncher launcher;

    // Observes job containers from outside (docker stats / nvidia-smi → /metrics).
    private final WorkerMetrics metrics;

    // Durable mirror of every status update this agent sends. Survives a worker
    // crash: recover() reads it on boot, register flushes it to the coordinator.
    private final WorkerStatusStore statusStore;

    // Boot recovery: reconciles in-flight jobs against their containers before register.
    private final WorkerRecovery recovery;

    // Path to the worker's checkpoint file (worker_checkpoint.yaml) — source of the stable workerId.
    private final String checkpointPath;

    // Resources advertised to the coordinator at registration.
    private final String hostname;
    private final int memory;
    private final int cpu;
    private final boolean gpu;
    private final Set<String> capabilities;

    private final JobLivenessMonitor.Config livenessConfig;
    // How often the heartbeat loop pings the coordinator.
    private final long heartbeatSendIntervalMs;
    // How long the idle loop sleeps between pullJob calls.
    private final long jobPullIntervalMs;
    private volatile boolean running;
    // Set by a drain command — the loop stops pulling new jobs while true.
    private volatile boolean draining;
    // Id of the job currently executing (null when idle) — lets a job command target it.
    private volatile String currentJobId;
    // Set on the WebSocket thread when a task reports FAILED; read at container
    // exit to fail the job even if the container exited 0. Reset per job in
    // openReportingChannel.
    private volatile boolean currentJobTaskFailed;
    private String workerId;

    public WorkerAgent(WorkerConfig config, ObjectStore objectStore) throws IOException {
        this(config, objectStore, new SqliteWorkerStatusStore(Path.of(config.getStatusDbPath())));
    }

    // Package-private so tests can inject an in-memory status store.
    WorkerAgent(WorkerConfig config, ObjectStore objectStore, WorkerStatusStore statusStore) throws IOException {
        this.statusStore = statusStore;
        // One retention sweep per boot bounds what a dead coordinator leaves behind.
        statusStore.prune(Duration.ofDays(config.getStatusRetentionDays()));
        this.coordinatorClient = new CoordinatorClient(
                config.getCoordinator().getHost(), config.getCoordinator().getPort());
        this.checkpointPath = config.getCheckpointPath();
        this.hostname = config.getHostname();
        this.memory = config.getResources().getMemory();
        this.cpu = config.getResources().getCpu();
        this.gpu = config.getResources().isGpu();
        this.capabilities = config.getResources().getCapabilities() == null
                ? Set.of()
                : Set.copyOf(config.getResources().getCapabilities());
        this.heartbeatSendIntervalMs = config.getCoordinator().getHeartbeatIntervalSeconds() * 1000L;
        this.jobPullIntervalMs = config.getCoordinator().getPollIntervalSeconds() * 1000L;
        WorkerConfig.Liveness liveness = config.getLiveness();
        this.livenessConfig = new JobLivenessMonitor.Config(
                liveness.getStartupTimeoutSeconds() * 1000L,
                liveness.getPingIntervalSeconds() * 1000L,
                liveness.getMaxMissedPings(),
                liveness.isAutoKill());
        this.launcher = new JobLauncher(objectStore,
                config.getDocker().getNetwork(), config.getMlflow().getTrackingUri(),
                liveness.getShutdownGraceSeconds(),
                Duration.ofMinutes(config.getDocker().getImagePullTimeoutMinutes()));
        // The agent is the inspector so tests can stub container state; the real
        // probe still lives in JobLauncher (see containerState below).
        this.recovery = new WorkerRecovery(statusStore, this);

        // Bind to all NICs. Containers on the bridge network reach this server via
        // the host's real hostname (passed in workerAgentUrl).
        this.jobCallbacks = new JobCallbackHandler(new InetSocketAddress("0.0.0.0", config.getPort()));
        this.jobCallbacks.start();
        try {
            this.jobCallbacks.awaitReady();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted waiting for WebSocket server to start", e);
        }
        log.info("WebSocket server listening on {}", workerAgentUrl());

        // /metrics for Prometheus (runs in the control plane's `metrics` compose
        // profile and scrapes this port). Always on. A failed bind (e.g. a second
        // worker on this host) must never take the worker down.
        this.metrics = new WorkerMetrics(config.getResources().isGpu());
        try {
            metrics.start(config.getMetricsPort());
        } catch (IOException e) {
            log.warn("Metrics endpoint unavailable — could not bind :{}: {}",
                    config.getMetricsPort(), e.getMessage());
        }
    }

    /**
     * Registers with the coordinator and enters the main loop:
     * pull job → execute → report status → repeat.
     * Blocks until {@link #stop()} is called or the thread is interrupted.
     *
     * <p><b>Limitation:</b> runs one job at a time — {@code executeJob} blocks on
     * the container. Concurrent jobs would need a bounded thread pool and a status
     * handler keyed by jobId instead of one replaced per job.
     */
    public void run() {
        // The checkpoint file holds the stable id (generated on first boot). The
        // same id survives restarts, so the coordinator can reconcile our jobs.
        workerId = WorkerCheckpoint.resolveOrCreate(java.nio.file.Path.of(checkpointPath));
        // Reconcile in-flight jobs against their containers before we register.
        List<JobToReconcile> jobsToReconcile = recovery.recover();
        // Register with the held jobs. The coordinator answers with the ones it
        // already marked terminal (heartbeat lost while this worker was down).
        // Their containers may still run here — the worker must kill them.
        List<StatusUpdate> knownJobs = jobsToReconcile.stream()
                .map(j -> jobUpdate(j.jobId(), j.jobState(), null, null))
                .toList();
        Set<String> jobIdsToKill = coordinatorClient.register(
                workerId, hostname, memory, cpu, gpu, capabilities, knownJobs);
        log.info("Worker running: workerId={}, hostname={}", workerId, hostname);

        coordinatorClient.startHeartbeat(workerId, heartbeatSendIntervalMs);
        // Open the coordinator → worker push channels (resync/drain, cancel/preempt).
        coordinatorClient.subscribeSystemCommands(workerId, this::onSystemCommand);
        coordinatorClient.subscribeJobCommands(workerId, this::onJobCommand);
        running = true;
        // Act on recovery before pulling new work: a container that survived the
        // restart is still this worker's job — resume watching it first.
        reconcileJobs(jobsToReconcile, jobIdsToKill);
        while (running) {
            Optional<Job> job = draining ? Optional.empty() : coordinatorClient.pullJob(workerId);
            if (job.isPresent()) {
                executeJob(job.get());
            } else {
                try {
                    Thread.sleep(jobPullIntervalMs);
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
     * The WebSocket URL job processes use to reach {@link JobCallbackHandler}.
     * Uses the real hostname (not localhost) so containers on the bridge network
     * can route back to the worker. Travels in EXECUTION_PAYLOAD.
     */
    public String workerAgentUrl() {
        return "ws://" + hostname + ":" + jobCallbacks.getPort();
    }

    /** Registers the per-job handler for task status arriving on the Job → Worker leg. */
    public void onStatusUpdate(JobCallbackHandler.StatusHandler handler) {
        jobCallbacks.setStatusHandler(handler);
    }

    // ── per-job orchestration ────────────────────────────────────────────────

    private void executeJob(Job job) {
        log.info("Executing job: jobId={}, name={}, artifactUri={}", job.getId(), job.getName(), job.getArtifactUri());

        Path inputDir = Path.of("/tmp/jobs", job.getId(), "input");
        Path outputDir = Path.of("/tmp/jobs", job.getId(), "output");
        Path logFile = Path.of("/tmp/jobs", job.getId(), "stdout.log");

        // Published so a cancel/preempt command can target the running job.
        currentJobId = job.getId();
        // First durable trace of ownership: the job entry, still STARTING (the
        // coordinator set that at claim). recover() finds it if we crash here.
        statusStore.update(jobUpdate(job.getId(), JobState.JOB_STATE_STARTING, null, null));
        try {
            Files.createDirectories(inputDir);
            Files.createDirectories(outputDir);
            launcher.stageInputFiles(job);
            runJobContainer(job, inputDir, outputDir, logFile);
            launcher.uploadOutputs(job.getId(), outputDir, logFile);
        } catch (IOException e) {
            log.error("Failed to set up file staging for jobId={}: {}", job.getId(), e.getMessage(), e);
        } finally {
            currentJobId = null;
            launcher.cleanupTempDirs(job.getId());
            log.info("Finished job: jobId={}, name={}", job.getId(), job.getName());
        }
    }

    // ── boot re-attach (recovery decisions) ─────────────────────────────────

    /**
     * Applies the recovery policy to each in-flight job. Fixed rules, no choices:
     * <ul>
     *   <li>Coordinator marked it terminal → {@link #killJob}. Never re-attach.</li>
     *   <li>Container running → {@link #recoverJob}: re-attach and watch to the end.</li>
     *   <li>Container exited or absent → {@link #failLostJob}: salvage, report FAILED.</li>
     * </ul>
     * Work done so far is always salvaged. Nothing is ever silently re-run.
     */
    private void reconcileJobs(List<JobToReconcile> jobsToReconcile, Set<String> jobIdsToKill) {
        for (JobToReconcile job : jobsToReconcile) {
            if (jobIdsToKill.contains(job.jobId())) {
                killJob(job.jobId(), job.containerState());
                continue;
            }
            switch (job.containerState()) {
                case RUNNING -> recoverJob(job.jobId());
                case EXITED, ABSENT -> failLostJob(job.jobId(), job.containerState());
            }
        }
    }

    /**
     * Kills a job the coordinator already marked dead (heartbeat lost — this
     * worker was down past {@code heartbeatTimeoutSeconds}). To this worker the
     * job looks alive; the coordinator's verdict wins.
     * <ul>
     *   <li>Stop the container. The coordinator's terminal state is final; the
     *       user may have already resubmitted the job.</li>
     *   <li>Salvage outputs and logs — the checkpoint is kept.</li>
     *   <li>Report once. The coordinator drops it (job already terminal), but
     *       its close ack clears this worker's store rows.</li>
     * </ul>
     */
    private void killJob(String jobId, ContainerState state) {
        log.warn("Job already terminal on the coordinator — killing its container: jobId={}, containerState={}",
                jobId, state);
        if (state == ContainerState.RUNNING) {
            launcher.stopContainer(jobId);
            state = ContainerState.EXITED;  // stopped just now; logs are salvageable
        }
        failLostJob(jobId, state);
    }

    /**
     * Best-effort recovery for a job whose container is no longer running: salvage
     * outputs and logs, report the job FAILED / NOT_FOUND_ON_RECOVERY, and clean
     * up. Deliberately imprecise — the worker does not try to reconstruct the
     * outcome; the user reads the job's checkpoint to decide progress, completion,
     * or the need to re-run. Both container states land here:
     * <ul>
     *   <li><b>EXITED</b> — the container finished while the worker was down (any
     *       exit code, or a liveness kill the worker never observed). Its logs are
     *       still salvageable; it is removed only after the coordinator acks.</li>
     *   <li><b>ABSENT</b> — the container is gone: it never launched (crash between
     *       claim and docker run), it was removed externally (docker rm / prune /
     *       daemon reset / host rebuild), or it finished and the worker crashed
     *       after removing it but before reporting the result.</li>
     * </ul>
     * TODO (TODO.md #24): differentiate these scenarios and recover the real
     * outcome (read the exit code for EXITED; persist the terminal state before
     * container removal) instead of the coarse FAILED.
     *
     * <p>Idempotent: rows leave the store only on the coordinator's ack, and the
     * exited container is removed only after that ack — so a crash mid-recovery
     * re-runs this path to the same result (re-reporting a terminal job is a
     * coordinator no-op).
     */
    private void failLostJob(String jobId, ContainerState state) {
        log.warn("Recovering lost job: jobId={}, containerState={} → FAILED / NOT_FOUND_ON_RECOVERY", jobId, state);
        Path outputDir = Path.of("/tmp/jobs", jobId, "output");
        Path logFile = Path.of("/tmp/jobs", jobId, "stdout.log");
        try {
            if (state == ContainerState.EXITED) {
                launcher.salvageLogs(jobId, logFile);
            }
            launcher.uploadOutputs(jobId, outputDir, logFile);

            // Status-only reporting: no telemetry stream or liveness monitor — the
            // container is not running and its SDK is gone.
            CoordinatorStatusStream statusStream = coordinatorClient.openStatusStream(jobId);
            StatusUpdate terminal = failedUpdate(jobId, FailureReason.FAILURE_REASON_NOT_FOUND_ON_RECOVERY,
                    "container " + (state == ContainerState.EXITED ? "exited" : "absent") + " on worker restart");
            statusStore.update(terminal);
            statusStream.report(terminal);
            statusStream.complete();
            if (awaitStatusClose(statusStream)) {
                statusStore.ack(jobId);
                // Remove only after the ack: a crash before this leaves the exited
                // container in place for the next recovery pass to re-report.
                if (state == ContainerState.EXITED) {
                    JobLauncher.removeContainer(jobId);
                }
            } else {
                log.warn("No close ack from coordinator for lost jobId={}; keeping status rows and container", jobId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted recovering lost jobId={}; the next boot re-runs recovery", jobId);
        } finally {
            launcher.cleanupTempDirs(jobId);
            log.info("Finished lost-job recovery: jobId={}", jobId);
        }
    }

    /**
     * Recovers a job whose container kept running across a worker restart. Same
     * reporting pipes and teardown as a fresh job, minus staging and docker run —
     * the container already exists, the agent re-attaches and waits for it to
     * finish. The liveness monitor starts fresh, so its startup window gives the
     * briefly-unwatched container time to ping again before it can be judged stalled.
     */
    private void recoverJob(String jobId) {
        log.info("Recovering job: jobId={}", jobId);
        Path outputDir = Path.of("/tmp/jobs", jobId, "output");
        Path logFile = Path.of("/tmp/jobs", jobId, "stdout.log");

        // Published so a cancel/preempt command can target the recovered job.
        currentJobId = jobId;
        try {
            JobReporting reporting = openReportingChannel(jobId);
            // The job's name is not persisted; the id stands in for the metrics label.
            metrics.jobStarted(jobId, jobId);
            // The container is running, so the job is RUNNING. Re-assert it: the
            // worker may have died before any task update, leaving the coordinator
            // at STARTING — which cannot go terminal directly. A coordinator that
            // already saw RUNNING de-dupes this to a no-op.
            StatusUpdate runningUpdate = jobUpdate(jobId, JobState.JOB_STATE_RUNNING, null, null);
            statusStore.update(runningUpdate);
            reporting.statusStream().report(runningUpdate);
            StatusUpdate terminal = reattachJob(jobId, logFile, reporting.liveness());
            reportTerminalUpdate(jobId, jobId, terminal, reporting);
            launcher.uploadOutputs(jobId, outputDir, logFile);
        } finally {
            currentJobId = null;
            launcher.cleanupTempDirs(jobId);
            log.info("Finished recovered job: jobId={}", jobId);
        }
    }

    /** True once a drain command has told this worker to stop pulling new jobs. */
    boolean isDraining() {
        return draining;
    }

    // ── coordinator → worker command dispatch (pushed on the command streams) ──

    private void onSystemCommand(SystemCommand command) {
        switch (command.getKindCase()) {
            case DRAIN -> {
                draining = command.getDrain().getDrain();
                log.info("Received drain command from coordinator: workerId={}, draining={}", workerId, draining);
            }
            case KIND_NOT_SET -> log.warn("Received empty SystemCommand from coordinator: workerId={}", workerId);
        }
    }

    private void onJobCommand(JobCommand command) {
        switch (command.getKindCase()) {
            case CANCEL -> stopRunningJob(command.getCancel().getJobId(), "cancel");
            case PREEMPT -> stopRunningJob(command.getPreempt().getJobId(), "preempt");
            case KIND_NOT_SET -> log.warn("Received empty JobCommand from coordinator: workerId={}", workerId);
        }
    }

    /**
     * Stops the container if the command targets the job this worker is running.
     * The container's exit then drives the terminal status the worker reports.
     */
    private void stopRunningJob(String jobId, String action) {
        String current = currentJobId;
        if (jobId.equals(current)) {
            log.info("Received {} command for running jobId={}; stopping container", action, jobId);
            launcher.stopContainer(jobId);
        } else {
            log.warn("Ignoring {} command for jobId={}: not the job this worker is running (current={})",
                    action, jobId, current);
        }
    }

    /** The per-job reporting pipes: the two coordinator streams and the liveness monitor. */
    private record JobReporting(CoordinatorStatusStream statusStream,
                                CoordinatorTelemetryStream telemetryStream,
                                JobLivenessMonitor liveness) {}

    /** Opens the reporting pipes, runs the container, sends the terminal update, closes the pipes. */
    private void runJobContainer(Job job, Path inputDir, Path outputDir, Path logFile) {
        JobReporting reporting = openReportingChannel(job.getId());
        metrics.jobStarted(job.getId(), job.getName());
        StatusUpdate terminal = spawnJob(job, inputDir, outputDir, logFile,
                reporting.liveness());
        reportTerminalUpdate(job.getId(), job.getName(), terminal, reporting);
    }

    /** Persists and sends the terminal update, then tears down the reporting pipes. */
    private void reportTerminalUpdate(String jobId, String jobName, StatusUpdate terminal,
                                      JobReporting reporting) {
        try {
            statusStore.update(terminal);
            reporting.statusStream().report(terminal);
        } finally {
            closeReportingChannel(jobId, jobName, terminal.getJobState(), reporting);
        }
    }

    /**
     * Opens the two per-job coordinator streams, starts the liveness monitor, and
     * binds the WebSocket handlers to them. The status handler stamps job RUNNING
     * onto each task update — the SDK only knows task state. The coordinator moves
     * STARTING → RUNNING on the first and de-dupes the rest.
     */
    private JobReporting openReportingChannel(String jobId) {
        // Fresh per-job state before the container runs.
        currentJobTaskFailed = false;

        CoordinatorStatusStream statusStream = coordinatorClient.openStatusStream(jobId);
        onStatusUpdate(update -> relayTaskStatus(statusStream, update));

        CoordinatorTelemetryStream telemetryStream = coordinatorClient.openTelemetryStream(jobId);

        // KILL PATH 1 — liveness, purely worker-local. Every inbound SDK frame
        // bumps the monitor (via the activity listener); a container silent past
        // the thresholds is gracefully stopped. The monitor also owns the one
        // liveness clock the worker stamps onto telemetry — so it is created
        // before the report handler that reads it.
        JobLivenessMonitor liveness = new JobLivenessMonitor(jobId,
                livenessConfig, () -> launcher.stopContainer(jobId));
        jobCallbacks.setActivityListener(liveness::recordActivity);

        // Telemetry: forward each SDK Report, re-stamped with the monitor's last
        // liveness time (see relayTelemetry). The activity listener above runs
        // before this handler on each frame, so that time is this report's arrival.
        jobCallbacks.setReportHandler(report -> relayTelemetry(telemetryStream, liveness, report));

        liveness.start();

        return new JobReporting(statusStream, telemetryStream, liveness);
    }

    /** Unbinds the WebSocket handlers, then closes the monitor and both streams. */
    private void closeReportingChannel(String jobId, String jobName, JobState terminalState, JobReporting reporting) {
        reporting.liveness().close();
        jobCallbacks.setActivityListener(null);
        // Stop routing telemetry before closing the stream. A late frame after
        // this is dropped — telemetry is lossy by design.
        jobCallbacks.setReportHandler(null);
        reporting.telemetryStream().complete();
        awaitTelemetryClose(reporting.telemetryStream());
        metrics.jobFinished(jobId, jobName, metricsOutcomeLabel(terminalState));
        reporting.statusStream().complete();
        // The coordinator's close ack confirms it saw the terminal update — the
        // one coarse ack per job. Only then do the job's rows leave the store.
        if (awaitStatusClose(reporting.statusStream())) {
            statusStore.ack(jobId);
        } else {
            log.warn("No close ack from coordinator for jobId={}; keeping status rows for the register flush",
                    jobId);
        }
    }

    /** Bounded wait for the close ack; never blocks job teardown for long. */
    private static void awaitTelemetryClose(CoordinatorTelemetryStream telemetryStream) {
        try {
            telemetryStream.awaitCompletion(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Bounded wait for the close ack; never blocks job teardown for long. */
    private static boolean awaitStatusClose(CoordinatorStatusStream statusStream) {
        try {
            return statusStream.awaitCompletion(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Launches the container, blocks until it exits, and builds the terminal job
     * update. The one early-kill path is the liveness check (see
     * {@link #openReportingChannel}); there is no run deadline. A task still
     * mid-execution keeps its last reported state — the worker does not fail it.
     */
    private StatusUpdate spawnJob(Job job, Path inputDir, Path outputDir, Path logFile,
                                  JobLivenessMonitor liveness) {
        String jobId = job.getId();
        int exitCode;
        try {
            exitCode = spawnJobProcess(jobDetails(job), inputDir, outputDir, logFile, job.getParamsMap());
        } catch (IOException | InterruptedException e) {
            log.error("Failed to execute job: jobId={}, error={}", jobId, e.getMessage(), e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return failedUpdate(jobId, FailureReason.FAILURE_REASON_PROCESS_START_FAILED, e.getMessage());
        }
        return getTerminalUpdateFromExit(jobId, exitCode, liveness);
    }

    /**
     * Re-attach counterpart of {@link #spawnJob}: re-attaches to a
     * container recovery found still running, blocks until the job finishes, and
     * maps its exit the same way.
     */
    private StatusUpdate reattachJob(String jobId, Path logFile, JobLivenessMonitor liveness) {
        int exitCode;
        try {
            exitCode = attachJobProcess(jobId, logFile);
        } catch (IOException | InterruptedException e) {
            log.error("Failed to re-attach to job: jobId={}, error={}", jobId, e.getMessage(), e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return failedUpdate(jobId, FailureReason.FAILURE_REASON_PROCESS_START_FAILED,
                    "re-attach failed: " + e.getMessage());
        }
        return getTerminalUpdateFromExit(jobId, exitCode, liveness);
    }

    /** Maps the container's exit to the terminal job update. */
    private StatusUpdate getTerminalUpdateFromExit(String jobId, int exitCode, JobLivenessMonitor liveness) {
        // KILL PATH — liveness. The monitor stopped a silent container; its
        // verdict beats the kill-induced exit code.
        if (liveness.isUnresponsive()) {
            WorkerMetrics.JOBS_KILLED_UNRESPONSIVE.inc();
            return killedUpdate(jobId, FailureReason.FAILURE_REASON_UNRESPONSIVE,
                    "no liveness from the container");
        }
        // A task that reported FAILED fails the whole job, even if the container
        // then exited 0 (e.g. the SDK caught the error during shutdown). Any task
        // failure stops the job — see CLAUDE.md "State model".
        if (currentJobTaskFailed && exitCode == JobLauncher.EXIT_SUCCESS) {
            log.warn("Job container exited 0 but a task reported FAILED: jobId={}", jobId);
            return failedUpdate(jobId, FailureReason.FAILURE_REASON_PROCESS_EXITED, "a task reported FAILED");
        }
        if (exitCode == JobLauncher.EXIT_SUCCESS) {
            return completedUpdate(jobId);
        }
        log.warn("Job process exited with non-zero code: jobId={}, exitCode={}", jobId, exitCode);
        return failedUpdate(jobId, FailureReason.FAILURE_REASON_PROCESS_EXITED, "exit code " + exitCode);
    }

    private JobDetails jobDetails(Job job) {
        ExecutionPayload payload = new ExecutionPayload(workerAgentUrl(), job.getId(), job.getParamsMap());
        int memoryMb = job.hasResources() ? job.getResources().getMemoryMb() : 0;
        int cpuCores = job.hasResources() ? job.getResources().getCpuCores() : 0;
        return new JobDetails(job.getId(), job.getArtifactUri(), payload.encode(), memoryMb, cpuCores);
    }

    /** Maps a terminal job state to its Prometheus job-outcome label. */
    private static String metricsOutcomeLabel(JobState terminalState) {
        return switch (terminalState) {
            case JOB_STATE_COMPLETED -> "completed";
            case JOB_STATE_KILLED -> "killed";
            default -> "failed";
        };
    }

    /**
     * Stamps job RUNNING onto a task update from the SDK and forwards it to the
     * coordinator. Registered as the per-job status handler in
     * {@link #openReportingChannel}. Persists before sending: a crash between the two
     * re-asserts the update on the next register instead of losing it.
     *
     * <p>Job stays RUNNING even on a task FAILED — the job is still alive at that
     * instant; it goes terminal only at container exit. The failure is remembered
     * ({@code currentJobTaskFailed}) so {@link #getTerminalUpdateFromExit} fails
     * the job even if the container then exits 0.
     */
    private void relayTaskStatus(CoordinatorStatusStream statusStream, StatusUpdate update) {
        if (update.getTaskState() == TaskState.TASK_STATE_FAILED) {
            currentJobTaskFailed = true;
        }
        StatusUpdate stamped = update.toBuilder().setJobState(JobState.JOB_STATE_RUNNING).build();
        statusStore.update(stamped);
        statusStream.report(stamped);
    }

    /**
     * Re-stamps a telemetry {@link Report} with the job's last liveness time — the
     * marker the coordinator surfaces on {@code GetJobStatus} — then forwards it.
     * Registered as the per-job report handler in {@link #openReportingChannel}.
     * The value is the {@link JobLivenessMonitor}'s {@code lastLivenessAt}, the one
     * liveness clock the worker owns; the SDK's own timestamp is not trusted here.
     *
     * <p>TODO (TODO.md #23): forward on a fixed cadence instead of on arrival,
     * aggregating buffered reports and stamping one liveness time per flush.
     */
    private void relayTelemetry(CoordinatorTelemetryStream telemetryStream, JobLivenessMonitor liveness,
                                Report report) {
        telemetryStream.report(report.toBuilder().setTimestampMs(liveness.lastLivenessAt()).build());
    }

    // ── job-level status updates ─────────────────────────────────────────────

    private static StatusUpdate completedUpdate(String jobId) {
        return jobUpdate(jobId, JobState.JOB_STATE_COMPLETED, null, null);
    }

    private static StatusUpdate failedUpdate(String jobId, FailureReason reason, String detail) {
        return jobUpdate(jobId, JobState.JOB_STATE_FAILED, reason, detail);
    }

    private static StatusUpdate killedUpdate(String jobId, FailureReason reason, String detail) {
        return jobUpdate(jobId, JobState.JOB_STATE_KILLED, reason, detail);
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
     * Package-private so tests can override it and simulate jobs without docker.
     */
    int spawnJobProcess(JobDetails details, Path inputDir, Path outputDir, Path logFile,
                        Map<String, String> params) throws IOException, InterruptedException {
        return launcher.spawn(details, inputDir, outputDir, logFile, params);
    }

    /**
     * Re-attaches to a recovered container via {@link JobLauncher} and blocks until
     * exit. Package-private so tests can override it and simulate the resumed wait.
     */
    int attachJobProcess(String jobId, Path logFile) throws IOException, InterruptedException {
        return launcher.attachAndWait(jobId, logFile);
    }

    /**
     * Boot recovery's container probe — the real read is {@link JobLauncher}'s
     * docker inspect. On the agent so tests can stub container state.
     */
    @Override
    public ContainerState containerState(String jobId) {
        return launcher.containerState(jobId);
    }

    // ── lifecycle ────────────────────────────────────────────────────────────

    @Override
    public void close() throws Exception {
        stop();
        metrics.stop();
        jobCallbacks.stop();
        log.info("WebSocket server stopped");
        coordinatorClient.close();
        statusStore.close();
    }

    public static void main(String[] args) throws IOException {
        String configPath = System.getenv("WORKER_CONFIG");
        if (configPath == null || configPath.isBlank()) {
            System.err.println("WORKER_CONFIG must point to the worker config file");
            System.exit(1);
            return;
        }

        WorkerConfig config;
        try {
            config = WorkerConfig.load(Path.of(configPath));
            config.validate();
        } catch (Exception e) {
            System.err.println("Failed to load WORKER_CONFIG=" + configPath + ": " + e.getMessage());
            System.exit(1);
            return;
        }
        log.info("Loaded config from WORKER_CONFIG={}", configPath);

        ObjectStore objectStore = createObjectStore(config.getMinio());

        WorkerAgent agent = new WorkerAgent(config, objectStore);
        registerShutdownHook(agent);
        agent.run();
    }

    // TODO: graceful shutdown — drain first (stop pulling, let the running job and
    // its streams finish or hand the job back), then close. Today this closes
    // everything at once; see TODO.md #16 (worker drain/drained state machine).
    private static void registerShutdownHook(WorkerAgent agent) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down worker");
            try {
                agent.close();
            } catch (Exception e) {
                log.error("Error shutting down worker: {}", e.getMessage());
            }
        }, "worker-shutdown"));
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
