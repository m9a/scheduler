package com.scheduler.coordinator.worker;

import com.scheduler.coordinator.CoordinatorMetrics;
import com.scheduler.coordinator.JobManager;
import com.scheduler.coordinator.ProtoMapper;
import com.scheduler.coordinator.persistence.WorkerStore;
import com.scheduler.core.FailureMessages;
import com.scheduler.core.JobStatus;
import com.scheduler.core.WorkerInfo;
import com.scheduler.proto.job.Report;
import com.scheduler.proto.job.StatusUpdate;
import com.scheduler.proto.v1.FailureReason;
import com.scheduler.proto.v1.JobState;
import com.scheduler.proto.v1.TaskState;
import com.scheduler.proto.worker.HeartbeatRequest;
import com.scheduler.proto.worker.HeartbeatResponse;
import com.scheduler.proto.worker.PullJobRequest;
import com.scheduler.proto.worker.PullJobResponse;
import com.scheduler.proto.worker.RegisterWorkerRequest;
import com.scheduler.proto.worker.RegisterWorkerResponse;
import com.scheduler.proto.worker.ReportTelemetryResponse;
import com.scheduler.proto.worker.StatusUpdateResponse;
import com.scheduler.proto.worker.SubscribeRequest;
import com.scheduler.proto.worker.SystemCommand;
import com.scheduler.proto.worker.JobCommand;
import com.scheduler.proto.worker.Resync;
import com.scheduler.proto.worker.Drain;
import com.scheduler.proto.worker.Cancel;
import com.scheduler.proto.worker.Preempt;
import com.scheduler.proto.worker.WorkerServiceGrpc;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Optional;
import java.util.UUID;

/**
 * <b>Worker → Coordinator edge.</b> Implements the WorkerService gRPC API that
 * workers (the other side is the worker's {@code CoordinatorClient}) call into.
 * Pure transport: unpacks protos via {@link ProtoMapper}, delegates to
 * {@link JobManager} (job state) and {@link WorkerRegistry} (worker liveness),
 * and packs the replies — no scheduling or lifecycle logic of its own.
 *
 * <pre>
 * Worker ──gRPC──► WorkerHandler ──► JobManager        WorkerRegistry
 *   RegisterWorker                                       register
 *   PullJob                          claimNextJob        find
 *   ReportStatus (stream)            handleStatusUpdate
 *   ReportTelemetry                  handleReport
 *   Heartbeat                                            updateHeartbeat
 * </pre>
 */
public class WorkerHandler extends WorkerServiceGrpc.WorkerServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(WorkerHandler.class);

    private final JobManager jobManager;
    private final WorkerRegistry workers;
    // Open coordinator → worker push streams, keyed by worker id.
    private final WorkerCommandStreams commandStreams = new WorkerCommandStreams();

    public WorkerHandler(JobManager jobManager, WorkerStore workerStore) {
        this.jobManager = jobManager;
        this.workers = new WorkerRegistry(workerStore);
    }

    @Override
    public void registerWorker(RegisterWorkerRequest request, StreamObserver<RegisterWorkerResponse> responseObserver) {
        // Worker owns its identity (stable across restarts, see task #14). Fall back to
        // a generated id only if an older worker sends none — and log it as anomalous.
        String workerId = request.getWorkerId();
        if (workerId == null || workerId.isBlank()) {
            workerId = UUID.randomUUID().toString();
            log.warn("registerWorker from hostname={} sent no worker_id; generated {}",
                    request.getHostname(), workerId);
        }
        com.scheduler.proto.v1.ResourceRequirements resources = request.getResources();
        log.info("Received registerWorker from hostname={}, memoryMb={}, cpuCores={}, gpu={}, workerId={}",
                request.getHostname(), resources.getMemoryMb(), resources.getCpuCores(), resources.getGpu(), workerId);
        workers.register(new WorkerInfo(
                workerId,
                request.getHostname(),
                resources.getMemoryMb(),
                resources.getCpuCores(),
                resources.getGpu(),
                new HashSet<>(resources.getCapabilitiesList()),
                Instant.now(),
                Instant.now()
        ));

        responseObserver.onNext(RegisterWorkerResponse.newBuilder()
                .setWorkerId(workerId)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void pullJob(PullJobRequest request, StreamObserver<PullJobResponse> responseObserver) {
        log.info("Received pullJob from workerId={}", request.getWorkerId());
        Optional<WorkerInfo> worker = workers.find(request.getWorkerId());
        if (worker.isEmpty()) {
            log.warn("Unknown workerId={} in pullJob", request.getWorkerId());
            responseObserver.onNext(PullJobResponse.getDefaultInstance());
            responseObserver.onCompleted();
            return;
        }

        Optional<JobStatus> claimed = jobManager.claimNextJob(worker.get());

        PullJobResponse.Builder builder = PullJobResponse.newBuilder();
        if (claimed.isPresent()) {
            log.info("Assigned jobId={} to workerId={}", claimed.get().id(), request.getWorkerId());
            builder.setJob(ProtoMapper.toProto(claimed.get(), jobManager.lastActivity(claimed.get().id())));
        } else {
            log.debug("No jobs available for workerId={}", request.getWorkerId());
        }

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<StatusUpdate> reportStatus(
            StreamObserver<StatusUpdateResponse> responseObserver) {
        return new StreamObserver<>() {
            @Override
            public void onNext(StatusUpdate update) {
                if (update.getJobState() != JobState.JOB_STATE_UNSPECIFIED) {
                    log.info("Received job status update: jobId={}, jobState={}{}",
                            update.getJobId(), update.getJobState(),
                            update.getFailureReason() != FailureReason.FAILURE_REASON_UNSPECIFIED
                                    ? ", reason=" + FailureMessages.format(update.getFailureReason(), update.getFailureDetail()) : "");
                }
                if (update.getTaskState() != TaskState.TASK_STATE_UNSPECIFIED) {
                    log.info("Received task status update: jobId={}, taskIndex={}, taskName={}, status={}{}",
                            update.getJobId(), update.getTaskIndex(), update.getTaskName(), update.getTaskState(),
                            update.getErrorMessage().isEmpty() ? "" : ", error=" + update.getErrorMessage());
                }
                jobManager.handleStatusUpdate(update);
            }

            @Override
            public void onError(Throwable t) {
                log.error("ReportStatus stream error: {}", t.getMessage());
            }

            @Override
            public void onCompleted() {
                responseObserver.onNext(StatusUpdateResponse.getDefaultInstance());
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public StreamObserver<Report> reportTelemetry(StreamObserver<ReportTelemetryResponse> responseObserver) {
        // One telemetry stream per job (the worker's CoordinatorTelemetryStream). Each
        // Report is applied to the task's latest-wins snapshot; the single response is
        // sent once the worker closes the stream at job end.
        return new StreamObserver<>() {
            @Override
            public void onNext(Report report) {
                log.debug("Received telemetry from worker: jobId={}, taskIndex={}, entries={}",
                        report.getJobId(), report.getTaskIndex(), report.getEntriesCount());
                try {
                    jobManager.handleReport(report.getJobId(), report.getTaskIndex(),
                            report.getTimestampMs(), report.getEntriesList());
                } catch (Exception e) {
                    // Telemetry is lossy by design — never break the stream over one report.
                    log.warn("Dropping telemetry for job={}, taskIndex={}: {}",
                            report.getJobId(), report.getTaskIndex(), e.getMessage());
                }
            }

            @Override
            public void onError(Throwable t) {
                log.error("ReportTelemetry stream error: {}", t.getMessage());
            }

            @Override
            public void onCompleted() {
                responseObserver.onNext(ReportTelemetryResponse.getDefaultInstance());
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        log.debug("Received heartbeat from workerId={}", request.getWorkerId());
        workers.updateHeartbeat(request.getWorkerId());
        responseObserver.onNext(HeartbeatResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    /**
     * Worker subscribes once; the coordinator keeps the stream open and pushes
     * system commands (resync, drain) on it. The handler does not complete the call
     * — it stashes the observer and returns, so the stream stays open for later
     * pushes. The cancel handler drops it when the worker disconnects.
     */
    @Override
    public void systemCommands(SubscribeRequest request, StreamObserver<SystemCommand> responseObserver) {
        String workerId = request.getWorkerId();
        log.info("Received systemCommands subscribe from workerId={}", workerId);
        commandStreams.addSysStream(workerId, responseObserver);
        if (responseObserver instanceof ServerCallStreamObserver<SystemCommand> serverStream) {
            serverStream.setOnCancelHandler(() -> {
                log.info("systemCommands stream closed by workerId={}", workerId);
                commandStreams.removeSysStream(workerId);
            });
        }
    }

    /** Job-command counterpart of {@link #systemCommands} (cancel, preempt). */
    @Override
    public void jobCommands(SubscribeRequest request, StreamObserver<JobCommand> responseObserver) {
        String workerId = request.getWorkerId();
        log.info("Received jobCommands subscribe from workerId={}", workerId);
        commandStreams.addJobStream(workerId, responseObserver);
        if (responseObserver instanceof ServerCallStreamObserver<JobCommand> serverStream) {
            serverStream.setOnCancelHandler(() -> {
                log.info("jobCommands stream closed by workerId={}", workerId);
                commandStreams.removeJobStream(workerId);
            });
        }
    }

    // ── coordinator → worker push (used by recovery/scheduling logic) ──────────
    // Each returns false if the worker has no open stream (it will resync on reconnect).

    /** Ask a worker to re-register and re-declare its current jobs (boot/resync recovery). */
    public boolean requestResync(String workerId) {
        return commandStreams.sendSysCmd(workerId,
                SystemCommand.newBuilder().setResync(Resync.getDefaultInstance()).build());
    }

    /** Tell a worker to stop (or resume) pulling new jobs. */
    public boolean drain(String workerId, boolean drain) {
        return commandStreams.sendSysCmd(workerId,
                SystemCommand.newBuilder().setDrain(Drain.newBuilder().setDrain(drain)).build());
    }

    /** Tell a worker to cancel a specific running job. */
    public boolean cancelJob(String workerId, String jobId, FailureReason reason) {
        return commandStreams.sendJobCmd(workerId,
                JobCommand.newBuilder().setCancel(Cancel.newBuilder().setJobId(jobId).setReason(reason)).build());
    }

    /** Tell a worker to preempt a specific running job (stop to free resources). */
    public boolean preemptJob(String workerId, String jobId) {
        return commandStreams.sendJobCmd(workerId,
                JobCommand.newBuilder().setPreempt(Preempt.newBuilder().setJobId(jobId)).build());
    }

    /** Read at Prometheus scrape time by CoordinatorMetrics. */
    public int workerCount() {
        return workers.count();
    }

    /** Snapshot of registered workers for the read-only HTTP API (UI). */
    public java.util.List<com.scheduler.core.WorkerInfo> listWorkers() {
        return workers.list();
    }

    /** Loads persisted workers into the registry on boot (see {@link WorkerRegistry#seed}). */
    public void seedWorkers(java.time.Instant lastHeartbeat) {
        workers.seed(lastHeartbeat);
    }

    /** First scan after {@code scanInterval} — for a fresh start with no seeded workers. */
    public void startHeartbeatMonitor(Duration heartbeatTimeout, Duration scanInterval) {
        startHeartbeatMonitor(heartbeatTimeout, scanInterval, scanInterval);
    }

    /**
     * Starts the liveness monitor in {@link WorkerRegistry}; a dead worker's
     * in-flight jobs are failed with HEARTBEAT_LOST. The first scan is held off for
     * {@code initialDelay} — on boot this is the re-registration window, so seeded
     * workers can reconnect before any eviction.
     */
    public void startHeartbeatMonitor(Duration heartbeatTimeout, Duration scanInterval, Duration initialDelay) {
        workers.startMonitor(heartbeatTimeout, scanInterval, initialDelay, worker -> {
            int failed = jobManager.failJobsForWorker(worker.id(), FailureReason.FAILURE_REASON_HEARTBEAT_LOST);
            if (failed > 0) {
                log.warn("Failed {} job(s) for dead worker: workerId={}", failed, worker.id());
            }
            CoordinatorMetrics.HEARTBEAT_LOSSES.inc();
        });
    }

    public void shutdownHeartbeatMonitor() {
        workers.shutdownMonitor();
    }
}
