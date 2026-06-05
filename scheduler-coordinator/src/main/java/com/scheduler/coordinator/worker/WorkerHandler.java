package com.scheduler.coordinator.worker;

import com.scheduler.coordinator.ProtoMapper;

import com.scheduler.core.FailureReason;
import com.scheduler.core.JobState;
import com.scheduler.core.JobStatus;
import com.scheduler.core.TaskStatus;
import com.scheduler.core.WorkerInfo;

import java.util.HashSet;
import com.scheduler.coordinator.JobManagerImpl;
import com.scheduler.proto.coordinator.*;
import com.scheduler.proto.job.StatusUpdate;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Handles gRPC RPCs from workers. Workers call this to register, pull jobs,
 * and stream status updates (both job-level and task-level).
 *
 * <pre>
 * Worker ──gRPC──► WorkerHandler ──► JobManagerImpl
 *                  (RegisterWorker)   (claimNextJob)
 *                  (PullJob)          (handleStatusUpdate)
 *                  (ReportStatus)
 * </pre>
 */
public class WorkerHandler extends WorkerServiceGrpc.WorkerServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(WorkerHandler.class);

    private final ConcurrentHashMap<String, WorkerInfo> workers = new ConcurrentHashMap<>();
    private final JobManagerImpl jobManager;
    private ScheduledExecutorService heartbeatMonitor;

    public WorkerHandler(JobManagerImpl jobManager) {
        this.jobManager = jobManager;
    }

    @Override
    public void registerWorker(RegisterWorkerRequest request, StreamObserver<RegisterWorkerResponse> responseObserver) {
        String workerId = UUID.randomUUID().toString();
        log.info("Received registerWorker from hostname={}, memoryMb={}, cpuCores={}, gpu={}, assigned workerId={}",
                request.getHostname(), request.getMemoryMb(), request.getCpuCores(), request.getGpu(), workerId);
        WorkerInfo worker = new WorkerInfo(
                workerId,
                request.getHostname(),
                request.getMemoryMb(),
                request.getCpuCores(),
                request.getGpu(),
                new HashSet<>(request.getCapabilitiesList()),
                Instant.now(),
                Instant.now()
        );
        workers.put(workerId, worker);

        responseObserver.onNext(RegisterWorkerResponse.newBuilder()
                .setWorkerId(workerId)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void pullJob(PullJobRequest request, StreamObserver<PullJobResponse> responseObserver) {
        log.info("Received pullJob from workerId={}", request.getWorkerId());
        WorkerInfo worker = workers.get(request.getWorkerId());
        if (worker == null) {
            log.warn("Unknown workerId={} in pullJob", request.getWorkerId());
            responseObserver.onNext(PullJobResponse.getDefaultInstance());
            responseObserver.onCompleted();
            return;
        }

        Optional<JobState> claimed = jobManager.claimNextJob(worker);

        PullJobResponse.Builder builder = PullJobResponse.newBuilder();
        if (claimed.isPresent()) {
            log.info("Assigned jobId={} to workerId={}", claimed.get().id(), request.getWorkerId());
            builder.setJob(ProtoMapper.toProto(claimed.get()));
        } else {
            log.debug("No jobs available for workerId={}", request.getWorkerId());
        }

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<StatusUpdate> reportStatus(StreamObserver<StatusUpdateResponse> responseObserver) {
        return new StreamObserver<>() {
            @Override
            public void onNext(StatusUpdate update) {
                JobStatus jobStatus = null;
                FailureReason failureReason = null;
                String failureDetail = null;
                if (update.getJobStatus() != com.scheduler.proto.v1.JobStatus.JOB_STATUS_UNSPECIFIED) {
                    jobStatus = ProtoMapper.toDomain(update.getJobStatus());

                    if (update.getFailureReason() != com.scheduler.proto.v1.FailureReason.FAILURE_REASON_UNSPECIFIED) {
                        failureReason = ProtoMapper.toDomain(update.getFailureReason());
                        failureDetail = update.getFailureDetail().isEmpty() ? null : update.getFailureDetail();
                    }

                    log.info("Received job status update: jobId={}, jobStatus={}{}",
                            update.getJobId(), jobStatus,
                            failureReason != null ? ", reason=" + failureReason.toMessage(failureDetail) : "");
                }

                TaskStatus taskStatus = null;
                if (update.getTaskStatus() != com.scheduler.proto.v1.TaskStatus.TASK_STATUS_UNSPECIFIED) {
                    taskStatus = ProtoMapper.toDomain(update.getTaskStatus());
                    log.info("Received task status update: jobId={}, taskIndex={}, taskName={}, status={}{}",
                            update.getJobId(), update.getTaskIndex(), update.getTaskName(), taskStatus,
                            update.getErrorMessage().isEmpty() ? "" : ", error=" + update.getErrorMessage());
                }

                jobManager.handleStatusUpdate(
                        update.getJobId(),
                        jobStatus,
                        failureReason, failureDetail,
                        update.getTaskIndex(),
                        update.getTaskName().isEmpty() ? null : update.getTaskName(),
                        taskStatus,
                        update.getErrorMessage().isEmpty() ? null : update.getErrorMessage()
                );
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
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        String workerId = request.getWorkerId();
        log.debug("Received heartbeat from workerId={}", workerId);
        updateHeartbeat(workerId);
        responseObserver.onNext(HeartbeatResponse.newBuilder()
                .setShouldDrain(false)
                .build());
        responseObserver.onCompleted();
    }

    void updateHeartbeat(String workerId) {
        workers.computeIfPresent(workerId, (id, worker) -> worker.withLastHeartbeat(Instant.now()));
    }

    /**
     * Starts a background thread that periodically scans all registered workers
     * and fails jobs for any worker whose last heartbeat is older than the timeout.
     */
    public void startHeartbeatMonitor(Duration heartbeatTimeout, Duration scanInterval) {
        heartbeatMonitor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "heartbeat-monitor");
            t.setDaemon(true);
            return t;
        });
        heartbeatMonitor.scheduleAtFixedRate(() -> {
            try {
                Instant cutoff = Instant.now().minus(heartbeatTimeout);
                for (WorkerInfo worker : workers.values()) {
                    if (worker.lastHeartbeat().isBefore(cutoff)) {
                        log.warn("Worker heartbeat lost: workerId={}, hostname={}, lastHeartbeat={}",
                                worker.id(), worker.hostname(), worker.lastHeartbeat());
                        int failed = jobManager.failJobsForWorker(worker.id(), FailureReason.HEARTBEAT_LOST);
                        if (failed > 0) {
                            log.warn("Failed {} job(s) for dead worker: workerId={}", failed, worker.id());
                        }
                        workers.remove(worker.id());
                    }
                }
            } catch (Exception e) {
                log.error("Heartbeat monitor scan failed: {}", e.getMessage(), e);
            }
        }, scanInterval.toMillis(), scanInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void shutdownHeartbeatMonitor() {
        if (heartbeatMonitor != null) {
            heartbeatMonitor.shutdown();
        }
    }
}
