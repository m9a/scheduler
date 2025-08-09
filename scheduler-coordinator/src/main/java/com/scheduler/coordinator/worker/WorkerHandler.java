package com.scheduler.coordinator.worker;

import com.scheduler.coordinator.ProtoMapper;

import com.scheduler.core.JobState;
import com.scheduler.core.WorkerInfo;
import com.scheduler.coordinator.JobManagerImpl;
import com.scheduler.proto.coordinator.*;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles gRPC RPCs from workers. Workers call this to register, pull jobs,
 * and stream task status updates.
 *
 * <pre>
 * Worker ──gRPC──► WorkerHandler ──► JobManagerImpl
 *                  (RegisterWorker)   (claimNextJob)
 *                  (PullJob)          (updateTaskStatus, finalizeJob)
 *                  (ReportTaskStatus)
 * </pre>
 */
public class WorkerHandler extends WorkerServiceGrpc.WorkerServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(WorkerHandler.class);

    private final ConcurrentHashMap<String, WorkerInfo> workers = new ConcurrentHashMap<>();
    private final JobManagerImpl jobManager;

    public WorkerHandler(JobManagerImpl jobManager) {
        this.jobManager = jobManager;
    }

    @Override
    public void registerWorker(RegisterWorkerRequest request, StreamObserver<RegisterWorkerResponse> responseObserver) {
        String workerId = UUID.randomUUID().toString();
        log.info("Received registerWorker from hostname={}, capacity={}, assigned workerId={}",
                request.getHostname(), request.getCapacity(), workerId);
        WorkerInfo worker = new WorkerInfo(
                workerId,
                request.getHostname(),
                request.getCapacity(),
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
        Optional<JobState> claimed = jobManager.claimNextJob(request.getWorkerId());

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
    public StreamObserver<ReportTaskStatusRequest> reportTaskStatus(StreamObserver<ReportTaskStatusResponse> responseObserver) {
        return new StreamObserver<>() {
            private volatile String jobId;

            @Override
            public void onNext(ReportTaskStatusRequest request) {
                if (jobId == null) {
                    jobId = request.getJobId();
                }
                log.info("Received task status update: jobId={}, taskIndex={}, taskName={}, status={}{}",
                        request.getJobId(), request.getTaskIndex(), request.getTaskName(), request.getStatus(),
                        request.getErrorMessage().isEmpty() ? "" : ", error=" + request.getErrorMessage());
                jobManager.updateTaskStatus(
                        request.getJobId(),
                        request.getTaskIndex(),
                        request.getTaskName(),
                        ProtoMapper.toDomain(request.getStatus()),
                        request.getErrorMessage().isEmpty() ? null : request.getErrorMessage()
                );
            }

            @Override
            public void onError(Throwable t) {
                log.error("ReportTaskStatus stream error: {}", t.getMessage());
                if (jobId != null) {
                    jobManager.finalizeJob(jobId);
                }
            }

            @Override
            public void onCompleted() {
                if (jobId != null) {
                    jobManager.finalizeJob(jobId);
                }
                responseObserver.onNext(ReportTaskStatusResponse.getDefaultInstance());
                responseObserver.onCompleted();
            }
        };
    }

    void updateHeartbeat(String workerId) {
        workers.computeIfPresent(workerId, (id, worker) -> worker.withLastHeartbeat(Instant.now()));
    }
}
