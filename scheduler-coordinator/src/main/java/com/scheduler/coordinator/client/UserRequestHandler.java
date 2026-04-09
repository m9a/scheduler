package com.scheduler.coordinator.client;

import com.scheduler.coordinator.ProtoMapper;

import com.scheduler.core.JobState;
import com.scheduler.core.exception.JobNotFoundException;
import com.scheduler.coordinator.JobManagerImpl;
import com.scheduler.proto.v1.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles gRPC RPCs from external clients (CLI, UI, etc.).
 *
 * <pre>
 * Client ──gRPC──► UserRequestHandler ──► JobManagerImpl
 *                  (SubmitJob)           (submit, getJob)
 *                  (GetJobStatus)
 * </pre>
 */
public class UserRequestHandler extends ClientServiceGrpc.ClientServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(UserRequestHandler.class);

    private final JobManagerImpl jobManager;

    public UserRequestHandler(JobManagerImpl jobManager) {
        this.jobManager = jobManager;
    }

    @Override
    public void submitJob(SubmitJobRequest request, StreamObserver<SubmitJobResponse> responseObserver) {
        log.info("Received submitJob name={}, jarPath={}", request.getName(), request.getJarPath());
        try {
            JobState execution = jobManager.submit(ProtoMapper.toDomain(request));
            log.info("Job submitted: jobId={}, name={}", execution.id(), execution.job().name());
            responseObserver.onNext(SubmitJobResponse.newBuilder()
                    .setJob(ProtoMapper.toProto(execution))
                    .build());
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            log.warn("Rejected submitJob name={}: {}", request.getName(), e.getMessage());
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void getJobStatus(GetJobStatusRequest request, StreamObserver<GetJobStatusResponse> responseObserver) {
        log.info("Received getJobStatus jobId={}", request.getJobId());
        try {
            JobState execution = jobManager.getJob(request.getJobId());
            responseObserver.onNext(GetJobStatusResponse.newBuilder()
                    .setJob(ProtoMapper.toProto(execution))
                    .build());
            responseObserver.onCompleted();
        } catch (JobNotFoundException e) {
            log.warn("Job not found: jobId={}", request.getJobId());
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }
}
