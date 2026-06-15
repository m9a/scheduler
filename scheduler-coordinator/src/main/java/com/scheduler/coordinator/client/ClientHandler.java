package com.scheduler.coordinator.client;

import com.scheduler.coordinator.ProtoMapper;
import com.scheduler.core.InputFile;
import com.scheduler.core.JobStatus;
import com.scheduler.core.ObjectStore;
import com.scheduler.core.exception.JobNotFoundException;
import com.scheduler.coordinator.JobManager;
import com.scheduler.proto.v1.*;
import com.scheduler.proto.client.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * <b>Client → Coordinator edge.</b> Implements the ClientService gRPC API that
 * external clients (scheduler-cli, the SchedulerClient library) call into.
 * Pure transport plus input-file staging: protos are unpacked/packed via
 * {@link ProtoMapper}, job state lives in {@link JobManager}, files go through
 * the ObjectStore.
 *
 * <pre>
 * Client ──gRPC──► ClientHandler ──► JobManager
 *                  SubmitJob          submit   (input files staged to ObjectStore first)
 *                  GetJobStatus       getJob
 *                  ListJobFiles    ──► ObjectStore (MinIO)
 *                  GetJobOutput    ──► ObjectStore (chunked stream)
 * </pre>
 */
public class ClientHandler extends ClientServiceGrpc.ClientServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(ClientHandler.class);
    private static final int CHUNK_SIZE = 64 * 1024;

    private final JobManager jobManager;
    private final ObjectStore objectStore;

    public ClientHandler(JobManager jobManager, ObjectStore objectStore) {
        this.jobManager = jobManager;
        this.objectStore = objectStore;
    }

    @Override
    public void submitJob(SubmitJobRequest request, StreamObserver<SubmitJobResponse> responseObserver) {
        log.info("Received submitJob name={}, artifactUri={}", request.getName(), request.getArtifactUri());
        try {
            String jobId = UUID.randomUUID().toString();

            List<InputFile> resolvedFiles = resolveInputFiles(jobId, request.getInputFilesList());

            JobStatus execution = jobManager.submit(jobId, ProtoMapper.toDomain(request, resolvedFiles));
            log.info("Job submitted: jobId={}, name={}, inputFiles={}", execution.id(), execution.job().name(), resolvedFiles.size());
            responseObserver.onNext(SubmitJobResponse.newBuilder()
                    .setJob(ProtoMapper.toProto(execution, jobManager.lastActivity(execution.id())))
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
            JobStatus execution = jobManager.getJob(request.getJobId());
            responseObserver.onNext(GetJobStatusResponse.newBuilder()
                    .setJob(ProtoMapper.toProto(execution, jobManager.lastActivity(execution.id())))
                    .build());
            responseObserver.onCompleted();
        } catch (JobNotFoundException e) {
            log.warn("Job not found: jobId={}", request.getJobId());
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void listJobFiles(ListJobFilesRequest request, StreamObserver<ListJobFilesResponse> responseObserver) {
        log.info("Received listJobFiles jobId={}", request.getJobId());
        try {
            List<ObjectStore.ObjectInfo> objects = objectStore.listObjects("jobs/" + request.getJobId() + "/");
            ListJobFilesResponse.Builder builder = ListJobFilesResponse.newBuilder();
            for (ObjectStore.ObjectInfo obj : objects) {
                builder.addFiles(FileInfo.newBuilder()
                        .setName(obj.key())
                        .setSizeBytes(obj.sizeBytes())
                        .build());
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Failed to list files for jobId={}: {}", request.getJobId(), e.getMessage());
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void getJobOutput(GetJobOutputRequest request, StreamObserver<GetJobOutputResponse> responseObserver) {
        log.info("Received getJobOutput jobId={}, path={}", request.getJobId(), request.getPath());
        String key = "jobs/" + request.getJobId() + "/" + request.getPath();
        try {
            long size = objectStore.getObjectSize(key);
            responseObserver.onNext(GetJobOutputResponse.newBuilder()
                    .setHeader(FileInfo.newBuilder().setName(request.getPath()).setSizeBytes(size).build())
                    .build());

            try (InputStream in = objectStore.getObjectStream(key)) {
                byte[] buf = new byte[CHUNK_SIZE];
                int read;
                while ((read = in.read(buf)) != -1) {
                    responseObserver.onNext(GetJobOutputResponse.newBuilder()
                            .setChunk(com.google.protobuf.ByteString.copyFrom(buf, 0, read))
                            .build());
                }
            }
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Failed to stream file jobId={}, path={}: {}", request.getJobId(), request.getPath(), e.getMessage());
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }

    private List<InputFile> resolveInputFiles(String jobId, List<com.scheduler.proto.v1.InputFile> protoFiles) {
        List<InputFile> resolved = new ArrayList<>();
        for (com.scheduler.proto.v1.InputFile pf : protoFiles) {
            if (pf.hasContent()) {
                String key = "jobs/" + jobId + "/input/" + pf.getName();
                objectStore.putObject(key, pf.getContent().toByteArray());
                resolved.add(new InputFile(pf.getName(), key));
            } else if (pf.hasUri()) {
                if (!objectStore.exists(pf.getUri())) {
                    throw new IllegalArgumentException("Input file URI does not exist: " + pf.getUri());
                }
                resolved.add(new InputFile(pf.getName(), pf.getUri()));
            }
        }
        return resolved;
    }
}
