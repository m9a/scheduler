package com.scheduler.coordinator;

import com.scheduler.core.FailureReason;
import com.scheduler.core.InputFile;
import com.scheduler.core.Job;
import com.scheduler.core.JobState;
import com.scheduler.core.JobStatus;
import com.scheduler.core.ResourceRequirements;
import com.scheduler.core.TaskState;
import com.scheduler.core.TaskStatus;
import com.scheduler.proto.v1.SubmitJobRequest;

import java.util.HashSet;
import java.util.List;

public final class ProtoMapper {

    private ProtoMapper() {}

    public static Job toDomain(SubmitJobRequest request, List<InputFile> resolvedInputFiles) {
        return new Job(
                request.getName(),
                request.getArtifactUri(),
                request.getParamsMap(),
                request.getPriority(),
                resolvedInputFiles,
                toDomain(request.getResources())
        );
    }

    public static com.scheduler.proto.v1.Job toProto(JobState execution) {
        com.scheduler.proto.v1.Job.Builder builder = com.scheduler.proto.v1.Job.newBuilder()
                .setId(execution.id())
                .setName(execution.job().name())
                .setArtifactUri(execution.job().artifactUri())
                .putAllParams(execution.job().params())
                .setStatus(toProto(execution.status()))
                .addAllTasks(execution.taskStates().values().stream().map(ProtoMapper::toProto).toList())
                .setPriority(execution.job().priority())
                .addAllInputFiles(execution.job().inputFiles().stream()
                        .map(f -> com.scheduler.proto.v1.InputFile.newBuilder()
                                .setName(f.name()).setUri(f.uri()).build())
                        .toList())
                .setResources(toProto(execution.job().resources()));

        if (execution.createdAt() != null) {
            builder.setCreatedAtMillis(execution.createdAt().toEpochMilli());
        }
        if (execution.startedAt() != null) {
            builder.setStartedAtMillis(execution.startedAt().toEpochMilli());
        }
        if (execution.completedAt() != null) {
            builder.setCompletedAtMillis(execution.completedAt().toEpochMilli());
        }
        if (execution.failureReason() != null) {
            builder.setFailureReason(toProto(execution.failureReason()));
            builder.setFailureDetail(execution.failureDetail() != null ? execution.failureDetail() : "");
            builder.setErrorMessage(execution.failureReason().toMessage(execution.failureDetail()));
        }
        return builder.build();
    }

    public static com.scheduler.proto.v1.Task toProto(TaskState taskExecution) {
        com.scheduler.proto.v1.Task.Builder builder = com.scheduler.proto.v1.Task.newBuilder()
                .setId(taskExecution.id())
                .setName(taskExecution.taskName())
                .setSequenceNumber(taskExecution.taskIndex())
                .setStatus(toProto(taskExecution.status()));
        return builder.build();
    }

    public static com.scheduler.proto.v1.JobStatus toProto(JobStatus status) {
        return switch (status) {
            case SUBMITTED, QUEUED -> com.scheduler.proto.v1.JobStatus.JOB_STATUS_QUEUED;
            case STARTING -> com.scheduler.proto.v1.JobStatus.JOB_STATUS_STARTING;
            case RUNNING -> com.scheduler.proto.v1.JobStatus.JOB_STATUS_RUNNING;
            case COMPLETED -> com.scheduler.proto.v1.JobStatus.JOB_STATUS_COMPLETED;
            case FAILED -> com.scheduler.proto.v1.JobStatus.JOB_STATUS_FAILED;
            case KILLED -> com.scheduler.proto.v1.JobStatus.JOB_STATUS_KILLED;
            case CANCELLED -> com.scheduler.proto.v1.JobStatus.JOB_STATUS_CANCELLED;
        };
    }

    public static JobStatus toDomain(com.scheduler.proto.v1.JobStatus status) {
        return switch (status) {
            case JOB_STATUS_QUEUED -> JobStatus.QUEUED;
            case JOB_STATUS_STARTING -> JobStatus.STARTING;
            case JOB_STATUS_RUNNING -> JobStatus.RUNNING;
            case JOB_STATUS_COMPLETED -> JobStatus.COMPLETED;
            case JOB_STATUS_FAILED -> JobStatus.FAILED;
            case JOB_STATUS_KILLED -> JobStatus.KILLED;
            case JOB_STATUS_CANCELLED -> JobStatus.CANCELLED;
            default -> throw new IllegalArgumentException("Unknown job status: " + status);
        };
    }

    public static com.scheduler.proto.v1.TaskStatus toProto(TaskStatus status) {
        return switch (status) {
            case PENDING -> com.scheduler.proto.v1.TaskStatus.TASK_STATUS_PENDING;
            case RUNNING -> com.scheduler.proto.v1.TaskStatus.TASK_STATUS_RUNNING;
            case COMPLETED -> com.scheduler.proto.v1.TaskStatus.TASK_STATUS_COMPLETED;
            case FAILED -> com.scheduler.proto.v1.TaskStatus.TASK_STATUS_FAILED;
            case SKIPPED -> com.scheduler.proto.v1.TaskStatus.TASK_STATUS_SKIPPED;
        };
    }

    public static TaskStatus toDomain(com.scheduler.proto.v1.TaskStatus status) {
        return switch (status) {
            case TASK_STATUS_PENDING -> TaskStatus.PENDING;
            case TASK_STATUS_RUNNING -> TaskStatus.RUNNING;
            case TASK_STATUS_COMPLETED -> TaskStatus.COMPLETED;
            case TASK_STATUS_FAILED -> TaskStatus.FAILED;
            case TASK_STATUS_SKIPPED -> TaskStatus.SKIPPED;
            default -> throw new IllegalArgumentException("Unknown task status: " + status);
        };
    }

    public static com.scheduler.proto.v1.FailureReason toProto(FailureReason reason) {
        return switch (reason) {
            case HEARTBEAT_LOST -> com.scheduler.proto.v1.FailureReason.FAILURE_REASON_HEARTBEAT_LOST;
            case PROCESS_TIMEOUT -> com.scheduler.proto.v1.FailureReason.FAILURE_REASON_PROCESS_TIMEOUT;
            case PROCESS_EXITED -> com.scheduler.proto.v1.FailureReason.FAILURE_REASON_PROCESS_EXITED;
            case PROCESS_START_FAILED -> com.scheduler.proto.v1.FailureReason.FAILURE_REASON_PROCESS_START_FAILED;
        };
    }

    public static FailureReason toDomain(com.scheduler.proto.v1.FailureReason reason) {
        return switch (reason) {
            case FAILURE_REASON_HEARTBEAT_LOST -> FailureReason.HEARTBEAT_LOST;
            case FAILURE_REASON_PROCESS_TIMEOUT -> FailureReason.PROCESS_TIMEOUT;
            case FAILURE_REASON_PROCESS_EXITED -> FailureReason.PROCESS_EXITED;
            case FAILURE_REASON_PROCESS_START_FAILED -> FailureReason.PROCESS_START_FAILED;
            default -> throw new IllegalArgumentException("Unknown failure reason: " + reason);
        };
    }

    public static ResourceRequirements toDomain(com.scheduler.proto.v1.ResourceRequirements proto) {
        if (proto == null || proto.equals(com.scheduler.proto.v1.ResourceRequirements.getDefaultInstance())) {
            return ResourceRequirements.DEFAULT;
        }
        return new ResourceRequirements(
                proto.getMemoryMb(),
                proto.getCpuCores(),
                proto.getGpu(),
                new HashSet<>(proto.getCapabilitiesList())
        );
    }

    public static com.scheduler.proto.v1.ResourceRequirements toProto(ResourceRequirements domain) {
        return com.scheduler.proto.v1.ResourceRequirements.newBuilder()
                .setMemoryMb(domain.memoryMb())
                .setCpuCores(domain.cpuCores())
                .setGpu(domain.gpu())
                .addAllCapabilities(domain.capabilities())
                .build();
    }
}
