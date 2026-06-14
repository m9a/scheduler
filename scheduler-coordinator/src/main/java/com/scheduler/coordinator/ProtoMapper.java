package com.scheduler.coordinator;

import com.scheduler.core.FailureMessages;
import com.scheduler.core.InputFile;
import com.scheduler.core.Job;
import com.scheduler.core.JobStatus;
import com.scheduler.core.ResourceRequirements;
import com.scheduler.core.TaskStatus;
import com.scheduler.proto.client.SubmitJobRequest;

import java.util.HashSet;
import java.util.List;

/**
 * Assembles the client-facing wire shapes from the coordinator's domain objects
 * and vice versa: the submit request → immutable {@link Job} definition, and the
 * runtime {@link JobStatus} snapshot → the client {@code Job} proto (which merges
 * the definition with live state). This is genuine definition/snapshot ⇄ wire
 * assembly, not a status/enum copy — state and telemetry now use the proto types
 * directly with no conversion (see CLAUDE.md "One status message").
 */
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

    public static com.scheduler.proto.v1.Job toProto(JobStatus execution) {
        com.scheduler.proto.v1.Job.Builder builder = com.scheduler.proto.v1.Job.newBuilder()
                .setId(execution.id())
                .setName(execution.job().name())
                .setArtifactUri(execution.job().artifactUri())
                .putAllParams(execution.job().params())
                .setState(execution.state())
                .addAllTasks(execution.taskStatuses().values().stream().map(ProtoMapper::toProto).toList())
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
            builder.setFailureReason(execution.failureReason());
            builder.setFailureDetail(execution.failureDetail() != null ? execution.failureDetail() : "");
            builder.setErrorMessage(FailureMessages.format(execution.failureReason(), execution.failureDetail()));
        }
        return builder.build();
    }

    public static com.scheduler.proto.v1.Task toProto(TaskStatus taskExecution) {
        return com.scheduler.proto.v1.Task.newBuilder()
                .setId(taskExecution.id())
                .setName(taskExecution.taskName())
                .setSequenceNumber(taskExecution.taskIndex())
                .setState(taskExecution.state())
                .addAllReports(taskExecution.reports().values())   // proto ReportEntry, stored as-is
                .build();
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
