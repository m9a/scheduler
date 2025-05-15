package com.scheduler.coordinator;

import com.scheduler.core.Job;
import com.scheduler.core.JobExecution;
import com.scheduler.core.JobStatus;
import com.scheduler.core.Task;
import com.scheduler.core.TaskStatus;
import com.scheduler.proto.v1.SubmitJobRequest;

import java.util.List;

public final class ProtoMapper {

    private ProtoMapper() {}

    public static Job toDomain(SubmitJobRequest request) {
        List<Task> tasks = request.getTasksList().stream()
                .map(t -> new Task(t.getName()))
                .toList();
        return new Job(
                request.getName(),
                request.getJarPath(),
                request.getMainClass().isEmpty() ? null : request.getMainClass(),
                tasks,
                request.getPriority()
        );
    }

    public static com.scheduler.proto.v1.Job toProto(JobExecution execution) {
        com.scheduler.proto.v1.Job.Builder builder = com.scheduler.proto.v1.Job.newBuilder()
                .setId(execution.id())
                .setName(execution.job().name())
                .setJarPath(execution.job().jarPath())
                .setStatus(toProto(execution.status()))
                .addAllTasks(execution.job().tasks().stream().map(ProtoMapper::toProto).toList())
                .setPriority(execution.job().priority());

        if (execution.job().mainClass() != null) {
            builder.setMainClass(execution.job().mainClass());
        }

        if (execution.createdAt() != null) {
            builder.setCreatedAtMillis(execution.createdAt().toEpochMilli());
        }
        if (execution.startedAt() != null) {
            builder.setStartedAtMillis(execution.startedAt().toEpochMilli());
        }
        if (execution.completedAt() != null) {
            builder.setCompletedAtMillis(execution.completedAt().toEpochMilli());
        }
        if (execution.errorMessage() != null) {
            builder.setErrorMessage(execution.errorMessage());
        }
        return builder.build();
    }

    public static com.scheduler.proto.v1.Task toProto(Task task) {
        return com.scheduler.proto.v1.Task.newBuilder()
                .setName(task.name())
                .build();
    }

    public static com.scheduler.proto.v1.JobStatus toProto(JobStatus status) {
        return switch (status) {
            case SUBMITTED, QUEUED -> com.scheduler.proto.v1.JobStatus.JOB_STATUS_QUEUED;
            case STARTING -> com.scheduler.proto.v1.JobStatus.JOB_STATUS_STARTING;
            case RUNNING -> com.scheduler.proto.v1.JobStatus.JOB_STATUS_RUNNING;
            case COMPLETED -> com.scheduler.proto.v1.JobStatus.JOB_STATUS_COMPLETED;
            case FAILED -> com.scheduler.proto.v1.JobStatus.JOB_STATUS_FAILED;
            case CANCELLED -> com.scheduler.proto.v1.JobStatus.JOB_STATUS_CANCELLED;
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
}
