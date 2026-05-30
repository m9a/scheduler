package com.scheduler.worker;

import com.scheduler.core.FailureReason;
import com.scheduler.proto.v1.TaskStatus;

import java.util.Objects;

/**
 * Worker's representation of status updates. Two kinds:
 * <ul>
 *   <li><b>Task-level:</b> received via WebSocket from job containers (SDK sends these
 *       as binary proto). Has taskIndex, taskName, taskStatus populated; jobStatus/failureReason are null.</li>
 *   <li><b>Job-level:</b> created by the worker itself to report job lifecycle
 *       (STARTING, RUNNING, COMPLETED, FAILED, KILLED). Has jobStatus/failureReason
 *       populated; task fields are defaults.</li>
 * </ul>
 */
record StatusUpdate(
        String jobId,
        int taskIndex,
        String taskName,
        String taskStatus,
        long durationMs,
        String errorMessage,
        String jobStatus,
        FailureReason failureReason,
        String failureDetail
) {

    StatusUpdate {
        Objects.requireNonNull(jobId, "jobId");
    }

    static StatusUpdate jobUpdate(String jobId, String jobStatus,
                                   FailureReason failureReason, String failureDetail) {
        return new StatusUpdate(jobId, 0, null, null, 0, null,
                jobStatus, failureReason, failureDetail);
    }

    static StatusUpdate fromProto(com.scheduler.proto.job.StatusUpdate proto) {
        String status = switch (proto.getTaskStatus()) {
            case TASK_STATUS_RUNNING -> "RUNNING";
            case TASK_STATUS_COMPLETED -> "COMPLETED";
            case TASK_STATUS_FAILED -> "FAILED";
            default -> "UNKNOWN";
        };
        return new StatusUpdate(
                proto.getJobId(),
                proto.getTaskIndex(),
                proto.getTaskName(),
                status,
                proto.getDurationMs(),
                proto.getErrorMessage().isEmpty() ? null : proto.getErrorMessage(),
                null, null, null
        );
    }
}
