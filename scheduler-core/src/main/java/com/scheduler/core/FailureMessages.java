package com.scheduler.core;

import com.scheduler.proto.v1.FailureReason;

/**
 * Renders a proto {@link FailureReason} (+ optional detail) into the human
 * message shown to clients and logs. Lives here so both the coordinator (client
 * Job assembly, logs) and the worker (status-stream logs) share one mapping.
 */
public final class FailureMessages {

    private FailureMessages() {}

    public static String text(FailureReason reason) {
        return switch (reason) {
            case FAILURE_REASON_HEARTBEAT_LOST -> "Worker heartbeat lost";
            case FAILURE_REASON_PROCESS_TIMEOUT -> "Job process timed out";
            case FAILURE_REASON_PROCESS_EXITED -> "Job process exited with non-zero code";
            case FAILURE_REASON_PROCESS_START_FAILED -> "Failed to start job process";
            default -> "Job failed";
        };
    }

    /** Reason text, with {@code ": detail"} appended when a detail is present. */
    public static String format(FailureReason reason, String detail) {
        String message = text(reason);
        return (detail == null || detail.isEmpty()) ? message : message + ": " + detail;
    }
}
