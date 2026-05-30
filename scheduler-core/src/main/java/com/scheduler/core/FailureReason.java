package com.scheduler.core;

public enum FailureReason {
    HEARTBEAT_LOST("Worker heartbeat lost"),
    PROCESS_TIMEOUT("Job process timed out"),
    PROCESS_EXITED("Job process exited with non-zero code"),
    PROCESS_START_FAILED("Failed to start job process");

    private final String message;

    FailureReason(String message) {
        this.message = message;
    }

    public String message() {
        return message;
    }

    public String toMessage(String detail) {
        if (detail == null) {
            return message;
        }
        return message + ": " + detail;
    }
}
