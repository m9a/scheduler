package com.scheduler.core;

import java.util.Set;

public enum JobStatus {
    SUBMITTED,
    QUEUED,
    STARTING,
    RUNNING,
    COMPLETED,
    FAILED,
    KILLED,
    CANCELLED;

    private static final Set<JobStatus> TERMINAL = Set.of(COMPLETED, FAILED, KILLED, CANCELLED);

    public boolean canTransitionTo(JobStatus target) {
        if (this == target) return false;
        return switch (this) {
            case SUBMITTED -> target == QUEUED;
            case QUEUED -> target == STARTING || target == CANCELLED;
            case STARTING -> target == RUNNING || target == FAILED || target == KILLED || target == CANCELLED;
            case RUNNING -> target == COMPLETED || target == FAILED || target == KILLED || target == CANCELLED;
            case COMPLETED, FAILED, KILLED, CANCELLED -> false;
        };
    }

    public boolean isTerminal() {
        return TERMINAL.contains(this);
    }
}
