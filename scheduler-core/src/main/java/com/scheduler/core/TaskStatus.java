package com.scheduler.core;

import java.util.Set;

public enum TaskStatus {
    PENDING,
    RUNNING,
    COMPLETED,
    FAILED,
    SKIPPED;

    private static final Set<TaskStatus> TERMINAL = Set.of(COMPLETED, FAILED, SKIPPED);

    public boolean canTransitionTo(TaskStatus target) {
        if (this == target) return false;
        return switch (this) {
            case PENDING -> target == RUNNING || target == SKIPPED;
            case RUNNING -> target == COMPLETED || target == FAILED;
            case COMPLETED, FAILED, SKIPPED -> false;
        };
    }

    public boolean isTerminal() {
        return TERMINAL.contains(this);
    }
}
