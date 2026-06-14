package com.scheduler.core;

import com.scheduler.proto.v1.TaskState;

import java.util.Set;

/**
 * Task lifecycle transition rules, operating on the proto {@link TaskState} enum.
 * Mirrors the README "Task states" table; {@link TaskStatus} enforces them.
 */
public final class TaskStates {

    private TaskStates() {}

    private static final Set<TaskState> TERMINAL = Set.of(
            TaskState.TASK_STATE_COMPLETED, TaskState.TASK_STATE_FAILED);

    public static boolean isTerminal(TaskState state) {
        return TERMINAL.contains(state);
    }

    public static boolean canTransitionTo(TaskState from, TaskState to) {
        if (from == to) {
            return false;
        }
        return switch (from) {
            case TASK_STATE_PENDING -> to == TaskState.TASK_STATE_RUNNING || to == TaskState.TASK_STATE_FAILED;
            case TASK_STATE_RUNNING -> to == TaskState.TASK_STATE_COMPLETED || to == TaskState.TASK_STATE_FAILED;
            default -> false;
        };
    }
}
