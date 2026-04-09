package com.scheduler.core;

import java.time.Instant;
import java.util.Objects;

public class TaskState {

    private final String id;
    private final int taskIndex;
    private final String taskName;
    private TaskStatus status;
    private Instant startedAt;
    private Instant completedAt;
    private String errorMessage;
    private Integer exitCode;

    public TaskState(String id, int taskIndex, String taskName) {
        this.id = Objects.requireNonNull(id, "Task ID must not be null");
        this.taskIndex = taskIndex;
        this.taskName = Objects.requireNonNull(taskName, "Task name must not be null");
        this.status = TaskStatus.PENDING;
    }

    public String id() { return id; }
    public int taskIndex() { return taskIndex; }
    public String taskName() { return taskName; }
    public TaskStatus status() { return status; }
    public Instant startedAt() { return startedAt; }
    public Instant completedAt() { return completedAt; }
    public String errorMessage() { return errorMessage; }
    public Integer exitCode() { return exitCode; }

    public void setStatus(TaskStatus newStatus) {
        if (!status.canTransitionTo(newStatus)) {
            throw new IllegalStateException(
                    "Cannot transition task %d from %s to %s".formatted(taskIndex, status, newStatus));
        }
        this.status = newStatus;
    }

    public void setStartedAt(Instant startedAt) { this.startedAt = startedAt; }
    public void setCompletedAt(Instant completedAt) { this.completedAt = completedAt; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    public void setExitCode(Integer exitCode) { this.exitCode = exitCode; }
}
