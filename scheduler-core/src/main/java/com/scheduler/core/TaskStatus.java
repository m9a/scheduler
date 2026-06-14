package com.scheduler.core;

import com.scheduler.proto.v1.ReportEntry;
import com.scheduler.proto.v1.TaskState;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Mutable runtime snapshot of one task within a job: its lifecycle
 * {@link TaskState}, timestamps, error, and latest-wins telemetry. State
 * transitions enforce {@link TaskStates}.
 */
public class TaskStatus {

    private final String id;
    private final int taskIndex;
    private String taskName;
    private TaskState state;
    private Instant startedAt;
    private Instant completedAt;
    private String errorMessage;
    private Integer exitCode;
    // Latest-wins snapshot of job-emitted telemetry (progress_*, metrics, events),
    // keyed by entry key. Stored as the proto ReportEntry — no domain copy.
    private final Map<String, ReportEntry> reports = new LinkedHashMap<>();

    public TaskStatus(String id, int taskIndex, String taskName) {
        this.id = Objects.requireNonNull(id, "Task ID must not be null");
        this.taskIndex = taskIndex;
        this.taskName = Objects.requireNonNull(taskName, "Task name must not be null");
        this.state = TaskState.TASK_STATE_PENDING;
    }

    public String id() { return id; }
    public int taskIndex() { return taskIndex; }
    public String taskName() { return taskName; }
    public TaskState state() { return state; }
    public Instant startedAt() { return startedAt; }
    public Instant completedAt() { return completedAt; }
    public String errorMessage() { return errorMessage; }
    public Integer exitCode() { return exitCode; }
    public Map<String, ReportEntry> reports() { return reports; }

    /** Merges a forwarded telemetry batch into the snapshot — latest value per key wins. */
    public void applyReports(List<ReportEntry> entries) {
        for (ReportEntry entry : entries) {
            reports.put(entry.getKey(), entry);
        }
    }

    public void start(String taskName) {
        transition(TaskState.TASK_STATE_RUNNING);
        if (taskName != null) {
            this.taskName = taskName;
        }
        this.startedAt = Instant.now();
    }

    public void complete(String taskName) {
        transition(TaskState.TASK_STATE_COMPLETED);
        if (taskName != null) {
            this.taskName = taskName;
        }
        this.completedAt = Instant.now();
    }

    public void fail(String taskName, String errorMessage) {
        transition(TaskState.TASK_STATE_FAILED);
        if (taskName != null) {
            this.taskName = taskName;
        }
        this.completedAt = Instant.now();
        if (errorMessage != null) {
            this.errorMessage = errorMessage;
        }
    }

    private void transition(TaskState newState) {
        if (!TaskStates.canTransitionTo(state, newState)) {
            throw new IllegalStateException(
                    "Cannot transition task %d from %s to %s".formatted(taskIndex, state, newState));
        }
        this.state = newState;
    }

    public void setExitCode(Integer exitCode) { this.exitCode = exitCode; }
}
