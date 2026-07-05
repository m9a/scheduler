package com.scheduler.core;

import com.scheduler.proto.v1.JobState;

import java.util.Set;

/**
 * Job lifecycle transition rules, operating on the proto {@link JobState} enum
 * (the single state type — see CLAUDE.md "One status message"). The rules mirror
 * the README "Job Lifecycle" table; {@link JobStatus} enforces them on every
 * transition.
 */
public final class JobStates {

    private JobStates() {}

    private static final Set<JobState> TERMINAL = Set.of(
            JobState.JOB_STATE_COMPLETED, JobState.JOB_STATE_FAILED,
            JobState.JOB_STATE_KILLED, JobState.JOB_STATE_CANCELLED);

    public static boolean isTerminal(JobState state) {
        return TERMINAL.contains(state);
    }

    public static boolean canTransitionTo(JobState from, JobState to) {
        if (from == to) {
            return false;
        }
        return switch (from) {
            case JOB_STATE_QUEUED -> to == JobState.JOB_STATE_STARTING || to == JobState.JOB_STATE_CANCELLED;
            // You can fail to start, but you can't succeed without running.
            case JOB_STATE_STARTING -> to == JobState.JOB_STATE_RUNNING
                    || to == JobState.JOB_STATE_FAILED || to == JobState.JOB_STATE_KILLED
                    || to == JobState.JOB_STATE_CANCELLED;
            case JOB_STATE_RUNNING -> to == JobState.JOB_STATE_COMPLETED
                    || to == JobState.JOB_STATE_FAILED || to == JobState.JOB_STATE_KILLED
                    || to == JobState.JOB_STATE_CANCELLED;
            // Terminal states and UNSPECIFIED have no outgoing transitions.
            default -> false;
        };
    }
}
