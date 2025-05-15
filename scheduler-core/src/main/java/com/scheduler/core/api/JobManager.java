package com.scheduler.core.api;

import com.scheduler.core.Job;
import com.scheduler.core.JobExecution;
import com.scheduler.core.TaskStatus;

import java.util.Optional;

public interface JobManager {

    JobExecution submit(Job job);

    JobExecution getJob(String jobId);

    /**
     * Atomically claims the next QUEUED job, transitions it to STARTING,
     * and returns it. Returns empty if no jobs are available.
     */
    Optional<JobExecution> claimNextJob(String workerId);

    /**
     * Updates the status of a task within a job. Transitions the job status
     * as needed (STARTING→RUNNING on first task, RUNNING→COMPLETED when all
     * tasks finish, RUNNING→FAILED when a task fails).
     */
    void updateTaskStatus(String jobId, int taskIndex, TaskStatus status, String errorMessage);
}
