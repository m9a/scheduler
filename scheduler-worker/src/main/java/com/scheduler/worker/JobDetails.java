package com.scheduler.worker;

import java.util.Objects;

/**
 * Immutable snapshot of the information needed to spawn a job process.
 * Extracted from the proto Job message by WorkerAgent before spawning.
 */
record JobDetails(
        String jobId,
        String jarPath,
        String mainClass,
        String payload
) {

    public JobDetails {
        Objects.requireNonNull(jobId);
        Objects.requireNonNull(jarPath);
    }
}
