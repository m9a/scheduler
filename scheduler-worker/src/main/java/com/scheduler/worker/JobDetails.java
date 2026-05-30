package com.scheduler.worker;

import java.util.Objects;

/**
 * Immutable snapshot of the information needed to spawn a job process.
 * Extracted from the proto Job message by WorkerAgent before spawning.
 */
record JobDetails(
        String jobId,
        String artifactUri,
        String payload
) {

    public JobDetails {
        Objects.requireNonNull(jobId);
        Objects.requireNonNull(artifactUri);
    }
}
