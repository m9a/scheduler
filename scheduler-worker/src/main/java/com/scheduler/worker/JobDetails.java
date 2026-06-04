package com.scheduler.worker;

import java.util.Objects;

/**
 * Immutable snapshot of the information needed to spawn a job process.
 * Extracted from the proto Job message by WorkerAgent before spawning.
 */
record JobDetails(
        String jobId,
        String artifactUri,
        String payload,
        int memoryMb,
        int cpuCores
) {

    public JobDetails {
        Objects.requireNonNull(jobId);
        Objects.requireNonNull(artifactUri);
    }

    JobDetails(String jobId, String artifactUri, String payload) {
        this(jobId, artifactUri, payload, 0, 0);
    }
}
