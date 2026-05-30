package com.scheduler.core;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public record Job(
        String name,
        String artifactUri,
        Map<String, String> params,
        int priority,
        List<InputFile> inputFiles
) {

    public Job {
        Objects.requireNonNull(name, "Job name must not be null");
        Objects.requireNonNull(artifactUri, "Artifact URI must not be null");
        if (params == null) {
            params = Map.of();
        }
        if (inputFiles == null) {
            inputFiles = List.of();
        }
    }

    public Job(String name, String artifactUri, Map<String, String> params, int priority) {
        this(name, artifactUri, params, priority, List.of());
    }
}
