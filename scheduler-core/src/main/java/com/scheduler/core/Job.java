package com.scheduler.core;

import java.util.Objects;

public record Job(
        String name,
        String jarPath,
        String mainClass,
        int priority
) {

    public Job {
        Objects.requireNonNull(name, "Job name must not be null");
        Objects.requireNonNull(jarPath, "Jar path must not be null");
    }
}
