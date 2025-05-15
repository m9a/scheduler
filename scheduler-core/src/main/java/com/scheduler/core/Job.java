package com.scheduler.core;

import java.util.List;
import java.util.Objects;

public record Job(
        String name,
        String jarPath,
        String mainClass,
        List<Task> tasks,
        int priority
) {

    public Job {
        Objects.requireNonNull(name, "Job name must not be null");
        Objects.requireNonNull(jarPath, "Jar path must not be null");
        Objects.requireNonNull(tasks, "Tasks must not be null");
        if (tasks.isEmpty()) {
            throw new IllegalArgumentException("Job must have at least one task");
        }
        tasks = List.copyOf(tasks);
    }
}
