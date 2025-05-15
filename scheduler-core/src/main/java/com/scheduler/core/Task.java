package com.scheduler.core;

import java.util.Objects;

public record Task(String name) {

    public Task {
        Objects.requireNonNull(name, "Task name must not be null");
    }
}
