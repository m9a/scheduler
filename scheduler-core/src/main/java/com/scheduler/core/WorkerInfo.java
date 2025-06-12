package com.scheduler.core;

import java.time.Instant;
import java.util.Objects;

public record WorkerInfo(
        String id,
        String hostname,
        int capacity,
        Instant registeredAt,
        Instant lastHeartbeat
) {

    public WorkerInfo {
        Objects.requireNonNull(id, "Worker ID must not be null");
        Objects.requireNonNull(hostname, "Hostname must not be null");
    }

    public WorkerInfo withLastHeartbeat(Instant lastHeartbeat) {
        return new WorkerInfo(id, hostname, capacity, registeredAt, lastHeartbeat);
    }
}
