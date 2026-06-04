package com.scheduler.core;

import java.time.Instant;
import java.util.Objects;
import java.util.Set;

public record WorkerInfo(
        String id,
        String hostname,
        int capacity,
        int memoryMb,
        int cpuCores,
        Set<String> capabilities,
        Instant registeredAt,
        Instant lastHeartbeat
) {

    public WorkerInfo {
        Objects.requireNonNull(id, "Worker ID must not be null");
        Objects.requireNonNull(hostname, "Hostname must not be null");
        if (capabilities == null) {
            capabilities = Set.of();
        }
    }

    public WorkerInfo(String id, String hostname, int capacity,
                      Instant registeredAt, Instant lastHeartbeat) {
        this(id, hostname, capacity, 0, 0, Set.of(), registeredAt, lastHeartbeat);
    }

    public WorkerInfo withLastHeartbeat(Instant lastHeartbeat) {
        return new WorkerInfo(id, hostname, capacity, memoryMb, cpuCores, capabilities,
                registeredAt, lastHeartbeat);
    }
}
