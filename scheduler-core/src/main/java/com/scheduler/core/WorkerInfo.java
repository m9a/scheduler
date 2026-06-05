package com.scheduler.core;

import java.time.Instant;
import java.util.Objects;
import java.util.Set;

public record WorkerInfo(
        String id,
        String hostname,
        int memoryMb,
        int cpuCores,
        boolean gpu,
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

    public WorkerInfo withLastHeartbeat(Instant lastHeartbeat) {
        return new WorkerInfo(id, hostname, memoryMb, cpuCores, gpu, capabilities,
                registeredAt, lastHeartbeat);
    }
}
