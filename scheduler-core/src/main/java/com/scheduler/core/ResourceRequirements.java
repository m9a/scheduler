package com.scheduler.core;

import java.util.Set;

public record ResourceRequirements(int memoryMb, int cpuCores, Set<String> capabilities) {

    public static final ResourceRequirements NONE = new ResourceRequirements(0, 0, Set.of());

    public ResourceRequirements {
        capabilities = capabilities == null ? Set.of() : Set.copyOf(capabilities);
    }

    public boolean satisfiedBy(int workerMemoryMb, int workerCpuCores, Set<String> workerCapabilities) {
        if (memoryMb > 0 && workerMemoryMb < memoryMb) return false;
        if (cpuCores > 0 && workerCpuCores < cpuCores) return false;
        return workerCapabilities.containsAll(capabilities);
    }
}
