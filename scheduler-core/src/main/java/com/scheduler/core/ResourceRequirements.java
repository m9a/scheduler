package com.scheduler.core;

import java.util.Set;

public record ResourceRequirements(int memoryMb, int cpuCores, boolean gpu, Set<String> capabilities) {

    public static final ResourceRequirements DEFAULT = new ResourceRequirements(512, 1, false, Set.of());

    public ResourceRequirements {
        capabilities = capabilities == null ? Set.of() : Set.copyOf(capabilities);
    }

    public boolean satisfiedBy(int workerMemoryMb, int workerCpuCores, boolean workerGpu, Set<String> workerCapabilities) {
        // GPU is an exact match: GPU jobs run only on GPU workers, and GPU workers are
        // reserved for GPU jobs so a CPU-only workload never occupies scarce GPU hardware.
        if (gpu != workerGpu) return false;
        if (workerMemoryMb < memoryMb) return false;
        if (workerCpuCores < cpuCores) return false;
        return workerCapabilities.containsAll(capabilities);
    }
}
