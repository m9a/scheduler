package com.scheduler.sdk.meta;

import java.util.Set;

/**
 * Resource hints for a job, derived from {@code @ResourceProfile} within {@code @Job}.
 *
 * <p>Used exclusively by generated {@code _Descriptor} classes. The coordinator reads this
 * to match jobs to workers with sufficient capacity.
 *
 * <p><b>Not for application use.</b> Job authors declare resources via
 * {@code @Job(resource = @ResourceProfile(...))}.
 *
 * @param memoryMb minimum memory in megabytes (0 = no preference)
 * @param cpuCores minimum CPU cores (0 = no preference)
 * @param labels   required worker labels (e.g., "gpu", "high-memory")
 */
public record ResourceRequirements(int memoryMb, int cpuCores, Set<String> labels) {

    public static final ResourceRequirements DEFAULT = new ResourceRequirements(0, 0, Set.of());

    public ResourceRequirements {
        labels = Set.copyOf(labels);
    }
}
