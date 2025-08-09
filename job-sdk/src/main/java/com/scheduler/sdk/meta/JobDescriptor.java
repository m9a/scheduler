package com.scheduler.sdk.meta;

import java.util.List;

/**
 * Read-only catalog entry for a {@code @Job}-annotated class.
 *
 * <p>Generated at compile time by the annotation processor. Each {@code @Job} class produces
 * a {@code <ClassName>_Descriptor} that implements this interface. The worker agent discovers
 * all descriptors at startup via {@link java.util.ServiceLoader}:
 *
 * <pre>
 * ServiceLoader.load(JobDescriptor.class).forEach(descriptor -> {
 *     // descriptor.id()         → "daily-sales-etl"
 *     // descriptor.parameters() → what params the job accepts
 *     // descriptor.tasks()      → what tasks it runs and their order
 *     // descriptor.resources()  → how much memory/CPU it needs
 * });
 * </pre>
 *
 * <p>This allows the coordinator and worker to inspect a job's schema, validate submissions,
 * and match resource requirements — all without instantiating the job class.
 *
 * <p><b>Not for application use.</b> This interface is implemented only by generated
 * {@code _Descriptor} classes. Job authors interact with {@code @Job}, {@code @Task},
 * and {@code @Param} annotations instead.
 */
public interface JobDescriptor {

    /** Unique job identifier, from {@code @Job(id = ...)}. */
    String id();

    /** Human-readable description, from {@code @Job(description = ...)}. */
    String description();

    /** Maximum wall-clock seconds before the job is killed, from {@code @Job(timeoutSeconds = ...)}. */
    int timeoutSeconds();

    /** How many times a failed job may be retried, from {@code @Job(maxRetries = ...)}. */
    int maxRetries();

    /** Memory, CPU, and label requirements, from {@code @ResourceProfile}. */
    ResourceRequirements resources();

    /** Schema of parameters the job accepts, derived from {@code @Param} on the constructor. */
    List<ParamDescriptor> parameters();

    /** Ordered list of tasks the job executes, derived from {@code @Task} methods. */
    List<TaskDescriptor> tasks();

    /** The annotated job class itself, for instantiation by the generated harness. */
    Class<?> jobClass();
}
