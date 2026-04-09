package com.scheduler.sdk.meta;

import java.util.List;

/**
 * Immutable description of a single task within a job, derived from a {@code @Task}-annotated method.
 *
 * <p>Used exclusively by generated {@code _Descriptor} classes to describe the task graph.
 * The coordinator and worker read this to understand execution order and dependency relationships.
 *
 * <p><b>Not for application use.</b> Job authors declare tasks via {@code @Task} annotations.
 *
 * @param name      task name, from {@code @Task(name = ...)}
 * @param order     execution priority hint, from {@code @Task(order = ...)}
 * @param dependsOn names of tasks that must complete before this one starts
 * @param critical  if true, failure of this task fails the entire job
 */
public record TaskDescriptor(String name, int order, List<String> dependsOn, boolean critical) {

    public TaskDescriptor {
        dependsOn = List.copyOf(dependsOn);
    }
}
