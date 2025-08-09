package com.scheduler.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as a task within a {@link Job}-annotated class.
 * Methods are invoked in topological order (by {@code order} then dependency graph).
 *
 * <pre>
 * {@literal @}Task(name = "extract", order = 1)
 * public void extract(@Context JobContext ctx) { ... }
 * </pre>
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface Task {

    String name();

    int order() default 0;

    String[] dependsOn() default {};

    boolean critical() default false;
}
