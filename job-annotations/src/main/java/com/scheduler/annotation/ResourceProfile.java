package com.scheduler.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Declares resource requirements for a {@link Job}. Used as a nested annotation
 * within {@code @Job(resource = @ResourceProfile(...))}.
 */
@Retention(RetentionPolicy.SOURCE)
public @interface ResourceProfile {

    int minMemoryMb() default 0;

    int cpuCores() default 0;

    String[] labels() default {};
}
