package com.scheduler.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method to run after all {@code @Task} methods. At most one per {@code @Job} class.
 * Also invoked on a best-effort basis if a task fails.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface AfterJob {
}
