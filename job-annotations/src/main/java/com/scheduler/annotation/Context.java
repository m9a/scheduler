package com.scheduler.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a {@code @Task} method parameter to receive the {@link com.scheduler.sdk.JobContext}.
 *
 * <pre>
 * {@literal @}Task(name = "extract")
 * public void extract(@Context JobContext ctx, @Param("region") String region) { ... }
 * </pre>
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.PARAMETER)
public @interface Context {
}
