package com.scheduler.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class as a job definition. The annotation processor generates a
 * {@code _Descriptor} (metadata) and {@code _Harness} (entry point) at compile time.
 *
 * <pre>
 * {@literal @}Job(id = "daily-sales-etl", description = "Daily sales ETL pipeline")
 * public class DailySalesEtlJob {
 *     // @Task methods, @Param constructor params, etc.
 * }
 * </pre>
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.TYPE)
public @interface Job {

    String id();

    String description() default "";

    int timeoutSeconds() default 0;

    int maxRetries() default 0;

    ResourceProfile resource() default @ResourceProfile;
}
