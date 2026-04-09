package com.scheduler.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a constructor parameter as a job parameter. The value is extracted from
 * {@link com.scheduler.sdk.ExecutionPayload} at runtime.
 *
 * <pre>
 * public DailySalesEtlJob(@Param("region") String region,
 *                         @Param(value = "batchSize", defaultValue = "1000") int batchSize) {
 * </pre>
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.PARAMETER)
public @interface Param {

    String value();

    String defaultValue() default "";
}
