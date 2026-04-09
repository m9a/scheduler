package com.scheduler.sdk.meta;

/**
 * Immutable description of a parameter that a job accepts, derived from {@code @Param}
 * on constructor parameters.
 *
 * <p>Used exclusively by generated {@code _Descriptor} classes to describe the parameter schema.
 * The coordinator reads this to validate job submissions before dispatching to a worker.
 *
 * <p><b>Not for application use.</b> Job authors declare parameters via {@code @Param} annotations.
 *
 * @param name         parameter name, from {@code @Param("name")}
 * @param type         Java type (String.class, int.class, etc.)
 * @param required     true if no {@code defaultValue} was specified
 * @param defaultValue fallback value as a string, or null if required
 */
public record ParamDescriptor(String name, Class<?> type, boolean required, String defaultValue) {
}
