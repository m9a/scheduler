package com.scheduler.sdk;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Default implementation of {@link JobContext} used by generated {@code _Harness} classes.
 * Stores inter-task data in a {@link ConcurrentHashMap} and delegates status reporting
 * to {@link JobReporter}.
 */
public final class DefaultJobContext implements JobContext {

    private final JobReporter reporter;
    private final ConcurrentHashMap<String, Object> data = new ConcurrentHashMap<>();
    private volatile boolean cancelled;

    public DefaultJobContext(JobReporter reporter) {
        this.reporter = reporter;
    }

    @Override
    public void report(TaskStatus status, String message) {
        reporter.report(status, message);
    }

    @Override
    public <T> void put(String key, T value) {
        data.put(key, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(String key, Class<T> type) {
        Object value = data.get(key);
        if (value == null) {
            return null;
        }
        return (T) value;
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    public void cancel() {
        cancelled = true;
    }
}
