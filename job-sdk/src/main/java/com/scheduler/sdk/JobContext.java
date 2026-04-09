package com.scheduler.sdk;

/**
 * Context available to {@code @Task} methods in annotation-based jobs.
 * Provides inter-task data passing and progress reporting.
 *
 * <p>Injected into {@code @Task} methods via the {@code @Context} parameter annotation.
 * The generated {@code _Harness} creates a {@link DefaultJobContext} backed by a
 * {@link JobReporter} that sends status updates to WorkerAgent.
 */
public interface JobContext {

    void report(TaskStatus status, String message);

    <T> void put(String key, T value);

    <T> T get(String key, Class<T> type);

    boolean isCancelled();
}
