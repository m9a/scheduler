package com.scheduler.sdk;

/**
 * Task execution status reported by JobRunner (in the job process) to WorkerAgent (in the worker JVM).
 */
public enum TaskStatus {
    RUNNING,
    COMPLETED,
    FAILED
}
