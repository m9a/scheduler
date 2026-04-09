package com.scheduler.sdk;

/**
 * A named stage within a job. Job authors implement this interface
 * for each stage of their pipeline.
 *
 * <pre>
 * public class ExtractTask implements Task {
 *     public String name() { return "extract"; }
 *     public void execute(TaskContext ctx) {
 *         ctx.progress(0.5, "halfway");
 *         // ... do work ...
 *     }
 * }
 * </pre>
 */
public interface Task {

    String name();

    void execute(TaskContext ctx) throws Exception;
}
