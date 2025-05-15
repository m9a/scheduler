package com.scheduler.sdk;

/**
 * A named stage within a job. Job authors implement this interface
 * for each stage of their pipeline.
 *
 * <pre>
 * public class ExtractTask implements Task {
 *     public String name() { return "extract"; }
 *     public void execute() { ... }
 * }
 * </pre>
 */
public interface Task {

    String name();

    void execute() throws Exception;
}
