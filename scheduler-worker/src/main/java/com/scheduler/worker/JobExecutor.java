package com.scheduler.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Spawns a child JVM process for a job. Called by {@link WorkerAgent} when
 * a job is pulled from the coordinator.
 *
 * <pre>
 * WorkerAgent ──► JobExecutor ──spawns──► Child JVM (java -jar)
 *                                          └─ JobRunner runs tasks
 *                                          └─ reports status via HTTP to StatusCallbackServer
 * </pre>
 *
 * <p>If {@code mainClass} is set:
 * <pre>java -Dscheduler.callback.url=... -Dscheduler.job.id=... -cp jarPath mainClass</pre>
 *
 * <p>If {@code mainClass} is null:
 * <pre>java -Dscheduler.callback.url=... -Dscheduler.job.id=... -jar jarPath</pre>
 */
public class JobExecutor {

    private static final Logger log = LoggerFactory.getLogger(JobExecutor.class);

    private final String jarPath;
    private final String mainClass;
    private final String jobId;
    private final String callbackUrl;

    public JobExecutor(String jarPath, String mainClass, String jobId, String callbackUrl) {
        this.jarPath = jarPath;
        this.mainClass = mainClass;
        this.jobId = jobId;
        this.callbackUrl = callbackUrl;
    }

    public int run() throws IOException, InterruptedException {
        List<String> command = buildCommand();
        log.info("Starting job process: {}", String.join(" ", command));

        ProcessBuilder pb = new ProcessBuilder(command)
                .redirectErrorStream(true);
        Process process = pb.start();

        // The naming is from the parent's perspective — process.getInputStream() is an input stream to the parent that carries the stdout/stderr output from the child. It's the
        //  parent reading what the child writes.
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                log.info("[job:{}] {}", jobId, line);
            }
        }

        int exitCode = process.waitFor();
        log.info("Job process finished: jobId={}, exitCode={}", jobId, exitCode);
        return exitCode;
    }

    List<String> buildCommand() {
        List<String> command = new ArrayList<>();
        command.add("java");
        command.add("-Dscheduler.callback.url=" + callbackUrl);
        command.add("-Dscheduler.job.id=" + jobId);
        if (mainClass != null) {
            command.add("-cp");
            command.add(jarPath);
            command.add(mainClass);
        } else {
            command.add("-jar");
            command.add(jarPath);
        }
        return command;
    }
}
