package com.scheduler.worker;

import com.scheduler.core.ObjectStore;
import com.scheduler.proto.v1.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Runs the job container and handles its files — no communication with either
 * the job process or the coordinator (those are {@link JobCallbackServer} and
 * {@link CoordinatorClient} respectively). Called only by
 * {@link WorkerAgent#executeJob}: stage inputs from the object store, spawn
 * {@code docker run} and wait (with timeout), upload outputs/logs, clean up.
 *
 * <pre>
 * WorkerAgent ──► JobLauncher ──docker run──► job container
 *   stageInputFiles()    object store → /tmp/jobs/{id}/input
 *   spawn()              docker run --name job-{id} ... (blocks until exit/timeout)
 *   uploadOutputs()      /tmp/jobs/{id}/output + stdout.log → object store
 *   cleanupTempDirs()    rm /tmp/jobs/{id}
 * </pre>
 */
class JobLauncher {

    private static final Logger log = LoggerFactory.getLogger(JobLauncher.class);

    private final ObjectStore objectStore;
    private final Duration jobExecutionTimeout;
    private final String dockerNetwork;
    private final String mlflowTrackingUri;

    JobLauncher(ObjectStore objectStore, Duration jobExecutionTimeout,
                String dockerNetwork, String mlflowTrackingUri) {
        this.objectStore = objectStore;
        this.jobExecutionTimeout = jobExecutionTimeout;
        this.dockerNetwork = dockerNetwork;
        this.mlflowTrackingUri = mlflowTrackingUri;
    }

    /**
     * Spawns the job container with volume mounts for input/output, tees its
     * stdout/stderr to a log file and the logger, then waits for exit.
     * Returns the exit code, or -1 if the job hit the execution timeout.
     *
     * <p>{@code onTimeout} fires when the deadline is hit, <b>before</b> the kill
     * starts — the agent reports TIMEOUT there so the coordinator shows the job
     * as "kill in flight" during the docker rm below, which can take seconds.
     * The KILLED report happens in the agent once spawn returns -1 (kill confirmed).
     */
    int spawn(JobDetails details, Path inputDir, Path outputDir, Path logFile,
              Map<String, String> params, Runnable onTimeout) throws IOException, InterruptedException {
        List<String> command = buildCommand(details, inputDir, outputDir, params, dockerNetwork, mlflowTrackingUri);
        log.info("Starting job process: {}", String.join(" ", command));

        ProcessBuilder pb = new ProcessBuilder(command)
                .redirectErrorStream(true);

        Process process = pb.start();

        // Tee stdout/stderr to both log file and logger
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
             BufferedWriter logWriter = Files.newBufferedWriter(logFile)) {
            String line;
            while ((line = reader.readLine()) != null) {
                log.info("[job:{}] {}", details.jobId(), line);
                logWriter.write(line);
                logWriter.newLine();
            }
        }

        boolean finished = process.waitFor(jobExecutionTimeout.toMillis(), TimeUnit.MILLISECONDS);
        if (!finished) {
            log.warn("Job process timed out after {}: jobId={}", jobExecutionTimeout, details.jobId());
            onTimeout.run();
            process.destroyForcibly();
            // destroyForcibly kills the `docker run` CLI process but the container
            // keeps running. Force-remove it by name so nothing is left dangling.
            removeContainer(details.jobId());
            return -1;
        }

        int exitCode = process.exitValue();
        log.info("Job process finished: jobId={}, exitCode={}", details.jobId(), exitCode);
        return exitCode;
    }

    void stageInputFiles(Job job) {
        if (objectStore == null) {
            return;
        }
        for (com.scheduler.proto.v1.InputFile inputFile : job.getInputFilesList()) {
            String uri = inputFile.getUri();
            Path dest = Path.of("/tmp/jobs", job.getId(), "input", inputFile.getName());
            log.info("Downloading input file: jobId={}, name={}, uri={}", job.getId(), inputFile.getName(), uri);
            objectStore.getObject(uri, dest);
        }
    }

    void uploadOutputs(String jobId, Path outputDir, Path logFile) {
        if (objectStore == null) {
            return;
        }
        try (Stream<Path> files = Files.walk(outputDir)) {
            files.filter(Files::isRegularFile).forEach(file -> {
                String key = "jobs/" + jobId + "/output/" + outputDir.relativize(file);
                log.info("Uploading output file: jobId={}, key={}", jobId, key);
                objectStore.putObject(key, file);
            });
        } catch (IOException e) {
            log.error("Failed to upload output files for jobId={}: {}", jobId, e.getMessage(), e);
        }

        if (Files.exists(logFile)) {
            String key = "jobs/" + jobId + "/logs/stdout.log";
            log.info("Uploading stdout log: jobId={}, key={}", jobId, key);
            objectStore.putObject(key, logFile);
        }
    }

    void cleanupTempDirs(String jobId) {
        Path jobDir = Path.of("/tmp/jobs", jobId);
        try (Stream<Path> walk = Files.walk(jobDir)) {
            walk.sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException e) {
                            log.warn("Failed to delete temp file {}: {}", p, e.getMessage());
                        }
                    });
        } catch (IOException e) {
            log.warn("Failed to clean up temp dir for jobId={}: {}", jobId, e.getMessage());
        }
    }

    private static void removeContainer(String jobId) {
        String containerName = "job-" + jobId;
        try {
            Process rm = new ProcessBuilder("docker", "rm", "-f", "-v", containerName)
                    .redirectErrorStream(true)
                    .start();
            rm.getInputStream().readAllBytes();
            rm.waitFor(10, TimeUnit.SECONDS);
            log.info("Removed container: {}", containerName);
        } catch (Exception e) {
            log.warn("Failed to remove container {}: {}", containerName, e.getMessage());
        }
    }

    static List<String> buildCommand(JobDetails details, Path inputDir, Path outputDir,
                                      Map<String, String> params, String dockerNetwork,
                                      String mlflowTrackingUri) {
        List<String> command = new ArrayList<>();
        command.add("docker");
        command.add("run");
        command.add("--rm");
        command.add("--name");
        command.add("job-" + details.jobId());

        if (dockerNetwork != null) {
            command.add("--network");
            command.add(dockerNetwork);
        }

        command.add("-v");
        command.add(inputDir.toAbsolutePath() + ":/workspace/input:ro");
        command.add("-v");
        command.add(outputDir.toAbsolutePath() + ":/workspace/output");

        String containerPort = params.get("containerPort");
        if (containerPort != null) {
            command.add("-p");
            command.add("0:" + containerPort);
        }

        if (details.memoryMb() > 0) {
            command.add("--memory");
            command.add(details.memoryMb() + "m");
        }
        if (details.cpuCores() > 0) {
            command.add("--cpus");
            command.add(String.valueOf(details.cpuCores()));
        }

        command.add("-e");
        command.add("EXECUTION_PAYLOAD=" + details.payload());

        if (mlflowTrackingUri != null) {
            command.add("-e");
            command.add("MLFLOW_TRACKING_URI=" + mlflowTrackingUri);
        }

        command.add(details.artifactUri());
        return command;
    }
}
