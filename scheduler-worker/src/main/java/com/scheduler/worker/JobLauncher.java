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
 * Runs the job container and handles its files. It does not talk to the job
 * process or the coordinator — {@link JobCallbackHandler} and
 * {@link CoordinatorClient} do that. Only {@link WorkerAgent#executeJob} calls it.
 *
 * <p>The container runs <b>detached</b> ({@code docker run -d}, no {@code --rm}).
 * The Docker daemon owns it, not the worker process. A worker crash does not kill
 * the job, and an exited container keeps its exit code readable. Everything is
 * addressed by the {@code job-{id}} name, so a restarted worker can re-attach with
 * the same calls.
 *
 * <pre>
 * WorkerAgent ──► JobLauncher ──docker──► job container (daemon-owned)
 *   stageInputFiles()    object store → /tmp/jobs/{id}/input
 *   spawn()              docker run -d --name job-{id} ...   (returns at once)
 *                        docker logs -f  → stdout.log        (follower thread)
 *                        docker wait     → exit code         (blocks until exit)
 *                        docker rm       after the exit code is read
 *   uploadOutputs()      /tmp/jobs/{id}/output + stdout.log → object store
 *   cleanupTempDirs()    rm /tmp/jobs/{id}
 * </pre>
 */
class JobLauncher {

    private static final Logger log = LoggerFactory.getLogger(JobLauncher.class);

    // spawn() return code for a successful job (docker exit codes are >= 0).
    static final int EXIT_SUCCESS = 0;

    private final ObjectStore objectStore;
    private final String dockerNetwork;
    private final String mlflowTrackingUri;
    private final int shutdownGraceSeconds;
    // Bound for `docker run -d` — it includes the image pull, which can be slow.
    // From worker config `docker.imagePullTimeoutMinutes`.
    private final Duration imagePullTimeout;

    JobLauncher(ObjectStore objectStore, String dockerNetwork, String mlflowTrackingUri,
                int shutdownGraceSeconds, Duration imagePullTimeout) {
        this.objectStore = objectStore;
        this.dockerNetwork = dockerNetwork;
        this.mlflowTrackingUri = mlflowTrackingUri;
        this.shutdownGraceSeconds = shutdownGraceSeconds;
        this.imagePullTimeout = imagePullTimeout;
    }

    /**
     * Launches the container detached, follows its logs, and blocks on
     * {@code docker wait} until the container exits. Returns its exit code.
     *
     * <p>There is no run deadline — a training job may legitimately run for days.
     * A container that goes silent is killed by the {@link JobLivenessMonitor},
     * and cancel/preempt commands stop it via {@link #stopContainer}; both make
     * the wait return.
     */
    int spawn(JobDetails details, Path inputDir, Path outputDir, Path logFile,
              Map<String, String> params) throws IOException, InterruptedException {
        List<String> command = buildCommand(details, inputDir, outputDir, params, dockerNetwork, mlflowTrackingUri);
        log.info("Starting job container: {}", String.join(" ", command));

        String containerName = "job-" + details.jobId();

        // Step 1: create the container. `docker run -d` pulls the image if absent,
        // creates the container, and returns — the job has not produced anything
        // yet at this point. If the pull/create exceeds imagePullTimeout, we give
        // up and throw; the agent reports the job FAILED / PROCESS_START_FAILED.
        // No TIMEOUT state is involved — the job never started.
        Process run = new ProcessBuilder(command).redirectErrorStream(true).start();
        if (!run.waitFor(imagePullTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
            run.destroyForcibly();
            // The daemon may have created the container despite the client dying.
            removeContainer(details.jobId());
            throw new IOException("docker run -d did not return within " + imagePullTimeout
                    + " for " + containerName);
        }
        // The output is tiny (container id or an error) — safe to read after exit.
        String runOutput = new String(run.getInputStream().readAllBytes()).trim();
        if (run.exitValue() != 0) {
            throw new IOException("docker run -d failed for " + containerName + ": " + runOutput);
        }

        Thread logFollower = followLogs(details.jobId(), containerName, logFile);

        // Step 2: block until the container exits. `docker wait` prints the exit
        // code when the container stops, and returns at once if it already exited.
        // This wait has no time bound on purpose — a training job may run for days.
        // It cannot hang forever, because something always makes the container
        // exit: the job finishing, the liveness monitor killing a silent container,
        // or a cancel/preempt stopping it. Each of those ends this wait.
        Process wait = new ProcessBuilder("docker", "wait", containerName)
                .redirectErrorStream(true)
                .start();
        wait.waitFor();

        String waitOutput = new String(wait.getInputStream().readAllBytes()).trim();
        int exitCode;
        try {
            exitCode = Integer.parseInt(waitOutput);
        } catch (NumberFormatException e) {
            finalizeContainer(details.jobId(), logFollower);
            throw new IOException("docker wait returned no exit code for " + containerName + ": " + waitOutput);
        }
        finalizeContainer(details.jobId(), logFollower);
        log.info("Job container finished: jobId={}, exitCode={}", details.jobId(), exitCode);
        return exitCode;
    }

    /**
     * Cleanup shared by every spawn() exit: flush the last log lines (before the
     * agent uploads stdout.log), then remove the container. Outputs live on the
     * mounted volume, not inside the container, so removal loses nothing.
     */
    private void finalizeContainer(String jobId, Thread logFollower) throws InterruptedException {
        awaitLogFollower(logFollower, jobId);
        removeContainer(jobId);
    }

    /**
     * Follows the container's output with {@code docker logs -f} on a daemon
     * thread. Each line goes to the worker log and the job's log file. It reads by
     * container name, so it also works for a re-attached container. The thread
     * ends on its own when the container stops.
     */
    private Thread followLogs(String jobId, String containerName, Path logFile) {
        Thread follower = new Thread(() -> {
            try {
                Process logs = new ProcessBuilder("docker", "logs", "-f", containerName)
                        .redirectErrorStream(true)
                        .start();
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(logs.getInputStream()));
                     BufferedWriter logWriter = Files.newBufferedWriter(logFile)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        log.info("[job:{}] {}", jobId, line);
                        logWriter.write(line);
                        logWriter.newLine();
                    }
                } finally {
                    logs.destroyForcibly();
                }
            } catch (IOException e) {
                log.warn("Log follower for jobId={} ended with error: {}", jobId, e.getMessage());
            }
        }, "job-logs-" + jobId);
        follower.setDaemon(true);
        follower.start();
        return follower;
    }

    /** Bounded join: a wedged `docker logs` must never hold up job finalization. */
    private void awaitLogFollower(Thread follower, String jobId) throws InterruptedException {
        follower.join(TimeUnit.SECONDS.toMillis(10));
        if (follower.isAlive()) {
            log.warn("Log follower for jobId={} still running after 10s; abandoning it (daemon thread)", jobId);
        }
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

    /**
     * Gracefully stops a job's container: {@code docker stop} sends SIGTERM, then
     * SIGKILL after {@code shutdownGraceSeconds}. This gives the job's
     * {@code @OnShutdown} hook time to run.
     */
    void stopContainer(String jobId) {
        String containerName = "job-" + jobId;
        try {
            Process stop = new ProcessBuilder(
                    "docker", "stop", "-t", String.valueOf(shutdownGraceSeconds), containerName)
                    .redirectErrorStream(true)
                    .start();
            stop.getInputStream().readAllBytes();
            stop.waitFor(shutdownGraceSeconds + 10L, TimeUnit.SECONDS);
            log.info("Gracefully stopped container: {}", containerName);
        } catch (Exception e) {
            log.warn("Failed to stop container {}: {}", containerName, e.getMessage());
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
        // Detached, no --rm: the daemon owns the container, and an exited container
        // must keep its exit code readable until spawn() removes it.
        command.add("-d");
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
