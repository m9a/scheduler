package com.scheduler.worker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class JobLauncherTest {

    /**
     * Real-docker check of the detached path: launch, follow logs, read the exit
     * code via docker wait, and remove the container. Skipped when docker isn't
     * available (same gating as IntegrationTest).
     */
    @Test
    void spawnDetached(@TempDir Path dir) throws Exception {
        assumeTrue(dockerAvailable(), "Docker not available — skipping detached spawn test");

        JobLauncher launcher = new JobLauncher(null, null, null, 2, Duration.ofMinutes(2));
        JobDetails details = new JobDetails("launcher-test", "hello-world:latest", "cGF5bG9hZA==");
        Path logFile = dir.resolve("stdout.log");

        int exitCode = launcher.spawn(details, dir.resolve("in"), dir.resolve("out"), logFile, Map.of());

        assertEquals(0, exitCode);
        assertTrue(Files.readString(logFile).contains("Hello from Docker!"),
                "log follower should capture container stdout");
        // The container must be gone: spawn removes it after reading the exit code.
        Process inspect = new ProcessBuilder("docker", "inspect", "job-launcher-test").start();
        assertTrue(inspect.waitFor(10, TimeUnit.SECONDS));
        assertNotEquals(0, inspect.exitValue(), "container should be removed after finalization");
    }

    /**
     * Real-docker check of state detection: a live container reads RUNNING, then
     * EXITED after it stops, then ABSENT once removed. This is the signal boot
     * recovery uses to pick each job's fate. Skipped without docker.
     */
    @Test
    void containerState() throws Exception {
        assumeTrue(dockerAvailable(), "Docker not available — skipping container-state test");

        JobLauncher launcher = new JobLauncher(null, null, null, 2, Duration.ofMinutes(2));
        String jobId = "state-test-" + System.nanoTime();
        String containerName = "job-" + jobId;

        // Absent before anything is created.
        assertEquals(JobLauncher.ContainerState.ABSENT, launcher.containerState(jobId));

        // A long-lived container reads RUNNING.
        runDocker("docker", "run", "-d", "--name", containerName, "alpine:latest", "sleep", "300");
        try {
            assertEquals(JobLauncher.ContainerState.RUNNING, launcher.containerState(jobId));

            runDocker("docker", "stop", "-t", "0", containerName);
            assertEquals(JobLauncher.ContainerState.EXITED, launcher.containerState(jobId));
        } finally {
            runDocker("docker", "rm", "-f", containerName);
        }

        // Absent again once removed.
        assertEquals(JobLauncher.ContainerState.ABSENT, launcher.containerState(jobId));
    }

    /**
     * A restarted worker re-attaches to a container an earlier worker started.
     * attachAndWait() must block to the exit code, capture the full log, and
     * remove the container.
     */
    @Test
    void attachAndWait(@TempDir Path dir) throws Exception {
        assumeTrue(dockerAvailable(), "Docker not available — skipping attach test");

        JobLauncher launcher = new JobLauncher(null, null, null, 2, Duration.ofMinutes(2));
        String jobId = "attach-test-" + System.nanoTime();
        String containerName = "job-" + jobId;
        Path logFile = dir.resolve("stdout.log");

        // Stands in for the container a dead worker left running.
        runDocker("docker", "run", "-d", "--name", containerName,
                "alpine:latest", "sh", "-c", "echo attach-hello; sleep 2");
        try {
            int exitCode = launcher.attachAndWait(jobId, logFile);

            assertEquals(0, exitCode);
            assertTrue(Files.readString(logFile).contains("attach-hello"),
                    "re-attached follower should capture output from the container's start");
            assertEquals(JobLauncher.ContainerState.ABSENT, launcher.containerState(jobId),
                    "container should be removed after finalization");
        } finally {
            runDocker("docker", "rm", "-f", containerName);
        }
    }

    /**
     * Recovery finds a container that already exited: salvageLogs must capture its
     * output without removing it; removal is a separate, later step (after ack).
     */
    @Test
    void salvageLogs(@TempDir Path dir) throws Exception {
        assumeTrue(dockerAvailable(), "Docker not available — skipping salvage test");

        JobLauncher launcher = new JobLauncher(null, null, null, 2, Duration.ofMinutes(2));
        String jobId = "salvage-test-" + System.nanoTime();
        String containerName = "job-" + jobId;
        Path logFile = dir.resolve("stdout.log");

        // A container that finished while the worker was down.
        runDocker("docker", "run", "-d", "--name", containerName,
                "alpine:latest", "sh", "-c", "echo salvage-hello");
        try {
            // Wait for it to exit, then salvage.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (launcher.containerState(jobId) != JobLauncher.ContainerState.EXITED
                    && System.nanoTime() < deadline) {
                Thread.sleep(100);
            }
            launcher.salvageLogs(jobId, logFile);

            assertTrue(Files.readString(logFile).contains("salvage-hello"),
                    "salvage should capture the exited container's output");
            assertEquals(JobLauncher.ContainerState.EXITED, launcher.containerState(jobId),
                    "salvage must not remove the container");

            JobLauncher.removeContainer(jobId);
            assertEquals(JobLauncher.ContainerState.ABSENT, launcher.containerState(jobId));
        } finally {
            runDocker("docker", "rm", "-f", containerName);
        }
    }

    private static void runDocker(String... command) throws Exception {
        Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
        p.getInputStream().readAllBytes();
        assertTrue(p.waitFor(30, TimeUnit.SECONDS), "docker command timed out: " + String.join(" ", command));
    }

    private static boolean dockerAvailable() {
        try {
            Process p = new ProcessBuilder("docker", "info").start();
            return p.waitFor(10, TimeUnit.SECONDS) && p.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    // Base docker run command: detached, named, volumes, payload env.
    @Test
    void buildCommand() {
        JobDetails details = new JobDetails("job-4", "registry.example.com/etl-job:v1", "cGF5bG9hZA==");
        Path inputDir = Path.of("/tmp/jobs/job-4/input");
        Path outputDir = Path.of("/tmp/jobs/job-4/output");

        List<String> command = JobLauncher.buildCommand(details, inputDir, outputDir,
                Map.of(), null, null);

        assertEquals(List.of(
                "docker",
                "run", "-d",
                "--name", "job-job-4",
                "-v", inputDir.toAbsolutePath() + ":/workspace/input:ro",
                "-v", outputDir.toAbsolutePath() + ":/workspace/output",
                "-e", "EXECUTION_PAYLOAD=cGF5bG9hZA==",
                "registry.example.com/etl-job:v1"
        ), command);
    }

    // A bare image tag (no registry) passes through unchanged.
    @Test
    void buildCommandShortTag() {
        JobDetails details = new JobDetails("job-5", "my-job:latest", "cGF5bG9hZA==");
        Path inputDir = Path.of("/tmp/jobs/job-5/input");
        Path outputDir = Path.of("/tmp/jobs/job-5/output");

        List<String> command = JobLauncher.buildCommand(details, inputDir, outputDir,
                Map.of(), null, null);

        assertEquals(List.of(
                "docker",
                "run", "-d",
                "--name", "job-job-5",
                "-v", inputDir.toAbsolutePath() + ":/workspace/input:ro",
                "-v", outputDir.toAbsolutePath() + ":/workspace/output",
                "-e", "EXECUTION_PAYLOAD=cGF5bG9hZA==",
                "my-job:latest"
        ), command);
    }

    // A containerPort param publishes the port on an ephemeral host port.
    @Test
    void buildCommandPort() {
        JobDetails details = new JobDetails("job-6", "my-server:latest", "cGF5bG9hZA==");
        Path inputDir = Path.of("/tmp/jobs/job-6/input");
        Path outputDir = Path.of("/tmp/jobs/job-6/output");

        List<String> command = JobLauncher.buildCommand(details, inputDir, outputDir,
                Map.of("containerPort", "8080"), null, null);

        assertEquals(List.of(
                "docker",
                "run", "-d",
                "--name", "job-job-6",
                "-v", inputDir.toAbsolutePath() + ":/workspace/input:ro",
                "-v", outputDir.toAbsolutePath() + ":/workspace/output",
                "-p", "0:8080",
                "-e", "EXECUTION_PAYLOAD=cGF5bG9hZA==",
                "my-server:latest"
        ), command);
    }

    // Docker network and MLflow tracking URI flow into the command when configured.
    @Test
    void buildCommandNetworkMlflow() {
        JobDetails details = new JobDetails("job-7", "training:latest", "cGF5bG9hZA==");
        Path inputDir = Path.of("/tmp/jobs/job-7/input");
        Path outputDir = Path.of("/tmp/jobs/job-7/output");

        List<String> command = JobLauncher.buildCommand(details, inputDir, outputDir,
                Map.of(), "scheduler-net", "http://mlflow:5000");

        assertEquals(List.of(
                "docker",
                "run", "-d",
                "--name", "job-job-7",
                "--network", "scheduler-net",
                "-v", inputDir.toAbsolutePath() + ":/workspace/input:ro",
                "-v", outputDir.toAbsolutePath() + ":/workspace/output",
                "-e", "EXECUTION_PAYLOAD=cGF5bG9hZA==",
                "-e", "MLFLOW_TRACKING_URI=http://mlflow:5000",
                "training:latest"
        ), command);
    }
}
