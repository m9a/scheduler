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
    void spawnDetachedCapturesExitCodeAndLogs(@TempDir Path dir) throws Exception {
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

    private static boolean dockerAvailable() {
        try {
            Process p = new ProcessBuilder("docker", "info").start();
            return p.waitFor(10, TimeUnit.SECONDS) && p.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    @Test
    void buildCommandDocker() {
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

    @Test
    void buildCommandDockerShortTag() {
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

    @Test
    void buildCommandWithContainerPort() {
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

    @Test
    void buildCommandWithNetworkAndMlflow() {
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
