package com.scheduler.worker;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JobLauncherTest {

    @Test
    void buildCommandDocker() {
        JobDetails details = new JobDetails("job-4", "registry.example.com/etl-job:v1", "cGF5bG9hZA==");
        Path inputDir = Path.of("/tmp/jobs/job-4/input");
        Path outputDir = Path.of("/tmp/jobs/job-4/output");

        List<String> command = JobLauncher.buildCommand(details, inputDir, outputDir,
                Map.of(), null, null);

        assertEquals(List.of(
                "docker",
                "run", "--rm",
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
                "run", "--rm",
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
                "run", "--rm",
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
                "run", "--rm",
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
