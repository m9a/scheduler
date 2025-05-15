package com.scheduler.worker;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JobExecutorTest {

    @Test
    void buildCommandWithMainClass() {
        JobExecutor process = new JobExecutor(
                "/opt/jobs/etl.jar", "com.example.EtlJob", "job-1", "http://localhost:8080");

        List<String> command = process.buildCommand();

        assertEquals(List.of(
                "java",
                "-Dscheduler.callback.url=http://localhost:8080",
                "-Dscheduler.job.id=job-1",
                "-cp", "/opt/jobs/etl.jar",
                "com.example.EtlJob"
        ), command);
    }

    @Test
    void runHelloWorld() throws IOException, InterruptedException {
        String classpath = System.getProperty("java.class.path");
        JobExecutor process = new JobExecutor(
                classpath, "com.scheduler.worker.HelloWorld", "test-job", "http://localhost:0");

        int exitCode = process.run();

        assertEquals(0, exitCode);
    }

    @Test
    void buildCommandWithJar() {
        JobExecutor process = new JobExecutor(
                "/opt/jobs/etl.jar", null, "job-2", "http://localhost:9090");

        List<String> command = process.buildCommand();

        assertEquals(List.of(
                "java",
                "-Dscheduler.callback.url=http://localhost:9090",
                "-Dscheduler.job.id=job-2",
                "-jar", "/opt/jobs/etl.jar"
        ), command);
    }
}
