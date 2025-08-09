package com.scheduler.worker;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JobExecutorTest {

    @Test
    void buildCommandWithMainClass() {
        JobDetails details = new JobDetails("job-1", "/opt/jobs/etl.jar", "com.example.EtlJob", null);

        List<String> command = WorkerAgent.buildCommand(details);

        assertEquals(List.of(
                "java",
                "-cp", "/opt/jobs/etl.jar",
                "com.example.EtlJob"
        ), command);
    }

    @Test
    void buildCommandWithJar() {
        JobDetails details = new JobDetails("job-2", "/opt/jobs/etl.jar", null, null);

        List<String> command = WorkerAgent.buildCommand(details);

        assertEquals(List.of(
                "java",
                "-jar", "/opt/jobs/etl.jar"
        ), command);
    }
}
