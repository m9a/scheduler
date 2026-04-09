package com.scheduler.sdk;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JobProcessTest {

    private HttpServer server;
    private String callbackUrl;
    private List<TaskStatusUpdate> updates;

    @BeforeEach
    void setUp() throws IOException {
        updates = Collections.synchronizedList(new ArrayList<>());
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/task-status", exchange -> {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            updates.add(TaskStatusUpdate.fromJson(body));
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.start();
        callbackUrl = "http://localhost:" + server.getAddress().getPort();
    }

    @AfterEach
    void tearDown() {
        server.stop(0);
    }

    @Test
    void lifecycle() {
        JobProcess.run(List.of(
                task("extract", ctx -> {}),
                task("load", ctx -> {})
        ), "job-1", callbackUrl);

        assertEquals(4, updates.size());
        assertUpdate(updates.get(0), "job-1", 0, "extract", TaskStatus.RUNNING);
        assertUpdate(updates.get(1), "job-1", 0, "extract", TaskStatus.COMPLETED);
        assertUpdate(updates.get(2), "job-1", 1, "load", TaskStatus.RUNNING);
        assertUpdate(updates.get(3), "job-1", 1, "load", TaskStatus.COMPLETED);

        // RUNNING has zero duration; COMPLETED has non-negative duration
        assertEquals(0, updates.get(0).durationMs());
        assertTrue(updates.get(1).durationMs() >= 0);
    }

    @Test
    void failure() {
        JobProcess.run(List.of(
                task("transform", ctx -> { throw new RuntimeException("bad data"); })
        ), "job-2", callbackUrl);

        assertEquals(2, updates.size());
        assertUpdate(updates.get(0), "job-2", 0, "transform", TaskStatus.RUNNING);
        assertUpdate(updates.get(1), "job-2", 0, "transform", TaskStatus.FAILED);
        assertEquals("bad data", updates.get(1).errorMessage());
        assertTrue(updates.get(1).durationMs() >= 0);
    }

    @Test
    void stopOnFailure() {
        JobProcess.run(List.of(
                task("extract", ctx -> {}),
                task("transform", ctx -> { throw new RuntimeException("bad"); }),
                task("load", ctx -> {})
        ), "job-3", callbackUrl);

        // extract: RUNNING, COMPLETED. transform: RUNNING, FAILED. load: never runs.
        assertEquals(4, updates.size());
        assertUpdate(updates.get(0), "job-3", 0, "extract", TaskStatus.RUNNING);
        assertUpdate(updates.get(1), "job-3", 0, "extract", TaskStatus.COMPLETED);
        assertUpdate(updates.get(2), "job-3", 1, "transform", TaskStatus.RUNNING);
        assertUpdate(updates.get(3), "job-3", 1, "transform", TaskStatus.FAILED);
    }

    @Test
    void progressAndMetrics() {
        JobProcess.run(List.of(
                task("train", ctx -> {
                    ctx.progress(0.5, "halfway");
                    ctx.metric("rows", 1000);
                    ctx.progress(1.0, "done");
                })
        ), "job-4", callbackUrl);

        // Progress and metrics are logged, not sent over HTTP yet
        assertEquals(2, updates.size());
        assertUpdate(updates.get(0), "job-4", 0, "train", TaskStatus.RUNNING);
        assertUpdate(updates.get(1), "job-4", 0, "train", TaskStatus.COMPLETED);
    }

    @Test
    void stdoutCaptured() {
        JobProcess.run(List.of(
                task("emit", ctx -> System.out.println("hello from task"))
        ), "job-5", callbackUrl);

        assertEquals(2, updates.size());
        TaskStatusUpdate completed = updates.get(1);
        assertEquals(TaskStatus.COMPLETED, completed.status());
        assertNotNull(completed.output());
        assertTrue(completed.output().contains("hello from task"));
    }

    @Test
    void outputIncludedOnFailure() {
        JobProcess.run(List.of(
                task("crashing", ctx -> {
                    System.out.println("some progress output");
                    throw new RuntimeException("boom");
                })
        ), "job-6", callbackUrl);

        assertEquals(2, updates.size());
        TaskStatusUpdate failed = updates.get(1);
        assertEquals(TaskStatus.FAILED, failed.status());
        assertNotNull(failed.output());
        assertTrue(failed.output().contains("some progress output"));
        assertEquals("boom", failed.errorMessage());
    }

    @Test
    void missingEnvVars() {
        assertThrows(IllegalStateException.class, () ->
                JobProcess.run(List.of(task("t", ctx -> {}))));
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static void assertUpdate(TaskStatusUpdate update, String jobId, int taskIndex,
                                     String taskName, TaskStatus status) {
        assertEquals(jobId, update.jobId());
        assertEquals(taskIndex, update.taskIndex());
        assertEquals(taskName, update.taskName());
        assertEquals(status, update.status());
    }

    private static Task task(String name, TaskAction action) {
        return new Task() {
            @Override
            public String name() { return name; }

            @Override
            public void execute(TaskContext ctx) throws Exception { action.run(ctx); }
        };
    }

    @FunctionalInterface
    private interface TaskAction {
        void run(TaskContext ctx) throws Exception;
    }
}
