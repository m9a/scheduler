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

class JobRunnerTest {

    private HttpServer server;
    private String callbackUrl;
    private final List<TaskStatusUpdate> receivedUpdates = Collections.synchronizedList(new ArrayList<>());

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/task-status", exchange -> {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            receivedUpdates.add(TaskStatusUpdate.fromJson(body));
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
    void runAllTasks() {
        List<String> executionOrder = new ArrayList<>();

        JobRunner.run(List.of(
                new SimpleTask("extract", () -> executionOrder.add("extract")),
                new SimpleTask("transform", () -> executionOrder.add("transform")),
                new SimpleTask("load", () -> executionOrder.add("load"))
        ), "job-1", callbackUrl);

        assertEquals(List.of("extract", "transform", "load"), executionOrder);
        assertEquals(6, receivedUpdates.size());
        assertUpdate(receivedUpdates.get(0), "job-1", 0, "extract", TaskStatus.RUNNING);
        assertUpdate(receivedUpdates.get(1), "job-1", 0, "extract", TaskStatus.COMPLETED);
        assertUpdate(receivedUpdates.get(2), "job-1", 1, "transform", TaskStatus.RUNNING);
        assertUpdate(receivedUpdates.get(3), "job-1", 1, "transform", TaskStatus.COMPLETED);
        assertUpdate(receivedUpdates.get(4), "job-1", 2, "load", TaskStatus.RUNNING);
        assertUpdate(receivedUpdates.get(5), "job-1", 2, "load", TaskStatus.COMPLETED);
    }

    @Test
    void stopOnFailure() {
        List<String> executionOrder = new ArrayList<>();

        JobRunner.run(List.of(
                new SimpleTask("extract", () -> executionOrder.add("extract")),
                new SimpleTask("transform", () -> { throw new RuntimeException("bad data"); }),
                new SimpleTask("load", () -> executionOrder.add("load"))
        ), "job-2", callbackUrl);

        assertEquals(List.of("extract"), executionOrder);
        assertEquals(4, receivedUpdates.size());
        assertUpdate(receivedUpdates.get(2), "job-2", 1, "transform", TaskStatus.RUNNING);
        assertUpdate(receivedUpdates.get(3), "job-2", 1, "transform", TaskStatus.FAILED);
        assertEquals("bad data", receivedUpdates.get(3).errorMessage());
    }

    @Test
    void missingCallbackUrl() {
        System.clearProperty("scheduler.callback.url");
        System.clearProperty("scheduler.job.id");

        assertThrows(IllegalStateException.class, () ->
                JobRunner.run(List.of(new SimpleTask("t", () -> {}))));
    }

    private static void assertUpdate(TaskStatusUpdate update, String jobId, int taskIndex,
                                     String taskName, TaskStatus status) {
        assertEquals(jobId, update.jobId());
        assertEquals(taskIndex, update.taskIndex());
        assertEquals(taskName, update.taskName());
        assertEquals(status, update.status());
    }

    /**
     * A simple Task implementation for testing — demonstrates what job authors write.
     */
    private record SimpleTask(String taskName, Runnable action) implements Task {

        @Override
        public String name() {
            return taskName;
        }

        @Override
        public void execute() {
            action.run();
        }
    }
}
