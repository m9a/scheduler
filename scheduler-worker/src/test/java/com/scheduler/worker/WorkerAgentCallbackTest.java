package com.scheduler.worker;

import com.scheduler.sdk.JobRunner;
import com.scheduler.sdk.Task;
import com.scheduler.sdk.TaskStatus;
import com.scheduler.sdk.TaskStatusUpdate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WorkerAgentCallbackTest {

    private WorkerAgent agent;
    private List<TaskStatusUpdate> receivedUpdates;

    @BeforeEach
    void setUp() throws IOException {
        receivedUpdates = Collections.synchronizedList(new ArrayList<>());
        // coordinatorHost/port don't matter — we only test the HTTP callback, no gRPC RPCs
        agent = new WorkerAgent("localhost", 1, "localhost", 1);
        agent.onTaskStatus(receivedUpdates::add);
    }

    @AfterEach
    void tearDown() throws Exception {
        agent.close();
    }

    @Test
    void receiveUpdate() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        TaskStatusUpdate update = new TaskStatusUpdate("job-1", 0, "extract", TaskStatus.RUNNING, null);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + agent.taskStatusPort() + "/task-status"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(update.toJson()))
                .build();
        HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());

        assertEquals(200, response.statusCode());
        assertEquals(1, receivedUpdates.size());
        assertEquals("job-1", receivedUpdates.get(0).jobId());
        assertEquals("extract", receivedUpdates.get(0).taskName());
        assertEquals(TaskStatus.RUNNING, receivedUpdates.get(0).status());
    }

    @Test
    void receiveFromJobRunner() {
        String callbackUrl = "http://localhost:" + agent.taskStatusPort();

        JobRunner.run(List.of(
                new SimpleTask("step-1"),
                new SimpleTask("step-2")
        ), "job-42", callbackUrl);

        assertEquals(4, receivedUpdates.size());
        assertEquals(TaskStatus.RUNNING, receivedUpdates.get(0).status());
        assertEquals("step-1", receivedUpdates.get(0).taskName());
        assertEquals(TaskStatus.COMPLETED, receivedUpdates.get(1).status());
        assertEquals(TaskStatus.RUNNING, receivedUpdates.get(2).status());
        assertEquals("step-2", receivedUpdates.get(2).taskName());
        assertEquals(TaskStatus.COMPLETED, receivedUpdates.get(3).status());
        assertTrue(receivedUpdates.stream().allMatch(u -> "job-42".equals(u.jobId())));
    }

    @Test
    void rejectNonPost() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + agent.taskStatusPort() + "/task-status"))
                .GET()
                .build();
        HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());

        assertEquals(405, response.statusCode());
        assertTrue(receivedUpdates.isEmpty());
    }

    private record SimpleTask(String taskName) implements Task {
        @Override
        public String name() {
            return taskName;
        }

        @Override
        public void execute() {
            System.out.println("Executing task!");
        }
    }
}
