package com.scheduler.coordinator.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scheduler.coordinator.JobManager;
import com.scheduler.coordinator.worker.WorkerHandler;
import com.scheduler.core.Job;
import com.scheduler.proto.worker.RegisterWorkerRequest;
import com.scheduler.proto.worker.RegisterWorkerResponse;
import com.scheduler.proto.v1.ResourceRequirements;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ReadApiServerTest {

    private JobManager jobManager;
    private WorkerHandler workerHandler;
    private ReadApiServer server;
    private HttpClient http;
    private ObjectMapper json;

    @BeforeEach
    void setUp() throws Exception {
        jobManager = new JobManager(new com.scheduler.coordinator.persistence.InMemoryJobStore());
        workerHandler = new WorkerHandler(jobManager, new com.scheduler.coordinator.persistence.InMemoryWorkerStore());
        server = new ReadApiServer(jobManager, workerHandler, null); // API-only
        server.start(0); // ephemeral port
        http = HttpClient.newHttpClient();
        json = new ObjectMapper();
    }

    @AfterEach
    void tearDown() {
        server.stop();
    }

    private HttpResponse<String> rawGet(String path) throws Exception {
        return http.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private JsonNode get(String path) throws Exception {
        HttpResponse<String> resp = rawGet(path);
        assertEquals(200, resp.statusCode(), resp.body());
        return json.readTree(resp.body());
    }

    /** Registers a worker through the gRPC entry point with a no-op response observer. */
    private void registerWorker(String hostname, boolean gpu) {
        workerHandler.registerWorker(
                RegisterWorkerRequest.newBuilder()
                        .setHostname(hostname)
                        .setResources(ResourceRequirements.newBuilder()
                                .setMemoryMb(2048).setCpuCores(4).setGpu(gpu).build())
                        .build(),
                new StreamObserver<>() {
                    public void onNext(RegisterWorkerResponse value) { }
                    public void onError(Throwable t) { }
                    public void onCompleted() { }
                });
    }

    @Test
    void listJobs() throws Exception {
        jobManager.submit("job-1", new Job("first", "img:v1", null, 0, null, null));
        jobManager.submit("job-2", new Job("second", "img:v1", null, 0, null, null));

        JsonNode body = get("/api/jobs");
        assertEquals(2, body.size());
        // Newest first — job-2 submitted last.
        assertEquals("job-2", body.get(0).get("id").asText());
        assertEquals("JOB_STATE_QUEUED", body.get(0).get("state").asText());
    }

    @Test
    void getJob() throws Exception {
        jobManager.submit("job-1", new Job("only", "img:v1", null, 0, null, null));

        JsonNode body = get("/api/jobs/job-1");
        assertEquals("job-1", body.get("id").asText());
        assertEquals("only", body.get("name").asText());
        assertTrue(body.has("tasks"));
        assertTrue(body.get("failureReason").isNull());
    }

    @Test
    void getJobTasks() throws Exception {
        jobManager.submit("job-1", new Job("only", "img:v1", null, 0, null, null));

        JsonNode body = get("/api/jobs/job-1/tasks");
        assertTrue(body.isArray());
        // No tasks reported yet for a freshly queued job.
        assertEquals(0, body.size());
    }

    @Test
    void getJobNotFound() throws Exception {
        assertEquals(404, rawGet("/api/jobs/missing").statusCode());
    }

    @Test
    void getTasksJobNotFound() throws Exception {
        assertEquals(404, rawGet("/api/jobs/missing/tasks").statusCode());
    }

    @Test
    void listWorkers() throws Exception {
        registerWorker("worker-a", true);
        registerWorker("worker-b", false);

        JsonNode body = get("/api/workers");
        assertEquals(2, body.size());
        assertTrue(body.get(0).has("hostname"));
        assertTrue(body.get(0).has("lastHeartbeat"));
        assertTrue(body.get(0).has("capabilities"));
    }

    @Test
    void rejectsNonGet() throws Exception {
        HttpResponse<String> resp = http.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + "/api/jobs"))
                        .POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(405, resp.statusCode());
    }

    // ── static UI serving (Option B: coordinator serves the UI) ─────────────

    // Serves index.html at / and static assets with their content types.
    @Test
    void serveUi(@TempDir Path uiDir) throws Exception {
        Files.writeString(uiDir.resolve("index.html"), "<!doctype html><title>Scheduler</title>");
        Files.createDirectories(uiDir.resolve("assets"));
        Files.writeString(uiDir.resolve("assets/app.js"), "console.log('hi')");

        ReadApiServer uiServer = new ReadApiServer(jobManager, workerHandler, uiDir);
        uiServer.start(0);
        try {
            HttpResponse<String> index = http.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + uiServer.port() + "/")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, index.statusCode());
            assertTrue(index.body().contains("Scheduler"));
            assertEquals("text/html; charset=utf-8", index.headers().firstValue("Content-Type").orElse(""));

            HttpResponse<String> asset = http.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + uiServer.port() + "/assets/app.js")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, asset.statusCode());
            assertEquals("text/javascript", asset.headers().firstValue("Content-Type").orElse(""));
        } finally {
            uiServer.stop();
        }
    }

    // A client-side route with no file behind it falls back to index.html (SPA).
    @Test
    void unknownRoute(@TempDir Path uiDir) throws Exception {
        Files.writeString(uiDir.resolve("index.html"), "<!doctype html><title>SPA</title>");

        ReadApiServer uiServer = new ReadApiServer(jobManager, workerHandler, uiDir);
        uiServer.start(0);
        try {
            // A client-side route the server has no file for → index.html (SPA fallback).
            HttpResponse<String> resp = http.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + uiServer.port() + "/some/client/route")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, resp.statusCode());
            assertTrue(resp.body().contains("SPA"));
        } finally {
            uiServer.stop();
        }
    }

    @Test
    void rejectsPathTraversal(@TempDir Path uiDir) throws Exception {
        Files.writeString(uiDir.resolve("index.html"), "ok");

        ReadApiServer uiServer = new ReadApiServer(jobManager, workerHandler, uiDir);
        uiServer.start(0);
        try {
            // Encoded ../ must not escape uiDir. (URI normalization is bypassed with %2e%2e.)
            HttpResponse<String> resp = http.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + uiServer.port() + "/%2e%2e/%2e%2e/etc/hosts")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            // Either confined-and-missing (SPA fallback 200 to index) or 403 — never the real /etc/hosts.
            assertTrue(resp.statusCode() == 200 || resp.statusCode() == 403);
            assertFalse(resp.body().contains("root:"));
        } finally {
            uiServer.stop();
        }
    }
}
