package com.scheduler.coordinator.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scheduler.coordinator.JobManager;
import com.scheduler.coordinator.worker.WorkerHandler;
import com.scheduler.core.JobStatus;
import com.scheduler.core.TaskStatus;
import com.scheduler.core.WorkerInfo;
import com.scheduler.core.exception.JobNotFoundException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * The coordinator's single HTTP surface for the monitoring UI. It serves the
 * UI's static files (at {@code /}) and a pull-only JSON read API (at
 * {@code /api/*}). One origin for files + data: no CORS, no separate web server
 * to keep in step. The UI polls; live follow stays on the gRPC client path.
 *
 * <p>Runs in-process on its own port. It reads the same {@link JobManager} and
 * {@link WorkerHandler} the gRPC handlers use, so the two surfaces can't drift.
 * JSON is hand-mapped from the domain records — decoupled from the wire proto,
 * so a proto change doesn't reshape the UI contract.
 *
 * <p>The API lives under {@code /api}, so it never collides with UI asset paths.
 * Everything else falls through to the static handler: files from {@code uiDir},
 * {@code index.html} for unknown routes (SPA fallback). When {@code uiDir} is
 * null or missing, the server is API-only and logs why.
 */
public class ReadApiServer {

    private static final Logger log = LoggerFactory.getLogger(ReadApiServer.class);

    private final JobManager jobManager;
    private final WorkerHandler workerHandler;
    // Directory of built UI static files (Vite dist/). Null → API-only.
    private final Path uiDir;
    private final ObjectMapper json = new ObjectMapper();
    private HttpServer server;

    public ReadApiServer(JobManager jobManager, WorkerHandler workerHandler, Path uiDir) {
        this.jobManager = jobManager;
        this.workerHandler = workerHandler;
        this.uiDir = uiDir;
    }

    public void start(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        // Small pool: read-only polling by a handful of UI clients (see capacity TODO).
        server.setExecutor(Executors.newFixedThreadPool(4));
        server.createContext("/api/jobs", this::handleJobs);
        server.createContext("/api/workers", this::handleWorkers);
        // "/" is the lowest-priority context (longest-prefix match), so it only
        // catches what the /api contexts don't. Registered only with a UI to serve.
        if (uiDir != null && Files.isDirectory(uiDir)) {
            server.createContext("/", this::handleStatic);
            log.info("Serving UI static files from {}", uiDir);
        } else {
            log.warn("UI directory not set or missing ({}); HTTP server is API-only", uiDir);
        }
        server.start();
        log.info("Coordinator HTTP read API started on port {}", server.getAddress().getPort());
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    /** Actual bound port — lets callers (and tests) pass 0 for an ephemeral port. */
    public int port() {
        return server.getAddress().getPort();
    }

    /** Routes /jobs (list), /jobs/{id} (detail), /jobs/{id}/tasks. Only GET is allowed. */
    private void handleJobs(HttpExchange exchange) throws IOException {
        try {
            if (!"GET".equals(exchange.getRequestMethod())) {
                writeError(exchange, 405, "method not allowed: " + exchange.getRequestMethod());
                return;
            }
            String path = exchange.getRequestURI().getPath();
            // "/api/jobs" → list; "/api/jobs/{id}" → detail; "/api/jobs/{id}/tasks" → task list.
            String rest = path.substring("/api/jobs".length());
            if (rest.isEmpty() || rest.equals("/")) {
                writeJson(exchange, 200, listJobs());
                return;
            }
            String[] parts = rest.substring(1).split("/", 2);
            String jobId = parts[0];
            try {
                JobStatus job = jobManager.getJob(jobId);
                if (parts.length == 2 && parts[1].equals("tasks")) {
                    writeJson(exchange, 200, taskList(job));
                } else if (parts.length == 1) {
                    writeJson(exchange, 200, jobDetail(job));
                } else {
                    writeError(exchange, 404, "unknown path: " + path);
                }
            } catch (JobNotFoundException e) {
                log.info("HTTP read API: job not found: jobId={}", jobId);
                writeError(exchange, 404, "job not found: " + jobId);
            }
        } catch (Exception e) {
            // Never let an unmapped error leak a stack trace to the client.
            log.error("HTTP read API: unexpected error handling {}", exchange.getRequestURI(), e);
            writeError(exchange, 500, "internal error");
        }
    }

    /** Routes /workers (list). Only GET is allowed. */
    private void handleWorkers(HttpExchange exchange) throws IOException {
        try {
            if (!"GET".equals(exchange.getRequestMethod())) {
                writeError(exchange, 405, "method not allowed: " + exchange.getRequestMethod());
                return;
            }
            writeJson(exchange, 200, listWorkers());
        } catch (Exception e) {
            log.error("HTTP read API: unexpected error handling {}", exchange.getRequestURI(), e);
            writeError(exchange, 500, "internal error");
        }
    }

    /**
     * Serves the built UI from {@code uiDir}. Unknown paths fall back to
     * {@code index.html} so the client-side app can render any route (SPA
     * fallback). Resolved paths are confined to {@code uiDir} to block
     * {@code ../} traversal out of the served tree.
     */
    private void handleStatic(HttpExchange exchange) throws IOException {
        try {
            if (!"GET".equals(exchange.getRequestMethod())) {
                writeError(exchange, 405, "method not allowed: " + exchange.getRequestMethod());
                return;
            }
            String rawPath = exchange.getRequestURI().getPath();
            String relative = rawPath.equals("/") ? "index.html" : rawPath.substring(1);
            Path file = uiDir.resolve(relative).normalize();
            // Confinement check — a crafted ../ must not escape uiDir.
            if (!file.startsWith(uiDir)) {
                log.warn("Rejected static path outside UI dir: {}", rawPath);
                writeError(exchange, 403, "forbidden");
                return;
            }
            // Missing file → serve index.html so client-side routes resolve.
            if (!Files.isRegularFile(file)) {
                file = uiDir.resolve("index.html");
                if (!Files.isRegularFile(file)) {
                    writeError(exchange, 404, "not found");
                    return;
                }
            }
            byte[] bytes = Files.readAllBytes(file);
            exchange.getResponseHeaders().set("Content-Type", contentType(file));
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        } catch (Exception e) {
            log.error("HTTP read API: unexpected error serving static {}", exchange.getRequestURI(), e);
            writeError(exchange, 500, "internal error");
        }
    }

    private static String contentType(Path file) {
        String name = file.getFileName().toString();
        if (name.endsWith(".html")) return "text/html; charset=utf-8";
        if (name.endsWith(".js")) return "text/javascript";
        if (name.endsWith(".css")) return "text/css";
        if (name.endsWith(".json")) return "application/json";
        if (name.endsWith(".svg")) return "image/svg+xml";
        if (name.endsWith(".ico")) return "image/x-icon";
        if (name.endsWith(".png")) return "image/png";
        if (name.endsWith(".woff2")) return "font/woff2";
        if (name.endsWith(".map")) return "application/json";
        return "application/octet-stream";
    }

    // ── hand-mapped JSON shapes (UI contract, not the wire proto) ───────────

    private List<Map<String, Object>> listJobs() {
        return jobManager.listJobs().stream().map(this::jobSummary).toList();
    }

    private Map<String, Object> jobSummary(JobStatus job) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", job.id());
        m.put("name", job.job().name());
        m.put("state", job.state().name());
        m.put("createdAt", iso(job.createdAt()));
        m.put("startedAt", iso(job.startedAt()));
        m.put("completedAt", iso(job.completedAt()));
        m.put("taskCounts", taskCounts(job));
        return m;
    }

    private Map<String, Object> jobDetail(JobStatus job) {
        Map<String, Object> m = jobSummary(job);
        m.put("failureReason", job.failureReason() == null ? null : job.failureReason().name());
        m.put("failureDetail", job.failureDetail());
        long lastActivity = jobManager.lastActivity(job.id());
        m.put("lastActivityMs", lastActivity == 0 ? null : lastActivity);
        m.put("tasks", taskList(job));
        return m;
    }

    private List<Map<String, Object>> taskList(JobStatus job) {
        return job.taskStatuses().values().stream().map(this::taskJson).toList();
    }

    private List<Map<String, Object>> listWorkers() {
        return workerHandler.listWorkers().stream().map(this::workerJson).toList();
    }

    private Map<String, Object> workerJson(WorkerInfo worker) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", worker.id());
        m.put("hostname", worker.hostname());
        m.put("memoryMb", worker.memoryMb());
        m.put("cpuCores", worker.cpuCores());
        m.put("gpu", worker.gpu());
        m.put("capabilities", List.copyOf(worker.capabilities()));
        m.put("registeredAt", iso(worker.registeredAt()));
        m.put("lastHeartbeat", iso(worker.lastHeartbeat()));
        return m;
    }

    private Map<String, Object> taskJson(TaskStatus task) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("taskIndex", task.taskIndex());
        m.put("taskName", task.taskName());
        m.put("state", task.state().name());
        m.put("startedAt", iso(task.startedAt()));
        m.put("completedAt", iso(task.completedAt()));
        m.put("errorMessage", task.errorMessage());
        m.put("exitCode", task.exitCode());
        return m;
    }

    private Map<String, Integer> taskCounts(JobStatus job) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (TaskStatus task : job.taskStatuses().values()) {
            counts.merge(task.state().name(), 1, Integer::sum);
        }
        return counts;
    }

    private static String iso(Instant instant) {
        return instant == null ? null : instant.toString();
    }

    // ── transport helpers ───────────────────────────────────────────────────

    private void writeJson(HttpExchange exchange, int status, Object body) throws IOException {
        byte[] bytes = json.writeValueAsBytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private void writeError(HttpExchange exchange, int status, String message) throws IOException {
        writeJson(exchange, status, Map.of("error", message));
    }
}
