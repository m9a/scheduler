package com.scheduler.worker;

import com.scheduler.sdk.TaskStatusUpdate;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * Local HTTP server on the worker that receives task status updates
 * from job processes (child JVMs).
 *
 * <pre>
 * Job JVM (child process)
 *   └─ JobRunner ──HTTP POST──► StatusCallbackServer ──► Consumer (forwards to coordinator)
 *                  /task-status
 * </pre>
 *
 * Each job process uses the job-sdk's {@link com.scheduler.sdk.JobRunner} which POSTs
 * {@link TaskStatusUpdate} JSON to {@code /task-status} on this server.
 */
public class StatusCallbackServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StatusCallbackServer.class);

    private final HttpServer server;
    private final Consumer<TaskStatusUpdate> updateHandler;

    public StatusCallbackServer(int port, Consumer<TaskStatusUpdate> updateHandler) throws IOException {
        this.updateHandler = updateHandler;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.createContext("/task-status", this::handleTaskStatus);
    }

    public void start() {
        server.start();
        log.info("Status callback server listening on port {}", getPort());
    }

    public int getPort() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
        log.info("Status callback server stopped");
    }

    private void handleTaskStatus(com.sun.net.httpserver.HttpExchange exchange) throws IOException {
        try {
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            TaskStatusUpdate update = TaskStatusUpdate.fromJson(body);
            log.info("Received status update from job process: jobId={}, taskIndex={}, taskName={}, status={}",
                    update.jobId(), update.taskIndex(), update.taskName(), update.status());

            updateHandler.accept(update);
            exchange.sendResponseHeaders(200, -1);
        } catch (Exception e) {
            log.error("Failed to handle task status update: {}", e.getMessage(), e);
            exchange.sendResponseHeaders(500, -1);
        } finally {
            exchange.close();
        }
    }
}
