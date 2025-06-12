package com.scheduler.sdk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

/**
 * Runs inside the job process (the child JVM spawned by JobExecutor on the worker).
 * Job authors call this from their JAR's {@code main()} to run tasks sequentially.
 * Each task's status is reported back to WorkerAgent via HTTP POST.
 *
 * <pre>
 *  Job process (child JVM)                     Worker JVM
 *  ───────────────────────                     ──────────
 *  main() {
 *    JobRunner.run(tasks)
 *      ├─ task.execute()
 *      └─ POST /task-status ──HTTP──► WorkerAgent (task status server)
 *  }
 * </pre>
 *
 * <p>JobExecutor passes two system properties when spawning the job process:
 * <ul>
 *   <li>{@code scheduler.callback.url} — WorkerAgent's task status HTTP server URL</li>
 *   <li>{@code scheduler.job.id} — the job execution ID</li>
 * </ul>
 *
 * <p>Usage (inside a job JAR):
 * <pre>
 * public class MyJob {
 *     public static void main(String[] args) {
 *         JobRunner.run(List.of(
 *             new ExtractTask(),
 *             new TransformTask(),
 *             new LoadTask()
 *         ));
 *     }
 * }
 * </pre>
 */
public final class JobRunner {

    private static final Logger log = LoggerFactory.getLogger(JobRunner.class);

    private JobRunner() {}

    public static void run(List<Task> tasks) {
        String callbackUrl = System.getProperty("scheduler.callback.url");
        String jobId = System.getProperty("scheduler.job.id");

        if (callbackUrl == null || callbackUrl.isEmpty()) {
            throw new IllegalStateException("System property 'scheduler.callback.url' is required");
        }
        if (jobId == null || jobId.isEmpty()) {
            throw new IllegalStateException("System property 'scheduler.job.id' is required");
        }

        run(tasks, jobId, callbackUrl);
    }

    public static void run(List<Task> tasks, String jobId, String callbackUrl) {
        HttpClient httpClient = HttpClient.newHttpClient();

        for (int i = 0; i < tasks.size(); i++) {
            Task task = tasks.get(i);
            log.info("Starting task {} ({}/{})", task.name(), i + 1, tasks.size());

            sendStatus(httpClient, callbackUrl, new TaskStatusUpdate(jobId, i, task.name(), TaskStatus.RUNNING, null));

            try {
                task.execute();
                log.info("Task {} completed", task.name());
                sendStatus(httpClient, callbackUrl, new TaskStatusUpdate(jobId, i, task.name(), TaskStatus.COMPLETED, null));
            } catch (Exception e) {
                log.error("Task {} failed: {}", task.name(), e.getMessage(), e);
                sendStatus(httpClient, callbackUrl, new TaskStatusUpdate(jobId, i, task.name(), TaskStatus.FAILED, e.getMessage()));
                return;
            }
        }

        log.info("All {} tasks completed", tasks.size());
    }

    private static void sendStatus(HttpClient client, String callbackUrl, TaskStatusUpdate update) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(callbackUrl + "/task-status"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(update.toJson()))
                .build();

        try {
            HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
            if (response.statusCode() != 200) {
                log.warn("Status report got HTTP {}: task={}, status={}",
                        response.statusCode(), update.taskName(), update.status());
            }
        } catch (Exception e) {
            log.error("Failed to report status for task {}: {}", update.taskName(), e.getMessage());
        }
    }
}
