package com.scheduler.sdk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/**
 * Sends task status updates from the generated {@code _Harness} (in the job process)
 * to WorkerAgent's existing {@code /task-status} HTTP endpoint.
 *
 * <pre>
 * Job process (child JVM)
 *   └─ _Harness
 *        ├─ taskStarted()  ──► JobReporter ──HTTP POST /task-status──► WorkerAgent
 *        ├─ @Task method executes                                       (parent JVM)
 *        └─ taskCompleted() ──► JobReporter ──HTTP POST /task-status──► WorkerAgent
 * </pre>
 *
 * <p>Reuses the same {@link TaskStatusUpdate} JSON format as {@link TaskContext},
 * so WorkerAgent's HTTP handler requires no changes.
 */
public final class JobReporter {

    private static final Logger log = LoggerFactory.getLogger(JobReporter.class);

    private final HttpClient httpClient;
    private final String callbackUrl;
    private final String jobId;

    private String currentTaskName;
    private int currentTaskIndex;
    private long taskStartTimeMs;

    private JobReporter(HttpClient httpClient, String callbackUrl, String jobId) {
        this.httpClient = httpClient;
        this.callbackUrl = callbackUrl;
        this.jobId = jobId;
    }

    /** Creates a reporter connected to the given callback URL. */
    public static JobReporter connect(String callbackUrl, String jobId) {
        return new JobReporter(HttpClient.newHttpClient(), callbackUrl, jobId);
    }

    /** Called by generated harness to report status from user code via {@link JobContext#report}. */
    void report(TaskStatus status, String message) {
        log.info("User report for task {}: status={}, message={}", currentTaskName, status, message);
    }

    public void taskStarted(int taskIndex, String taskName) {
        currentTaskIndex = taskIndex;
        currentTaskName = taskName;
        taskStartTimeMs = System.currentTimeMillis();
        sendStatus(new TaskStatusUpdate(jobId, taskIndex, taskName, TaskStatus.RUNNING, null));
    }

    public void taskCompleted(int taskIndex, String taskName) {
        long durationMs = System.currentTimeMillis() - taskStartTimeMs;
        sendStatus(new TaskStatusUpdate(jobId, taskIndex, taskName, TaskStatus.COMPLETED, null, durationMs, null));
    }

    public void taskFailed(int taskIndex, String taskName, String error) {
        long durationMs = System.currentTimeMillis() - taskStartTimeMs;
        sendStatus(new TaskStatusUpdate(jobId, taskIndex, taskName, TaskStatus.FAILED, error, durationMs, null));
    }

    private void sendStatus(TaskStatusUpdate update) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(callbackUrl + "/task-status"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(update.toJson()))
                .build();

        try {
            HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
            if (response.statusCode() != 200) {
                log.warn("Status report got HTTP {}: task={}, status={}",
                        response.statusCode(), update.taskName(), update.status());
            }
        } catch (Exception e) {
            log.error("Failed to report status for task {}: {}", update.taskName(), e.getMessage());
        }
    }
}
