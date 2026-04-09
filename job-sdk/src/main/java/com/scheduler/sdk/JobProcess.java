package com.scheduler.sdk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.util.List;

/**
 * Runs inside the job process (the child JVM spawned by
 * {@code com.scheduler.worker.WorkerAgent} on the worker).
 *
 * <p><b>Who calls this:</b> The job author, from their JAR's {@code main()} method.
 * The task list is defined by the job author in code — it is not passed in by the
 * coordinator or worker. The coordinator only knows task <em>names</em> (metadata
 * submitted by the client); the actual {@link Task} implementations live in the
 * job JAR.
 *
 * <pre>
 * // Inside a job JAR — this is what the job author writes:
 * public class MyEtlJob {
 *     public static void main(String[] args) {
 *         JobProcess.run(List.of(
 *             new ExtractTask(),    // implements Task
 *             new TransformTask(),
 *             new LoadTask()
 *         ));
 *     }
 * }
 * </pre>
 *
 * <p><b>How it gets invoked:</b> WorkerAgent (in the worker JVM) spawns this
 * JAR as a child process: {@code java -cp <jarPath> <mainClass>}. Two environment
 * variables are set by WorkerAgent:
 * <ul>
 *   <li>{@code AGENT_CALLBACK_URL} — WorkerAgent's HTTP server URL for receiving task events</li>
 *   <li>{@code JOB_ID} — the job execution ID assigned by the coordinator</li>
 * </ul>
 *
 * <p><b>What it does:</b> Runs tasks sequentially, creates a {@link TaskContext}
 * per task that handles lifecycle status (RUNNING, COMPLETED, FAILED), stdout/stderr
 * capture, and duration tracking — all sent to WorkerAgent via HTTP POST.
 *
 * <pre>
 *  Job process (child JVM)                     Worker JVM
 *  ───────────────────────                     ──────────
 *  main() {
 *    JobProcess.run(tasks)
 *      ├─ ctx.started()
 *      │    └─ POST RUNNING ──HTTP──► WorkerAgent
 *      ├─ task.execute(ctx)
 *      │    ├─ ctx.progress(0.5, "halfway")
 *      │    └─ ctx.metric("rows", 1000)
 *      └─ ctx.completed()
 *           └─ POST COMPLETED + duration + output ──HTTP──► WorkerAgent
 *  }
 * </pre>
 */
public final class JobProcess {

    private static final Logger log = LoggerFactory.getLogger(JobProcess.class);

    public static final String ENV_AGENT_CALLBACK_URL = "AGENT_CALLBACK_URL";
    public static final String ENV_JOB_ID = "JOB_ID";

    private JobProcess() {
    }

    /**
     * Production entry point. Reads {@link #ENV_AGENT_CALLBACK_URL} and
     * {@link #ENV_JOB_ID} from environment variables (set by WorkerAgent)
     * and delegates to {@link #run(List, String, String)}.
     */
    public static void run(List<Task> tasks) {
        String callbackUrl = System.getenv(ENV_AGENT_CALLBACK_URL);
        String jobId = System.getenv(ENV_JOB_ID);

        if (callbackUrl == null || callbackUrl.isEmpty()) {
            throw new IllegalStateException("Environment variable '" + ENV_AGENT_CALLBACK_URL + "' is required");
        }
        if (jobId == null || jobId.isEmpty()) {
            throw new IllegalStateException("Environment variable '" + ENV_JOB_ID + "' is required");
        }

        run(tasks, jobId, callbackUrl);
    }

    /**
     * Runs tasks sequentially. Creates a {@link TaskContext} per task that manages
     * lifecycle status, stdout capture, and duration — all sent via HTTP to WorkerAgent.
     */
    public static void run(List<Task> tasks, String jobId, String callbackUrl) {
        HttpClient httpClient = HttpClient.newHttpClient();

        for (int i = 0; i < tasks.size(); i++) {
            Task task = tasks.get(i);
            TaskContext ctx = new TaskContext(httpClient, callbackUrl, jobId, i, task.name());

            log.info("Starting task {} ({}/{})", task.name(), i + 1, tasks.size());
            ctx.started();

            try {
                task.execute(ctx);
                log.info("Task {} completed", task.name());
                ctx.completed();
            } catch (Exception e) {
                log.error("Task {} failed: {}", task.name(), e.getMessage(), e);
                ctx.failed(e.getMessage());
                return;
            }
        }

        log.info("All {} tasks completed", tasks.size());
    }
}
