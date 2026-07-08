package com.scheduler.worker.persistence;

import com.scheduler.proto.job.StatusUpdate;
import com.scheduler.proto.v1.FailureReason;
import com.scheduler.proto.v1.JobState;
import com.scheduler.proto.v1.TaskState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SqliteWorkerStatusStoreTest {

    private SqliteWorkerStatusStore open(Path dir) {
        return new SqliteWorkerStatusStore(dir.resolve("worker_status.db"));
    }

    private static StatusUpdate jobRunning(String jobId) {
        return StatusUpdate.newBuilder().setJobId(jobId).setJobState(JobState.JOB_STATE_RUNNING).build();
    }

    private static StatusUpdate taskUpdate(String jobId, int idx, TaskState state) {
        return StatusUpdate.newBuilder()
                .setJobId(jobId).setJobState(JobState.JOB_STATE_RUNNING)
                .setTaskIndex(idx).setTaskName("task-" + idx).setTaskState(state).build();
    }

    private static StatusUpdate jobFailed(String jobId, String detail) {
        return StatusUpdate.newBuilder()
                .setJobId(jobId).setJobState(JobState.JOB_STATE_FAILED)
                .setFailureReason(FailureReason.FAILURE_REASON_PROCESS_EXITED).setFailureDetail(detail).build();
    }

    @Test
    void latestWinsPerRow(@TempDir Path dir) {
        try (SqliteWorkerStatusStore store = open(dir)) {
            store.update(taskUpdate("j1", 0, TaskState.TASK_STATE_RUNNING));
            store.update(taskUpdate("j1", 0, TaskState.TASK_STATE_COMPLETED));  // overwrites same row

            List<StatusUpdate> all = store.loadAllJobs();
            assertEquals(1, all.size());
            assertEquals(TaskState.TASK_STATE_COMPLETED, all.get(0).getTaskState());
        }
    }

    @Test
    void taskRowsBeforeJobEntry(@TempDir Path dir) {
        try (SqliteWorkerStatusStore store = open(dir)) {
            store.update(jobRunning("j1"));                                 // job entry (task_idx -1)
            store.update(taskUpdate("j1", 0, TaskState.TASK_STATE_COMPLETED));
            store.update(taskUpdate("j1", 1, TaskState.TASK_STATE_RUNNING));

            List<StatusUpdate> all = store.loadAllJobs();
            assertEquals(3, all.size());
            // Task entries first (0, 1), job entry last.
            assertEquals(0, all.get(0).getTaskIndex());
            assertEquals(1, all.get(1).getTaskIndex());
            assertEquals(TaskState.TASK_STATE_UNSPECIFIED, all.get(2).getTaskState());
            assertEquals(JobState.JOB_STATE_RUNNING, all.get(2).getJobState());
        }
    }

    // A terminal update's failure reason and detail survive the round trip to disk.
    @Test
    void terminalRoundTrip(@TempDir Path dir) {
        try (SqliteWorkerStatusStore store = open(dir)) {
            store.update(jobFailed("j1", "exit code 7"));

            StatusUpdate loaded = store.loadAllJobs().get(0);
            assertEquals(JobState.JOB_STATE_FAILED, loaded.getJobState());
            assertEquals(FailureReason.FAILURE_REASON_PROCESS_EXITED, loaded.getFailureReason());
            assertEquals("exit code 7", loaded.getFailureDetail());
        }
    }

    // Ack drops every row of the acked job and leaves other jobs alone.
    @Test
    void ackDropsJob(@TempDir Path dir) {
        try (SqliteWorkerStatusStore store = open(dir)) {
            store.update(taskUpdate("j1", 0, TaskState.TASK_STATE_COMPLETED));
            store.update(jobFailed("j1", "boom"));
            store.update(jobRunning("j2"));

            store.ack("j1");

            List<StatusUpdate> all = store.loadAllJobs();
            assertEquals(1, all.size());
            assertEquals("j2", all.get(0).getJobId());
        }
    }

    // Prune removes stale terminal jobs only; in-flight jobs are never pruned.
    @Test
    void prune(@TempDir Path dir) {
        try (SqliteWorkerStatusStore store = open(dir)) {
            store.update(jobFailed("old", "boom"));   // terminal, completed_at ~ now
            store.update(jobRunning("live"));         // non-terminal, no completed_at

            // Retention of -1ms means "older than the future" → the terminal job is stale, the live one isn't.
            int removed = store.prune(Duration.ofMillis(-1));
            assertEquals(1, removed);

            List<StatusUpdate> all = store.loadAllJobs();
            assertEquals(1, all.size());
            assertEquals("live", all.get(0).getJobId());
        }
    }
}
