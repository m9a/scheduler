package com.scheduler.coordinator.persistence;

import com.scheduler.core.InputFile;
import com.scheduler.core.Job;
import com.scheduler.core.JobStatus;
import com.scheduler.core.ResourceRequirements;
import com.scheduler.core.TaskStatus;
import com.scheduler.coordinator.persistence.JobStore.PersistedJob;
import com.scheduler.proto.v1.FailureReason;
import com.scheduler.proto.v1.JobState;
import com.scheduler.proto.v1.TaskState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SqliteJobStoreTest {

    private Job sampleJob(String name) {
        return new Job(name, "registry/img:v1",
                Map.of("region", "emea"), 3,
                List.of(new InputFile("data.csv", "s3://bucket/data.csv")),
                new ResourceRequirements(2048, 4, true, Set.of("cuda")));
    }

    private JobStatus running(String id, Map<Integer, TaskStatus> tasks) {
        return new JobStatus(id, sampleJob("etl"), JobState.JOB_STATE_RUNNING, tasks,
                Instant.ofEpochMilli(500), Instant.ofEpochMilli(900), null, null, null);
    }

    private JobStatus terminal(String id, JobState state, long completedAtMs) {
        return new JobStatus(id, sampleJob(id), state, new LinkedHashMap<>(),
                Instant.ofEpochMilli(100), Instant.ofEpochMilli(200),
                Instant.ofEpochMilli(completedAtMs), null, null);
    }

    @Test
    void roundTrip(@TempDir Path dir) {
        SqliteJobStore store = new SqliteJobStore(dir.resolve("jobs.db"));
        try {
            Map<Integer, TaskStatus> tasks = new LinkedHashMap<>();
            tasks.put(0, TaskStatus.restore("t-0", 0, "extract", TaskState.TASK_STATE_COMPLETED,
                    Instant.ofEpochMilli(1000), Instant.ofEpochMilli(2000), null, 0));
            tasks.put(1, TaskStatus.restore("t-1", 1, "train", TaskState.TASK_STATE_RUNNING,
                    Instant.ofEpochMilli(2000), null, null, null));

            store.save(running("job-1", tasks), "worker-7");

            PersistedJob loaded = store.find("job-1").orElseThrow();
            assertEquals("worker-7", loaded.assignedWorkerId());
            JobStatus s = loaded.status();
            assertEquals("etl", s.job().name());
            assertEquals("emea", s.job().params().get("region"));
            assertEquals(3, s.job().priority());
            assertEquals(2048, s.job().resources().memoryMb());
            assertTrue(s.job().resources().capabilities().contains("cuda"));
            assertEquals("data.csv", s.job().inputFiles().get(0).name());
            assertEquals(JobState.JOB_STATE_RUNNING, s.state());
            assertEquals(Instant.ofEpochMilli(900), s.startedAt());
            assertNull(s.completedAt());
            assertEquals(2, s.taskStatuses().size());
            assertEquals(TaskState.TASK_STATE_COMPLETED, s.taskStatuses().get(0).state());
            assertEquals(0, s.taskStatuses().get(0).exitCode());
            assertEquals(TaskState.TASK_STATE_RUNNING, s.taskStatuses().get(1).state());
            assertNull(s.taskStatuses().get(1).completedAt());
        } finally {
            store.close();
        }
    }

    @Test
    void saveReplaces(@TempDir Path dir) {
        SqliteJobStore store = new SqliteJobStore(dir.resolve("jobs.db"));
        try {
            store.save(running("job-1", new LinkedHashMap<>()), null);
            store.save(terminal("job-1", JobState.JOB_STATE_COMPLETED, 3000), "worker-7");
            assertEquals(1, store.listAll().size());
            assertEquals(JobState.JOB_STATE_COMPLETED, store.find("job-1").orElseThrow().status().state());
        } finally {
            store.close();
        }
    }

    @Test
    void loadNonTerminalExcludesTerminal(@TempDir Path dir) {
        SqliteJobStore store = new SqliteJobStore(dir.resolve("jobs.db"));
        try {
            store.save(running("active", new LinkedHashMap<>()), "w1");
            store.save(terminal("done", JobState.JOB_STATE_COMPLETED, 3000), "w1");
            store.save(terminal("failed", JobState.JOB_STATE_FAILED, 3000), "w1");

            List<PersistedJob> nonTerminal = store.loadNonTerminal();
            assertEquals(1, nonTerminal.size());
            assertEquals("active", nonTerminal.get(0).status().id());
        } finally {
            store.close();
        }
    }

    @Test
    void persistsAcrossReopen(@TempDir Path dir) {
        Path db = dir.resolve("jobs.db");
        SqliteJobStore first = new SqliteJobStore(db);
        first.save(running("job-1", new LinkedHashMap<>()), "w1");
        first.close();

        SqliteJobStore second = new SqliteJobStore(db);
        try {
            assertTrue(second.find("job-1").isPresent());
        } finally {
            second.close();
        }
    }

    @Test
    void retentionDeletesOldTerminal(@TempDir Path dir) {
        SqliteJobStore store = new SqliteJobStore(dir.resolve("jobs.db"));
        try {
            store.save(terminal("old", JobState.JOB_STATE_COMPLETED, 1_000), "w1");
            store.save(terminal("recent", JobState.JOB_STATE_COMPLETED, 9_000), "w1");
            store.save(running("active", new LinkedHashMap<>()), "w1");

            int removed = store.deleteTerminalCompletedBefore(5_000);
            assertEquals(1, removed);
            assertTrue(store.find("old").isEmpty());
            assertTrue(store.find("recent").isPresent());
            assertTrue(store.find("active").isPresent());  // non-terminal never swept
        } finally {
            store.close();
        }
    }

    @Test
    void failureRoundTrip(@TempDir Path dir) {
        SqliteJobStore store = new SqliteJobStore(dir.resolve("jobs.db"));
        try {
            JobStatus failed = new JobStatus("job-1", sampleJob("etl"), JobState.JOB_STATE_FAILED,
                    new LinkedHashMap<>(), Instant.ofEpochMilli(100), Instant.ofEpochMilli(200),
                    Instant.ofEpochMilli(300), FailureReason.FAILURE_REASON_HEARTBEAT_LOST, "worker gone");
            store.save(failed, null);

            JobStatus s = store.find("job-1").orElseThrow().status();
            assertEquals(FailureReason.FAILURE_REASON_HEARTBEAT_LOST, s.failureReason());
            assertEquals("worker gone", s.failureDetail());
        } finally {
            store.close();
        }
    }
}
