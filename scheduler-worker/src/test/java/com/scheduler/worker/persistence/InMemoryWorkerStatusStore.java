package com.scheduler.worker.persistence;

import com.scheduler.core.JobStates;
import com.scheduler.proto.job.StatusUpdate;
import com.scheduler.proto.v1.TaskState;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * In-memory {@link WorkerStatusStore} for unit tests — same latest-wins semantics as
 * {@link SqliteWorkerStatusStore} without touching disk. Keyed by {@code (jobId, taskIdx)};
 * {@code taskIdx = -1} is the job entry.
 */
public class InMemoryWorkerStatusStore implements WorkerStatusStore {

    private static final int JOB_ENTRY = -1;

    private record Key(String jobId, int taskIdx) {}
    private record Row(StatusUpdate update, Long completedAt) {}

    private final Map<Key, Row> rows = new LinkedHashMap<>();

    @Override
    public synchronized void update(StatusUpdate u) {
        boolean hasTask = u.getTaskState() != TaskState.TASK_STATE_UNSPECIFIED;
        int taskIdx = hasTask ? u.getTaskIndex() : JOB_ENTRY;
        boolean terminal = !hasTask && JobStates.isTerminal(u.getJobState());
        rows.put(new Key(u.getJobId(), taskIdx),
                new Row(u, terminal ? Instant.now().toEpochMilli() : null));
    }

    @Override
    public synchronized List<StatusUpdate> loadAllJobs() {
        List<Map.Entry<Key, Row>> ordered = new ArrayList<>(rows.entrySet());
        ordered.sort(Comparator
                .comparing((Map.Entry<Key, Row> e) -> e.getKey().jobId())
                .thenComparing(e -> e.getKey().taskIdx() < 0 ? 1 : 0)
                .thenComparing(e -> e.getKey().taskIdx()));
        List<StatusUpdate> out = new ArrayList<>();
        for (Map.Entry<Key, Row> e : ordered) {
            out.add(e.getValue().update());
        }
        return out;
    }

    @Override
    public synchronized void ack(String jobId) {
        rows.keySet().removeIf(k -> k.jobId().equals(jobId));
    }

    @Override
    public synchronized int prune(Duration retentionPeriod) {
        long cutoff = Instant.now().minus(retentionPeriod).toEpochMilli();
        List<String> stale = rows.entrySet().stream()
                .filter(e -> e.getKey().taskIdx() == JOB_ENTRY
                        && e.getValue().completedAt() != null
                        && e.getValue().completedAt() < cutoff)
                .map(e -> e.getKey().jobId())
                .toList();
        int before = rows.size();
        for (String jobId : stale) {
            rows.keySet().removeIf(k -> k.jobId().equals(jobId));
        }
        return before - rows.size();
    }

    @Override
    public synchronized void close() {
        // Keep the rows: the sqlite store's data survives close too, and restart
        // tests hand the same store to the next agent as the "disk".
    }
}
