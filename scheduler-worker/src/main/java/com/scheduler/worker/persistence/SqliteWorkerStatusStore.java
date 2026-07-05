package com.scheduler.worker.persistence;

import com.scheduler.core.JobStates;
import com.scheduler.proto.job.StatusUpdate;
import com.scheduler.proto.v1.FailureReason;
import com.scheduler.proto.v1.JobState;
import com.scheduler.proto.v1.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * SQLite-backed {@link WorkerStatusStore} — one local file on the worker, no
 * external dependency. Each row holds one {@link StatusUpdate}'s fields. Proto
 * enums are stored by wire number, timestamps as epoch millis. Only this class
 * knows SQL; callers see the interface and the {@link StatusUpdate} proto.
 *
 * <p>One row per {@code (job_id, task_idx)}. {@code task_idx = -1} is the job
 * entry — it records ownership and job state before any task reports.
 * {@code task_idx >= 0} is a task entry; task columns are NULL on the job entry.
 * Statements are prepared once and reused. Methods are synchronized — one
 * connection, single writer, matching how SQLite works.
 */
public class SqliteWorkerStatusStore implements WorkerStatusStore {

    private static final Logger log = LoggerFactory.getLogger(SqliteWorkerStatusStore.class);

    // Sentinel task_idx for the per-job entry (no task section).
    private static final int JOB_ENTRY = -1;

    private static final String COLUMNS =
            "job_id, task_idx, job_state, task_name, task_state, error_message, " +
            "failure_reason, failure_detail, updated_at, completed_at";

    private final Connection conn;
    private final PreparedStatement upsertStmt;
    private final PreparedStatement loadAllStmt;
    private final PreparedStatement ackStmt;
    private final PreparedStatement pruneStmt;

    public SqliteWorkerStatusStore(Path dbPath) {
        try {
            if (dbPath.getParent() != null) {
                Files.createDirectories(dbPath.getParent());
            }
            conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
            initSchema();
            upsertStmt = conn.prepareStatement(
                    "INSERT OR REPLACE INTO job_status (" + COLUMNS + ") VALUES (?,?,?,?,?,?,?,?,?,?)");
            // Task entries (task_idx >= 0) before the job entry (task_idx = -1) per job, so
            // task state lands before a terminal update trips the coordinator's guard.
            loadAllStmt = conn.prepareStatement(
                    "SELECT " + COLUMNS + " FROM job_status " +
                    "ORDER BY job_id, CASE WHEN task_idx < 0 THEN 1 ELSE 0 END, task_idx");
            ackStmt = conn.prepareStatement("DELETE FROM job_status WHERE job_id = ?");
            pruneStmt = conn.prepareStatement(
                    "DELETE FROM job_status WHERE job_id IN (" +
                    "SELECT job_id FROM job_status WHERE task_idx = " + JOB_ENTRY +
                    " AND completed_at IS NOT NULL AND completed_at < ?)");
            log.info("Opened SQLite worker status store at {}", dbPath);
        } catch (IOException | SQLException e) {
            throw new IllegalStateException("Failed to open SQLite worker status store at " + dbPath, e);
        }
    }

    private void initSchema() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("PRAGMA busy_timeout = 5000");
            st.executeUpdate("""
                CREATE TABLE IF NOT EXISTS job_status (
                    job_id         TEXT    NOT NULL,
                    task_idx       INTEGER NOT NULL,
                    job_state      INTEGER NOT NULL,
                    task_name      TEXT,
                    task_state     INTEGER,
                    error_message  TEXT,
                    failure_reason INTEGER,
                    failure_detail TEXT,
                    updated_at     INTEGER NOT NULL,
                    completed_at   INTEGER,
                    PRIMARY KEY (job_id, task_idx)
                )""");
            st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_job_status_completed_at ON job_status(completed_at)");
        }
    }

    @Override
    public synchronized void update(StatusUpdate u) {
        boolean hasTask = u.getTaskState() != TaskState.TASK_STATE_UNSPECIFIED;
        int taskIdx = hasTask ? u.getTaskIndex() : JOB_ENTRY;
        boolean terminal = !hasTask && JobStates.isTerminal(u.getJobState());
        try {
            upsertStmt.setString(1, u.getJobId());
            upsertStmt.setInt(2, taskIdx);
            upsertStmt.setInt(3, u.getJobState().getNumber());
            setNullableString(upsertStmt, 4, hasTask ? emptyToNull(u.getTaskName()) : null);
            if (hasTask) {
                upsertStmt.setInt(5, u.getTaskState().getNumber());
            } else {
                upsertStmt.setNull(5, Types.INTEGER);
            }
            setNullableString(upsertStmt, 6, hasTask ? emptyToNull(u.getErrorMessage()) : null);
            if (u.getFailureReason() != FailureReason.FAILURE_REASON_UNSPECIFIED) {
                upsertStmt.setInt(7, u.getFailureReason().getNumber());
            } else {
                upsertStmt.setNull(7, Types.INTEGER);
            }
            setNullableString(upsertStmt, 8, emptyToNull(u.getFailureDetail()));
            upsertStmt.setLong(9, Instant.now().toEpochMilli());
            if (terminal) {
                upsertStmt.setLong(10, Instant.now().toEpochMilli());
            } else {
                upsertStmt.setNull(10, Types.INTEGER);
            }
            upsertStmt.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to persist status for job " + u.getJobId(), e);
        }
    }

    @Override
    public synchronized List<StatusUpdate> loadAllJobs() {
        try (ResultSet rs = loadAllStmt.executeQuery()) {
            List<StatusUpdate> out = new ArrayList<>();
            while (rs.next()) {
                out.add(readRow(rs));
            }
            return out;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to load worker status rows", e);
        }
    }

    @Override
    public synchronized void ack(String jobId) {
        try {
            ackStmt.setString(1, jobId);
            ackStmt.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to ack (prune) job " + jobId, e);
        }
    }

    @Override
    public synchronized int prune(Duration retentionPeriod) {
        long cutoff = Instant.now().minus(retentionPeriod).toEpochMilli();
        try {
            pruneStmt.setLong(1, cutoff);
            int removed = pruneStmt.executeUpdate();
            if (removed > 0) {
                log.info("Retention sweep removed {} status row(s) for jobs terminal before {}", removed, cutoff);
            }
            return removed;
        } catch (SQLException e) {
            throw new IllegalStateException("Worker status retention sweep failed", e);
        }
    }

    @Override
    public synchronized void close() {
        closeQuietly(upsertStmt);
        closeQuietly(loadAllStmt);
        closeQuietly(ackStmt);
        closeQuietly(pruneStmt);
        try {
            conn.close();
        } catch (SQLException e) {
            log.warn("Failed to close SQLite worker status store: {}", e.getMessage());
        }
    }

    // ── row -> proto mapping ────────────────────────────────────────────────

    private StatusUpdate readRow(ResultSet rs) throws SQLException {
        StatusUpdate.Builder b = StatusUpdate.newBuilder()
                .setJobId(rs.getString("job_id"))
                .setJobState(JobState.forNumber(rs.getInt("job_state")));

        int taskIdx = rs.getInt("task_idx");
        if (taskIdx >= 0) {
            b.setTaskIndex(taskIdx);
            b.setTaskState(TaskState.forNumber(rs.getInt("task_state")));
            String taskName = rs.getString("task_name");
            if (taskName != null) {
                b.setTaskName(taskName);
            }
            String error = rs.getString("error_message");
            if (error != null) {
                b.setErrorMessage(error);
            }
        }

        int failureReason = rs.getInt("failure_reason");
        if (!rs.wasNull()) {
            b.setFailureReason(FailureReason.forNumber(failureReason));
        }
        String failureDetail = rs.getString("failure_detail");
        if (failureDetail != null) {
            b.setFailureDetail(failureDetail);
        }
        return b.build();
    }

    private static void setNullableString(PreparedStatement ps, int idx, String value) throws SQLException {
        if (value == null) {
            ps.setNull(idx, Types.VARCHAR);
        } else {
            ps.setString(idx, value);
        }
    }

    private static String emptyToNull(String s) {
        return s == null || s.isEmpty() ? null : s;
    }

    private void closeQuietly(PreparedStatement ps) {
        try {
            if (ps != null) {
                ps.close();
            }
        } catch (SQLException e) {
            log.warn("Failed to close prepared statement: {}", e.getMessage());
        }
    }
}
