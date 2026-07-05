package com.scheduler.coordinator.persistence;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scheduler.core.InputFile;
import com.scheduler.core.Job;
import com.scheduler.core.JobStates;
import com.scheduler.core.JobStatus;
import com.scheduler.core.ResourceRequirements;
import com.scheduler.core.TaskStatus;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * SQLite-backed {@link JobStore} — the coordinator's durable mirror. One local
 * file, no external dependency (single coordinator; see README "Coordinator
 * Failover & State Persistence"). Each job is one {@code jobs} row: definition +
 * lifecycle as columns, tasks as a JSON blob. Proto enums are stored by wire
 * number, timestamps as epoch millis. Only this class knows SQL; callers see the
 * {@link JobStore} interface and domain types.
 *
 * <p>Statements are prepared once and reused, so SQLite caches each plan.
 * Methods are synchronized: SQLite is single-writer and coordinator writes are
 * already serialized through {@code JobManager}'s lock, so one shared connection
 * is simplest and safe.
 */
public class SqliteJobStore implements JobStore {

    private static final Logger log = LoggerFactory.getLogger(SqliteJobStore.class);

    // Terminal job-state numbers as a literal IN-list for retention/boot filters.
    // Derived from JobStates, so it can't drift from the lifecycle rules. These
    // are code-derived integers, never user input — safe to inline in SQL.
    private static final String TERMINAL_NUMBERS = Arrays.stream(JobState.values())
            .filter(s -> s != JobState.UNRECOGNIZED && JobStates.isTerminal(s))
            .map(s -> String.valueOf(s.getNumber()))
            .collect(Collectors.joining(","));

    private static final String COLUMNS =
            "job_id, name, artifact_uri, params, priority, input_files, resources, state, " +
            "created_at, started_at, completed_at, failure_reason, failure_detail, " +
            "assigned_worker_id, tasks, updated_at";

    private final ObjectMapper json = new ObjectMapper();
    private final Connection conn;

    // Prepared once, reused under the instance lock (see class doc).
    private final PreparedStatement saveStmt;
    private final PreparedStatement findStmt;
    private final PreparedStatement listAllStmt;
    private final PreparedStatement loadNonTerminalStmt;
    private final PreparedStatement deleteStmt;

    public SqliteJobStore(Path dbPath) {
        try {
            if (dbPath.getParent() != null) {
                Files.createDirectories(dbPath.getParent());
            }
            conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
            initSchema();
            saveStmt = conn.prepareStatement(
                    "INSERT OR REPLACE INTO jobs (" + COLUMNS + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            findStmt = conn.prepareStatement(
                    "SELECT " + COLUMNS + " FROM jobs WHERE job_id = ?");
            listAllStmt = conn.prepareStatement(
                    "SELECT " + COLUMNS + " FROM jobs ORDER BY created_at DESC");
            loadNonTerminalStmt = conn.prepareStatement(
                    "SELECT " + COLUMNS + " FROM jobs WHERE state NOT IN (" + TERMINAL_NUMBERS + ") ORDER BY created_at");
            deleteStmt = conn.prepareStatement(
                    "DELETE FROM jobs WHERE state IN (" + TERMINAL_NUMBERS + ") AND completed_at IS NOT NULL AND completed_at < ?");
            log.info("Opened SQLite job store at {}", dbPath);
        } catch (IOException | SQLException e) {
            throw new IllegalStateException("Failed to open SQLite job store at " + dbPath, e);
        }
    }

    private void initSchema() throws SQLException {
        try (Statement st = conn.createStatement()) {
            // The worker store opens a second connection to this same file; wait out
            // SQLite's single-writer lock instead of failing with SQLITE_BUSY.
            st.execute("PRAGMA busy_timeout = 5000");
            st.executeUpdate("""
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id             TEXT    PRIMARY KEY,
                    name               TEXT    NOT NULL,
                    artifact_uri       TEXT    NOT NULL,
                    params             TEXT    NOT NULL,
                    priority           INTEGER NOT NULL,
                    input_files        TEXT    NOT NULL,
                    resources          TEXT    NOT NULL,
                    state              INTEGER NOT NULL,
                    created_at         INTEGER NOT NULL,
                    started_at         INTEGER,
                    completed_at       INTEGER,
                    failure_reason     INTEGER,
                    failure_detail     TEXT,
                    assigned_worker_id TEXT,
                    tasks              TEXT    NOT NULL,
                    updated_at         INTEGER NOT NULL
                )""");
            st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state)");
            st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at)");
            st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_jobs_completed_at ON jobs(completed_at)");
        }
    }

    @Override
    public synchronized void save(JobStatus job, String assignedWorkerId) {
        try {
            Job def = job.job();
            saveStmt.setString(1, job.id());
            saveStmt.setString(2, def.name());
            saveStmt.setString(3, def.artifactUri());
            saveStmt.setString(4, toJson(def.params()));
            saveStmt.setInt(5, def.priority());
            saveStmt.setString(6, toJson(def.inputFiles()));
            saveStmt.setString(7, toJson(def.resources()));
            saveStmt.setInt(8, job.state().getNumber());
            saveStmt.setLong(9, job.createdAt().toEpochMilli());
            setNullableMillis(saveStmt, 10, job.startedAt());
            setNullableMillis(saveStmt, 11, job.completedAt());
            if (job.failureReason() == null) {
                saveStmt.setNull(12, Types.INTEGER);
            } else {
                saveStmt.setInt(12, job.failureReason().getNumber());
            }
            saveStmt.setString(13, job.failureDetail());
            saveStmt.setString(14, assignedWorkerId);
            saveStmt.setString(15, toJson(toTaskRows(job)));
            saveStmt.setLong(16, Instant.now().toEpochMilli());
            saveStmt.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to persist job " + job.id(), e);
        }
    }

    @Override
    public synchronized Optional<PersistedJob> find(String jobId) {
        try {
            findStmt.setString(1, jobId);
            try (ResultSet rs = findStmt.executeQuery()) {
                return rs.next() ? Optional.of(readRow(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to load job " + jobId, e);
        }
    }

    @Override
    public synchronized List<PersistedJob> listAll() {
        return readAll(listAllStmt, "listAll");
    }

    @Override
    public synchronized List<PersistedJob> loadNonTerminal() {
        return readAll(loadNonTerminalStmt, "loadNonTerminal");
    }

    @Override
    public synchronized int deleteTerminalCompletedBefore(long cutoffEpochMillis) {
        try {
            deleteStmt.setLong(1, cutoffEpochMillis);
            int removed = deleteStmt.executeUpdate();
            if (removed > 0) {
                log.info("Retention sweep removed {} terminal job(s) completed before {}", removed, cutoffEpochMillis);
            }
            return removed;
        } catch (SQLException e) {
            throw new IllegalStateException("Retention sweep failed", e);
        }
    }

    @Override
    public synchronized void close() {
        closeQuietly(saveStmt);
        closeQuietly(findStmt);
        closeQuietly(listAllStmt);
        closeQuietly(loadNonTerminalStmt);
        closeQuietly(deleteStmt);
        try {
            conn.close();
        } catch (SQLException e) {
            log.warn("Failed to close SQLite job store: {}", e.getMessage());
        }
    }

    // ── row <-> domain mapping ──────────────────────────────────────────────

    private List<PersistedJob> readAll(PreparedStatement query, String label) {
        try (ResultSet rs = query.executeQuery()) {
            List<PersistedJob> out = new ArrayList<>();
            while (rs.next()) {
                out.add(readRow(rs));
            }
            return out;
        } catch (SQLException e) {
            throw new IllegalStateException(label + " query failed", e);
        }
    }

    private PersistedJob readRow(ResultSet rs) throws SQLException {
        Job def = new Job(
                rs.getString("name"),
                rs.getString("artifact_uri"),
                fromJson(rs.getString("params"), new TypeReference<LinkedHashMap<String, String>>() {}),
                rs.getInt("priority"),
                fromJson(rs.getString("input_files"), new TypeReference<List<InputFile>>() {}),
                fromJson(rs.getString("resources"), ResourceRequirements.class));

        JobStatus status = new JobStatus(
                rs.getString("job_id"),
                def,
                JobState.forNumber(rs.getInt("state")),
                readTasks(rs.getString("tasks")),
                Instant.ofEpochMilli(rs.getLong("created_at")),
                nullableInstant(rs, "started_at"),
                nullableInstant(rs, "completed_at"),
                nullableFailureReason(rs),
                rs.getString("failure_detail"));

        return new PersistedJob(status, rs.getString("assigned_worker_id"));
    }

    // Tasks are stored as JSON; reports (telemetry) are intentionally not persisted.
    private record TaskRow(String id, int taskIndex, String taskName, int state,
                           Long startedAt, Long completedAt, String errorMessage, Integer exitCode) {}

    private List<TaskRow> toTaskRows(JobStatus job) {
        List<TaskRow> rows = new ArrayList<>();
        for (TaskStatus t : job.taskStatuses().values()) {
            rows.add(new TaskRow(t.id(), t.taskIndex(), t.taskName(), t.state().getNumber(),
                    toMillis(t.startedAt()), toMillis(t.completedAt()), t.errorMessage(), t.exitCode()));
        }
        return rows;
    }

    private Map<Integer, TaskStatus> readTasks(String tasksJson) {
        List<TaskRow> rows = fromJson(tasksJson, new TypeReference<List<TaskRow>>() {});
        Map<Integer, TaskStatus> tasks = new LinkedHashMap<>();
        for (TaskRow row : rows) {
            tasks.put(row.taskIndex(), TaskStatus.restore(
                    row.id(), row.taskIndex(), row.taskName(), TaskState.forNumber(row.state()),
                    toInstant(row.startedAt()), toInstant(row.completedAt()), row.errorMessage(), row.exitCode()));
        }
        return tasks;
    }

    private static FailureReason nullableFailureReason(ResultSet rs) throws SQLException {
        int number = rs.getInt("failure_reason");
        return rs.wasNull() ? null : FailureReason.forNumber(number);
    }

    private static void setNullableMillis(PreparedStatement ps, int idx, Instant instant) throws SQLException {
        if (instant == null) {
            ps.setNull(idx, Types.INTEGER);
        } else {
            ps.setLong(idx, instant.toEpochMilli());
        }
    }

    private static Instant nullableInstant(ResultSet rs, String column) throws SQLException {
        long millis = rs.getLong(column);
        return rs.wasNull() ? null : Instant.ofEpochMilli(millis);
    }

    private static Long toMillis(Instant instant) {
        return instant == null ? null : instant.toEpochMilli();
    }

    private static Instant toInstant(Long millis) {
        return millis == null ? null : Instant.ofEpochMilli(millis);
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

    private String toJson(Object value) {
        try {
            return json.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize " + value, e);
        }
    }

    private <T> T fromJson(String value, TypeReference<T> type) {
        try {
            return json.readValue(value, type);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to deserialize: " + value, e);
        }
    }

    private <T> T fromJson(String value, Class<T> type) {
        try {
            return json.readValue(value, type);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to deserialize: " + value, e);
        }
    }
}
