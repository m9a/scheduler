package com.scheduler.coordinator.persistence;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scheduler.core.WorkerInfo;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * SQLite-backed {@link WorkerStore} — the coordinator's durable worker registry,
 * one {@code workers} row per worker. Mirrors {@link SqliteJobStore}'s shape:
 * statements prepared once and reused, capabilities as a JSON blob, timestamps as
 * epoch millis, and this is the only class that knows the worker SQL/schema.
 *
 * <p>It opens its own connection to the same db file as {@link SqliteJobStore}.
 * Worker writes (register, eviction) run off a different thread than the hot job
 * write path and are not under {@code JobManager}'s lock, so a {@code busy_timeout}
 * PRAGMA lets a writer wait out SQLite's single-writer file lock instead of failing
 * with SQLITE_BUSY (the same PRAGMA is set on the job store).
 */
public class SqliteWorkerStore implements WorkerStore {

    private static final Logger log = LoggerFactory.getLogger(SqliteWorkerStore.class);

    private static final String COLUMNS =
            "worker_id, hostname, memory_mb, cpu_cores, gpu, capabilities, registered_at, last_heartbeat";

    private final ObjectMapper json = new ObjectMapper();
    private final Connection conn;

    private final PreparedStatement saveStmt;
    private final PreparedStatement deleteStmt;
    private final PreparedStatement loadAllStmt;

    public SqliteWorkerStore(Path dbPath) {
        try {
            if (dbPath.getParent() != null) {
                Files.createDirectories(dbPath.getParent());
            }
            conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
            initSchema();
            saveStmt = conn.prepareStatement(
                    "INSERT OR REPLACE INTO workers (" + COLUMNS + ") VALUES (?,?,?,?,?,?,?,?)");
            deleteStmt = conn.prepareStatement("DELETE FROM workers WHERE worker_id = ?");
            loadAllStmt = conn.prepareStatement(
                    "SELECT " + COLUMNS + " FROM workers ORDER BY registered_at");
            log.info("Opened SQLite worker store at {}", dbPath);
        } catch (IOException | SQLException e) {
            throw new IllegalStateException("Failed to open SQLite worker store at " + dbPath, e);
        }
    }

    private void initSchema() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("PRAGMA busy_timeout = 5000");
            st.executeUpdate("""
                CREATE TABLE IF NOT EXISTS workers (
                    worker_id      TEXT    PRIMARY KEY,
                    hostname       TEXT    NOT NULL,
                    memory_mb      INTEGER NOT NULL,
                    cpu_cores      INTEGER NOT NULL,
                    gpu            INTEGER NOT NULL,
                    capabilities   TEXT    NOT NULL,
                    registered_at  INTEGER NOT NULL,
                    last_heartbeat INTEGER NOT NULL
                )""");
        }
    }

    @Override
    public synchronized void save(WorkerInfo worker) {
        try {
            saveStmt.setString(1, worker.id());
            saveStmt.setString(2, worker.hostname());
            saveStmt.setInt(3, worker.memoryMb());
            saveStmt.setInt(4, worker.cpuCores());
            saveStmt.setInt(5, worker.gpu() ? 1 : 0);
            saveStmt.setString(6, toJson(worker.capabilities()));
            saveStmt.setLong(7, worker.registeredAt().toEpochMilli());
            saveStmt.setLong(8, worker.lastHeartbeat().toEpochMilli());
            saveStmt.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to persist worker " + worker.id(), e);
        }
    }

    @Override
    public synchronized void delete(String workerId) {
        try {
            deleteStmt.setString(1, workerId);
            deleteStmt.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to delete worker " + workerId, e);
        }
    }

    @Override
    public synchronized List<WorkerInfo> loadAll() {
        try (ResultSet rs = loadAllStmt.executeQuery()) {
            List<WorkerInfo> out = new ArrayList<>();
            while (rs.next()) {
                out.add(readRow(rs));
            }
            return out;
        } catch (SQLException e) {
            throw new IllegalStateException("loadAll query failed", e);
        }
    }

    @Override
    public synchronized void close() {
        closeQuietly(saveStmt);
        closeQuietly(deleteStmt);
        closeQuietly(loadAllStmt);
        try {
            conn.close();
        } catch (SQLException e) {
            log.warn("Failed to close SQLite worker store: {}", e.getMessage());
        }
    }

    private WorkerInfo readRow(ResultSet rs) throws SQLException {
        return new WorkerInfo(
                rs.getString("worker_id"),
                rs.getString("hostname"),
                rs.getInt("memory_mb"),
                rs.getInt("cpu_cores"),
                rs.getInt("gpu") != 0,
                fromJson(rs.getString("capabilities")),
                Instant.ofEpochMilli(rs.getLong("registered_at")),
                Instant.ofEpochMilli(rs.getLong("last_heartbeat")));
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

    private String toJson(Set<String> value) {
        try {
            return json.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize capabilities " + value, e);
        }
    }

    private Set<String> fromJson(String value) {
        try {
            return json.readValue(value, new TypeReference<HashSet<String>>() {});
        } catch (Exception e) {
            throw new IllegalStateException("Failed to deserialize capabilities: " + value, e);
        }
    }
}
