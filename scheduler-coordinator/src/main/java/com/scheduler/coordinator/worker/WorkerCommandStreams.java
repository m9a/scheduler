package com.scheduler.coordinator.worker;

import com.scheduler.proto.worker.JobCommand;
import com.scheduler.proto.worker.SystemCommand;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * The coordinator's hold on the open <b>coordinator → worker</b> command streams —
 * one system stream and one job stream per worker. {@link WorkerHandler} registers
 * a worker's stream when it subscribes and removes it when the stream closes; the
 * coordinator then sends a command by worker id. A send to a worker with no open
 * stream is a logged no-op (it will resync on reconnect).
 */
class WorkerCommandStreams {

    private static final Logger log = LoggerFactory.getLogger(WorkerCommandStreams.class);

    private final ConcurrentHashMap<String, WorkerStream<SystemCommand>> systemStreams = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, WorkerStream<JobCommand>> jobStreams = new ConcurrentHashMap<>();

    void addSysStream(String workerId, StreamObserver<SystemCommand> stream) {
        systemStreams.put(workerId, new WorkerStream<>(stream));
    }

    void addJobStream(String workerId, StreamObserver<JobCommand> stream) {
        jobStreams.put(workerId, new WorkerStream<>(stream));
    }

    void removeSysStream(String workerId) {
        systemStreams.remove(workerId);
    }

    void removeJobStream(String workerId) {
        jobStreams.remove(workerId);
    }

    /** Sends a system command; false if the worker has no open system stream. */
    boolean sendSysCmd(String workerId, SystemCommand command) {
        return send(systemStreams, workerId, command, "system");
    }

    /** Sends a job command; false if the worker has no open job stream. */
    boolean sendJobCmd(String workerId, JobCommand command) {
        return send(jobStreams, workerId, command, "job");
    }

    private <T> boolean send(ConcurrentHashMap<String, WorkerStream<T>> streams, String workerId, T command, String kind) {
        WorkerStream<T> stream = streams.get(workerId);
        if (stream == null) {
            log.warn("No {} command stream for workerId={}; dropping {}", kind, workerId, command.getClass().getSimpleName());
            return false;
        }
        try {
            stream.send(command);
            return true;
        } catch (Exception e) {
            // A broken stream throws on send — drop it; the worker re-subscribes on reconnect.
            log.warn("Failed to send {} command to workerId={}: {}; dropping stream", kind, workerId, e.getMessage());
            streams.remove(workerId);
            return false;
        }
    }

    /**
     * One worker's outbound command stream. Sending means calling {@code onNext},
     * which writes the message as frames on a single gRPC stream. {@code StreamObserver}
     * is not thread-safe, and sends can originate on different threads (a client's
     * cancel handler, the heartbeat monitor, boot resync), so {@link #send}
     * serializes them — concurrent {@code onNext} would interleave frames and corrupt
     * the message.
     */
    private static final class WorkerStream<T> {
        private final StreamObserver<T> stream;

        WorkerStream(StreamObserver<T> stream) {
            this.stream = stream;
        }

        synchronized void send(T command) {
            stream.onNext(command);
        }
    }
}
