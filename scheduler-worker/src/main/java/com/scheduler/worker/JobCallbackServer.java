package com.scheduler.worker;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

/**
 * <b>Job → Worker leg.</b> WebSocket server receiving everything job containers
 * send back to this worker: task status updates and key-value telemetry, as
 * binary frames over a single persistent connection per job. The senders are
 * the SDKs inside the container ({@code JobReporter} in Java, {@code Reporter}
 * in Python), which get this server's URL via the EXECUTION_PAYLOAD env var.
 *
 * <p>Each frame starts with a one-byte type tag, then the proto payload:
 * <pre>
 * Job container (SDK) ──WebSocket──► JobCallbackServer
 *   [0x01][StatusUpdate]              → StatusHandler (forwards to coordinator, per job)
 *   [0x03][Report]                    → ReportHandler (forwarded to coordinator)
 *   [0x02]                  ◄── ack, sent back after a status frame is handled
 * </pre>
 *
 * <p>Status frames are acked so the SDK can confirm delivery on the single
 * persistent connection: a lost status update corrupts job state, and a
 * half-open socket can swallow a send without erroring. Telemetry is not acked
 * (lossy by design).
 *
 * Owned by {@link WorkerAgent}, which wires the handlers: the status handler is
 * swapped in per job, the report handler is fixed for the worker's lifetime.
 */
class JobCallbackServer extends WebSocketServer {

    private static final Logger log = LoggerFactory.getLogger(JobCallbackServer.class);

    // Frame type tags — must match the SDKs' framing constants
    // (Java StatusUpdate.TYPE_TAG_STATUS / ReportSender.TYPE_TAG_REPORT, Python reporter.py).
    static final byte TYPE_TAG_STATUS = 0x01;
    // Sent back to the SDK to confirm a status frame was received and forwarded.
    static final byte TYPE_TAG_ACK = 0x02;
    static final byte TYPE_TAG_REPORT = 0x03;

    /** Receives task status updates parsed from WebSocket binary frames. */
    @FunctionalInterface
    public interface StatusHandler {
        void handle(com.scheduler.proto.job.StatusUpdate update);
    }

    /** Receives key-value telemetry (Report) parsed from WebSocket binary frames. */
    @FunctionalInterface
    public interface ReportHandler {
        void handle(com.scheduler.proto.job.Report report);
    }

    private volatile StatusHandler statusHandler;
    private volatile ReportHandler reportHandler;
    private final CountDownLatch ready = new CountDownLatch(1);

    JobCallbackServer(InetSocketAddress address) {
        super(address);
        setReuseAddr(true);
    }

    /** Blocks until the server thread has bound the socket and is accepting connections. */
    void awaitReady() throws InterruptedException {
        ready.await();
    }

    void setStatusHandler(StatusHandler handler) {
        this.statusHandler = handler;
    }

    void setReportHandler(ReportHandler handler) {
        this.reportHandler = handler;
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        log.info("WebSocket connection opened: remote={}", conn.getRemoteSocketAddress());
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        log.warn("Received unexpected text WebSocket message from {} — expected binary proto",
                conn.getRemoteSocketAddress());
    }

    @Override
    public void onMessage(WebSocket conn, ByteBuffer buffer) {
        try {
            if (buffer.remaining() < 2) {
                log.warn("Received too-short binary message ({} bytes) from {}",
                        buffer.remaining(), conn.getRemoteSocketAddress());
                return;
            }

            byte typeTag = buffer.get();
            byte[] payload = new byte[buffer.remaining()];
            buffer.get(payload);

            if (typeTag == TYPE_TAG_STATUS) {
                com.scheduler.proto.job.StatusUpdate update =
                        com.scheduler.proto.job.StatusUpdate.parseFrom(payload);
                log.info("Received status from JobProcess: jobId={}, taskIndex={}, taskName={}, taskState={}",
                        update.getJobId(), update.getTaskIndex(), update.getTaskName(), update.getTaskState());
                StatusHandler handler = statusHandler;
                if (handler != null) {
                    handler.handle(update);
                    // Ack only after the update is handled (forwarded) — the SDK
                    // resends if it doesn't see this, so we must not ack a drop.
                    conn.send(new byte[]{TYPE_TAG_ACK});
                } else {
                    log.warn("No status handler registered, dropping update: jobId={}", update.getJobId());
                }
            } else if (typeTag == TYPE_TAG_REPORT) {
                com.scheduler.proto.job.Report report =
                        com.scheduler.proto.job.Report.parseFrom(payload);
                log.debug("Received telemetry from JobProcess: jobId={}, taskIndex={}, entries={}",
                        report.getJobId(), report.getTaskIndex(), report.getEntriesCount());
                ReportHandler handler = reportHandler;
                if (handler != null) {
                    handler.handle(report);
                } else {
                    log.warn("No report handler registered, dropping telemetry: jobId={}", report.getJobId());
                }
            } else {
                log.warn("Unknown type tag 0x{} from {}",
                        String.format("%02x", typeTag), conn.getRemoteSocketAddress());
            }
        } catch (Exception e) {
            log.error("Failed to handle WebSocket message: {}", e.getMessage(), e);
        }
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        log.info("WebSocket connection closed: remote={}, code={}, reason={}",
                conn.getRemoteSocketAddress(), code, reason);
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        log.error("WebSocket error: remote={}, error={}",
                conn != null ? conn.getRemoteSocketAddress() : "null", ex.getMessage(), ex);
    }

    @Override
    public void onStart() {
        log.info("WebSocket server started on port {}", getPort());
        ready.countDown();
    }
}
