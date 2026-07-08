package com.scheduler.worker;

import com.scheduler.proto.job.Report;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * <b>Worker → Coordinator leg.</b> The open ReportTelemetry gRPC stream for one
 * job: forwards each SDK {@link Report}, re-stamped by the worker with its receive
 * time as the job's last-activity marker (see {@link WorkerAgent#relayTelemetry}).
 * Opened per job by {@link CoordinatorClient#openTelemetryStream}; written by the
 * job's report handler (WebSocket thread) and completed by the worker thread —
 * those two threads are why {@link #report} and {@link #complete} are synchronized.
 * The {@link JobLivenessMonitor} does not write here; liveness is worker-local.
 *
 * <pre>
 * WorkerAgent ──► CoordinatorTelemetryStream ──gRPC stream──► Coordinator
 * </pre>
 *
 * <p>{@link #report} is synchronized: two threads write here and a gRPC stream
 * forbids concurrent onNext. Telemetry is lossy by design, so a failed send (a
 * late frame after {@link #complete}, or a broken stream) is logged and dropped,
 * never propagated — it must not fail the job.
 */
class CoordinatorTelemetryStream {

    private static final Logger log = LoggerFactory.getLogger(CoordinatorTelemetryStream.class);

    private final StreamObserver<Report> requestObserver;
    private final CountDownLatch done;

    CoordinatorTelemetryStream(StreamObserver<Report> requestObserver, CountDownLatch done) {
        this.requestObserver = requestObserver;
        this.done = done;
    }

    synchronized void report(Report report) {
        try {
            requestObserver.onNext(report);
        } catch (Exception e) {
            log.debug("Dropping telemetry for job={}, taskIndex={}: {}",
                    report.getJobId(), report.getTaskIndex(), e.getMessage());
        }
    }

    synchronized void complete() {
        try {
            requestObserver.onCompleted();
        } catch (Exception e) {
            log.debug("Telemetry stream already closed: {}", e.getMessage());
        }
    }

    boolean awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {
        return done.await(timeout, unit);
    }
}
