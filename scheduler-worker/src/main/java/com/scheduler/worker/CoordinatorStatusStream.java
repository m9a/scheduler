package com.scheduler.worker;

import com.scheduler.core.FailureMessages;
import com.scheduler.proto.job.StatusUpdate;
import com.scheduler.proto.v1.FailureReason;
import com.scheduler.proto.v1.JobState;
import com.scheduler.proto.v1.TaskState;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <b>Worker → Coordinator leg.</b> The open ReportStatus gRPC stream for one job.
 * It pushes the {@link StatusUpdate} proto straight to the coordinator — no
 * conversion, it is the same proto the SDK sent (see CLAUDE.md "One status
 * message"). {@link CoordinatorClient#openStatusStream} opens one per job;
 * {@link WorkerAgent} writes task updates and terminal updates to it.
 *
 * <pre>
 * WorkerAgent ──► CoordinatorStatusStream ──gRPC stream──► Coordinator
 * </pre>
 *
 * <p>{@link #report} is synchronized: the relay (WebSocket thread) and the agent
 * (worker thread) both write here, and a gRPC stream forbids concurrent onNext.
 */
public class CoordinatorStatusStream {

    private static final Logger log = LoggerFactory.getLogger(CoordinatorStatusStream.class);

    private final StreamObserver<StatusUpdate> requestObserver;
    private final CountDownLatch done;
    // Set by the receive side when the coordinator's close-ack response arrives.
    // A stream error before the ack opens the latch without setting it.
    private final AtomicBoolean acked;

    CoordinatorStatusStream(StreamObserver<StatusUpdate> requestObserver, CountDownLatch done,
                            AtomicBoolean acked) {
        this.requestObserver = requestObserver;
        this.done = done;
        this.acked = acked;
    }

    public synchronized void report(StatusUpdate update) {

        // TODO: why do we still have JOB_STATE_UNSPECIFIED here ?
        if (update.getJobState() != JobState.JOB_STATE_UNSPECIFIED) {
            log.info("Reporting job status to coordinator: jobId={}, jobState={}{}",
                    update.getJobId(), update.getJobState(),
                    update.getFailureReason() != FailureReason.FAILURE_REASON_UNSPECIFIED
                            ? ", reason=" + FailureMessages.format(update.getFailureReason(), update.getFailureDetail())
                            : "");
        }

        // TODO: why do we still have TASK_STATE_UNSPECIFIED here ?
        if (update.getTaskState() != TaskState.TASK_STATE_UNSPECIFIED) {
            log.info("Forwarding task status to coordinator: jobId={}, taskIndex={}, taskName={}, taskState={}",
                    update.getJobId(), update.getTaskIndex(), update.getTaskName(), update.getTaskState());
        }
        requestObserver.onNext(update);
    }

    public void complete() {
        requestObserver.onCompleted();
    }

    /**
     * True only when the coordinator's close ack arrived — the caller's signal
     * that every update (incl. the terminal one) was applied and the job's store
     * rows may be dropped. False on timeout <b>or</b> a stream error before the
     * ack: the rows stay for the register flush to re-deliver.
     */
    public boolean awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {
        return done.await(timeout, unit) && acked.get();
    }
}
