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

/**
 * <b>Worker → Coordinator leg.</b> The open ReportStatus gRPC stream for one job:
 * pushes the {@link StatusUpdate} proto straight to the coordinator — no
 * conversion, it's the same proto the SDK sent (see CLAUDE.md "One status
 * message"). Opened per job by {@link CoordinatorClient#openStatusStream};
 * written to by {@link WorkerAgent} — its per-job status handler (task updates
 * stamped with job RUNNING) and its terminal/TIMEOUT updates.
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

    CoordinatorStatusStream(StreamObserver<StatusUpdate> requestObserver, CountDownLatch done) {
        this.requestObserver = requestObserver;
        this.done = done;
    }

    public synchronized void report(StatusUpdate update) {
        if (update.getJobState() != JobState.JOB_STATE_UNSPECIFIED) {
            log.info("Reporting job status to coordinator: jobId={}, jobState={}{}",
                    update.getJobId(), update.getJobState(),
                    update.getFailureReason() != FailureReason.FAILURE_REASON_UNSPECIFIED
                            ? ", reason=" + FailureMessages.format(update.getFailureReason(), update.getFailureDetail())
                            : "");
        }
        if (update.getTaskState() != TaskState.TASK_STATE_UNSPECIFIED) {
            log.info("Forwarding task status to coordinator: jobId={}, taskIndex={}, taskName={}, taskState={}",
                    update.getJobId(), update.getTaskIndex(), update.getTaskName(), update.getTaskState());
        }
        requestObserver.onNext(update);
    }

    public void complete() {
        requestObserver.onCompleted();
    }

    public boolean awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {
        return done.await(timeout, unit);
    }
}
