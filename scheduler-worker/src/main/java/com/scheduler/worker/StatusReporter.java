package com.scheduler.worker;

import com.scheduler.core.FailureReason;
import com.scheduler.proto.v1.JobStatus;
import com.scheduler.proto.v1.TaskStatus;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Streams status updates from the worker to the coordinator over gRPC.
 * Handles both job-level updates (STARTING, RUNNING, COMPLETED, FAILED, KILLED)
 * and task-level updates (forwarded from the SDK via WebSocket).
 *
 * <pre>
 * JobProcess ──WebSocket──► WorkerAgent ──► StatusReporter ──gRPC stream──► Coordinator
 * (job process)            (receives update)  (converts to proto)           (WorkerHandler)
 * </pre>
 */
public class StatusReporter {

    private static final Logger log = LoggerFactory.getLogger(StatusReporter.class);

    private final StreamObserver<com.scheduler.proto.job.StatusUpdate> requestObserver;
    private final CountDownLatch done;

    StatusReporter(StreamObserver<com.scheduler.proto.job.StatusUpdate> requestObserver, CountDownLatch done) {
        this.requestObserver = requestObserver;
        this.done = done;
    }

    public void report(com.scheduler.worker.StatusUpdate update) {
        com.scheduler.proto.job.StatusUpdate.Builder builder =
                com.scheduler.proto.job.StatusUpdate.newBuilder()
                        .setJobId(update.jobId());

        if (update.jobStatus() != null) {
            log.info("Reporting job status to coordinator: jobId={}, jobStatus={}{}",
                    update.jobId(), update.jobStatus(),
                    update.failureReason() != null
                            ? ", reason=" + update.failureReason().toMessage(update.failureDetail())
                            : "");
            builder.setJobStatus(toJobStatusProto(update.jobStatus()));
            if (update.failureReason() != null) {
                builder.setFailureReason(toFailureReasonProto(update.failureReason()));
                if (update.failureDetail() != null) {
                    builder.setFailureDetail(update.failureDetail());
                }
            }
        }

        if (update.taskStatus() != null) {
            log.info("Forwarding task status to coordinator: jobId={}, taskIndex={}, taskName={}, status={}",
                    update.jobId(), update.taskIndex(), update.taskName(), update.taskStatus());
            builder.setTaskIndex(update.taskIndex())
                    .setTaskStatus(toTaskStatusProto(update.taskStatus()));
            if (update.taskName() != null) {
                builder.setTaskName(update.taskName());
            }
            if (update.errorMessage() != null) {
                builder.setErrorMessage(update.errorMessage());
            }
        }

        requestObserver.onNext(builder.build());
    }

    public void complete() {
        requestObserver.onCompleted();
    }

    public boolean awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {
        return done.await(timeout, unit);
    }

    private static JobStatus toJobStatusProto(String status) {
        return switch (status) {
            case "STARTING" -> JobStatus.JOB_STATUS_STARTING;
            case "RUNNING" -> JobStatus.JOB_STATUS_RUNNING;
            case "COMPLETED" -> JobStatus.JOB_STATUS_COMPLETED;
            case "FAILED" -> JobStatus.JOB_STATUS_FAILED;
            case "KILLED" -> JobStatus.JOB_STATUS_KILLED;
            default -> JobStatus.JOB_STATUS_UNSPECIFIED;
        };
    }

    private static TaskStatus toTaskStatusProto(String status) {
        return switch (status) {
            case "RUNNING" -> TaskStatus.TASK_STATUS_RUNNING;
            case "COMPLETED" -> TaskStatus.TASK_STATUS_COMPLETED;
            case "FAILED" -> TaskStatus.TASK_STATUS_FAILED;
            default -> TaskStatus.TASK_STATUS_UNSPECIFIED;
        };
    }

    private static com.scheduler.proto.v1.FailureReason toFailureReasonProto(FailureReason reason) {
        return switch (reason) {
            case HEARTBEAT_LOST -> com.scheduler.proto.v1.FailureReason.FAILURE_REASON_HEARTBEAT_LOST;
            case PROCESS_TIMEOUT -> com.scheduler.proto.v1.FailureReason.FAILURE_REASON_PROCESS_TIMEOUT;
            case PROCESS_EXITED -> com.scheduler.proto.v1.FailureReason.FAILURE_REASON_PROCESS_EXITED;
            case PROCESS_START_FAILED -> com.scheduler.proto.v1.FailureReason.FAILURE_REASON_PROCESS_START_FAILED;
        };
    }
}
