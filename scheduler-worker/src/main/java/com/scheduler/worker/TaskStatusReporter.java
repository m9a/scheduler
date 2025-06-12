package com.scheduler.worker;

import com.scheduler.proto.v1.ReportTaskStatusRequest;
import com.scheduler.proto.v1.TaskStatus;
import com.scheduler.sdk.TaskStatusUpdate;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Streams task status updates from the worker to the coordinator over gRPC.
 * Created by {@link WorkerAgent#openTaskStatusStream()}.
 *
 * <p>WorkerAgent receives {@link TaskStatusUpdate} from JobRunner (via HTTP),
 * then passes them here to be converted to proto and streamed to the coordinator.
 *
 * <pre>
 * JobRunner ──HTTP──► WorkerAgent ──► TaskStatusReporter ──gRPC stream──► Coordinator
 * (job process)       (receives update)  (converts to proto)               (WorkerHandler)
 * </pre>
 */
public class TaskStatusReporter {

    private static final Logger log = LoggerFactory.getLogger(TaskStatusReporter.class);

    private final StreamObserver<ReportTaskStatusRequest> requestObserver;
    private final CountDownLatch done;

    TaskStatusReporter(StreamObserver<ReportTaskStatusRequest> requestObserver, CountDownLatch done) {
        this.requestObserver = requestObserver;
        this.done = done;
    }

    public void report(TaskStatusUpdate update) {
        log.info("Forwarding status to coordinator: jobId={}, taskIndex={}, taskName={}, status={}",
                update.jobId(), update.taskIndex(), update.taskName(), update.status());
        ReportTaskStatusRequest.Builder builder = ReportTaskStatusRequest.newBuilder()
                .setJobId(update.jobId())
                .setTaskIndex(update.taskIndex())
                .setStatus(toProto(update.status()));
        if (update.errorMessage() != null) {
            builder.setErrorMessage(update.errorMessage());
        }
        requestObserver.onNext(builder.build());
    }

    public void complete() {
        requestObserver.onCompleted();
    }

    public boolean awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {
        return done.await(timeout, unit);
    }

    private static TaskStatus toProto(com.scheduler.sdk.TaskStatus status) {
        return switch (status) {
            case RUNNING -> TaskStatus.TASK_STATUS_RUNNING;
            case COMPLETED -> TaskStatus.TASK_STATUS_COMPLETED;
            case FAILED -> TaskStatus.TASK_STATUS_FAILED;
        };
    }
}
