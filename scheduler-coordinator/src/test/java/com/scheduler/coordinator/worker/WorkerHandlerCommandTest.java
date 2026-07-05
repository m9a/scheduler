package com.scheduler.coordinator.worker;

import com.scheduler.coordinator.JobManager;
import com.scheduler.coordinator.persistence.InMemoryJobStore;
import com.scheduler.coordinator.persistence.InMemoryWorkerStore;
import com.scheduler.proto.v1.FailureReason;
import com.scheduler.proto.worker.JobCommand;
import com.scheduler.proto.worker.SubscribeRequest;
import com.scheduler.proto.worker.SystemCommand;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WorkerHandlerCommandTest {

    private WorkerHandler newHandler() {
        return new WorkerHandler(new JobManager(new InMemoryJobStore()), new InMemoryWorkerStore());
    }

    /** Collects pushed commands; in this unit test onNext runs on the calling thread. */
    private static final class Collector<T> implements StreamObserver<T> {
        final List<T> received = new ArrayList<>();
        @Override public void onNext(T value) { received.add(value); }
        @Override public void onError(Throwable t) {}
        @Override public void onCompleted() {}
    }

    @Test
    void systemPushAfterSubscribe() {
        WorkerHandler handler = newHandler();
        Collector<SystemCommand> stream = new Collector<>();
        handler.systemCommands(SubscribeRequest.newBuilder().setWorkerId("w1").build(), stream);

        assertTrue(handler.drain("w1", true));

        assertEquals(1, stream.received.size());
        assertEquals(SystemCommand.KindCase.DRAIN, stream.received.get(0).getKindCase());
        assertTrue(stream.received.get(0).getDrain().getDrain());
    }

    @Test
    void jobPushAfterSubscribe() {
        WorkerHandler handler = newHandler();
        Collector<JobCommand> stream = new Collector<>();
        handler.jobCommands(SubscribeRequest.newBuilder().setWorkerId("w1").build(), stream);

        assertTrue(handler.cancelJob("w1", "job-1", FailureReason.FAILURE_REASON_UNSPECIFIED));

        assertEquals(1, stream.received.size());
        assertEquals(JobCommand.KindCase.CANCEL, stream.received.get(0).getKindCase());
        assertEquals("job-1", stream.received.get(0).getCancel().getJobId());
    }

    // No open stream → push is a no-op that reports it didn't deliver (worker will
    // resync on reconnect), rather than throwing.
    @Test
    void pushWithoutSubscribe() {
        WorkerHandler handler = newHandler();
        assertFalse(handler.drain("ghost", true));
        assertFalse(handler.cancelJob("ghost", "job-1", FailureReason.FAILURE_REASON_UNSPECIFIED));
    }
}
