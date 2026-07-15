package com.scheduler.worker;

import com.scheduler.proto.job.StatusUpdate;
import com.scheduler.proto.v1.JobState;
import com.scheduler.worker.JobLauncher.ContainerState;
import com.scheduler.worker.WorkerRecovery.JobToReconcile;
import com.scheduler.worker.persistence.InMemoryWorkerStatusStore;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit test for boot recovery's classification. A stub inspector stands in for
 * docker, so recover() runs without a daemon.
 */
class WorkerRecoveryTest {

    // Three in-flight jobs get one job each from their container's state;
    // the already-terminal job is skipped.
    @Test
    void recover() {
        InMemoryWorkerStatusStore store = new InMemoryWorkerStatusStore();
        store.update(jobEntry("run-1", JobState.JOB_STATE_STARTING));    // running container
        store.update(jobEntry("exit-1", JobState.JOB_STATE_STARTING));   // exited container
        store.update(jobEntry("gone-1", JobState.JOB_STATE_STARTING));   // absent container
        store.update(jobEntry("done-1", JobState.JOB_STATE_COMPLETED));  // terminal → ignored

        Map<String, ContainerState> byId = Map.of(
                "run-1", ContainerState.RUNNING,
                "exit-1", ContainerState.EXITED,
                "gone-1", ContainerState.ABSENT);
        WorkerRecovery recovery = new WorkerRecovery(store, byId::get);

        List<JobToReconcile> jobs = recovery.recover();

        // done-1 is terminal, so it is not among the jobs.
        Map<String, ContainerState> stateById = jobs.stream()
                .collect(Collectors.toMap(JobToReconcile::jobId, JobToReconcile::containerState));
        assertEquals(3, stateById.size());
        assertEquals(ContainerState.RUNNING, stateById.get("run-1"));
        assertEquals(ContainerState.EXITED, stateById.get("exit-1"));
        assertEquals(ContainerState.ABSENT, stateById.get("gone-1"));
        assertFalse(stateById.containsKey("done-1"), "terminal job should be skipped");
    }

    // An empty store means nothing to recover.
    @Test
    void recoverEmpty() {
        WorkerRecovery recovery = new WorkerRecovery(new InMemoryWorkerStatusStore(),
                jobId -> ContainerState.ABSENT);
        assertTrue(recovery.recover().isEmpty());
    }

    private static StatusUpdate jobEntry(String jobId, JobState state) {
        return StatusUpdate.newBuilder().setJobId(jobId).setJobState(state).build();
    }
}
