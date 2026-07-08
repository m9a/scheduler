package com.scheduler.worker;

import com.scheduler.core.JobStates;
import com.scheduler.proto.job.StatusUpdate;
import com.scheduler.proto.v1.JobState;
import com.scheduler.proto.v1.TaskState;
import com.scheduler.worker.JobLauncher.ContainerState;
import com.scheduler.worker.persistence.WorkerStatusStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Boot recovery. Runs once before the worker registers. Reads the durable status
 * store, inspects each in-flight job's container, and decides its fate.
 *
 * <p>Detection only — {@link WorkerAgent} acts on the decisions after register:
 * re-attach a Running container; fail an Exited or Absent one best-effort
 * (salvage outputs/logs, report NOT_FOUND_ON_RECOVERY).
 */
class WorkerRecovery {

    private static final Logger log = LoggerFactory.getLogger(WorkerRecovery.class);

    /** One in-flight job and the container state recovery found for it. */
    record RecoveryDecision(String jobId, ContainerState containerState) {}

    private final WorkerStatusStore store;
    private final ContainerInspector inspector;

    WorkerRecovery(WorkerStatusStore store, ContainerInspector inspector) {
        this.store = store;
        this.inspector = inspector;
    }

    /**
     * Reads the store and inspects each non-terminal job's container. Returns the
     * per-job decisions later slices will act on. Terminal rows are left for the
     * register flush to deliver — recovery ignores them.
     */
    List<RecoveryDecision> recover() {
        Map<String, JobState> jobStateById = new LinkedHashMap<>();
        for (StatusUpdate u : store.loadAllJobs()) {
            // The job entry (no task section) carries the authoritative job state.
            if (u.getTaskState() == TaskState.TASK_STATE_UNSPECIFIED) {
                jobStateById.put(u.getJobId(), u.getJobState());
            }
        }

        List<RecoveryDecision> decisions = new ArrayList<>();
        for (Map.Entry<String, JobState> entry : jobStateById.entrySet()) {
            if (JobStates.isTerminal(entry.getValue())) {
                continue;
            }
            String jobId = entry.getKey();
            ContainerState state = inspector.containerState(jobId);
            logDecision(jobId, entry.getValue(), state);
            decisions.add(new RecoveryDecision(jobId, state));
        }

        if (decisions.isEmpty()) {
            log.info("Boot recovery: no in-flight jobs to recover");
        }
        return decisions;
    }

    /** Logs each decision; the agent acts on all of them after register. */
    private void logDecision(String jobId, JobState jobState, ContainerState state) {
        switch (state) {
            case RUNNING ->
                log.info("Boot recovery: jobId={} (state={}) container running → re-attach after register", jobId, jobState);
            case EXITED ->
                log.warn("Boot recovery: jobId={} (state={}) container exited → fail + salvage after register",
                        jobId, jobState);
            case ABSENT ->
                log.warn("Boot recovery: jobId={} (state={}) container absent → fail after register", jobId, jobState);
        }
    }
}
