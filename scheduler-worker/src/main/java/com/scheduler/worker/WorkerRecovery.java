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
 * Boot recovery. Runs once, before the worker registers.
 *
 * <ul>
 *   <li>Reads the durable status store: which jobs was I running?</li>
 *   <li>Inspects each job's container: is it still there?</li>
 *   <li>Returns one {@link JobToReconcile} per in-flight job.</li>
 * </ul>
 *
 * <p>Detection only. {@link WorkerAgent} applies the fixed policy after register:
 * a Running container is re-attached; an Exited or Absent one is failed
 * best-effort (salvage outputs/logs, report NOT_FOUND_ON_RECOVERY); a job the
 * coordinator already marked terminal is killed.
 */
class WorkerRecovery {

    private static final Logger log = LoggerFactory.getLogger(WorkerRecovery.class);

    /** One in-flight job: its last stored state and the container state found on boot. */
    record JobToReconcile(String jobId, JobState jobState, ContainerState containerState) {}

    private final WorkerStatusStore store;
    private final ContainerInspector inspector;

    WorkerRecovery(WorkerStatusStore store, ContainerInspector inspector) {
        this.store = store;
        this.inspector = inspector;
    }

    /**
     * Reads the store and inspects each non-terminal job's container. Terminal
     * rows are skipped: their job already finished and was reported; the rows
     * only wait for re-delivery + ack (the register flush), not for recovery.
     */
    List<JobToReconcile> recover() {
        Map<String, JobState> jobStateById = new LinkedHashMap<>();
        for (StatusUpdate u : store.loadAllJobs()) {
            if (isJobEntry(u)) {
                jobStateById.put(u.getJobId(), u.getJobState());
            }
        }

        List<JobToReconcile> jobs = new ArrayList<>();
        for (Map.Entry<String, JobState> entry : jobStateById.entrySet()) {
            if (JobStates.isTerminal(entry.getValue())) {
                continue;
            }
            String jobId = entry.getKey();
            ContainerState state = inspector.containerState(jobId);
            logFinding(jobId, entry.getValue(), state);
            jobs.add(new JobToReconcile(jobId, entry.getValue(), state));
        }

        if (jobs.isEmpty()) {
            log.info("Boot recovery: no in-flight jobs to recover");
        }
        return jobs;
    }

    /**
     * True for a job-entry row. The store holds two row kinds: task entries
     * (task section set, from SDK updates) and one job entry per job (no task
     * section — written at claim and at terminal). The job entry carries the
     * authoritative job state.
     */
    private static boolean isJobEntry(StatusUpdate update) {
        return update.getTaskState() == TaskState.TASK_STATE_UNSPECIFIED;
    }

    /** Logs what recovery found; the agent acts on it after register. */
    private void logFinding(String jobId, JobState jobState, ContainerState state) {
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
