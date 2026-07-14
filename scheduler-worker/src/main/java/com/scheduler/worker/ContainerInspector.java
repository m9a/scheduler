package com.scheduler.worker;

import com.scheduler.worker.JobLauncher.ContainerState;

/**
 * The one docker call boot recovery needs: read a job container's state.
 * {@link JobLauncher} implements it against docker; {@link WorkerAgent} delegates
 * to it and is what {@link WorkerRecovery} gets, so tests can stub the probe and
 * run recovery without docker.
 */
interface ContainerInspector {
    ContainerState containerState(String jobId);
}
