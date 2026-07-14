# Worker Failover & Recovery (Epic)

Plan for the worker-side recovery epic (TODO #21). We do this **before** finishing
coordinator recovery (persistence.md Phases 5–8), because what the worker needs to recover
itself determines what state we persist — and that state is also what the coordinator uses
for reconciliation. Deciding it once, here, avoids reworking the Slice 2 schema later.

Status: ✅ done · 🔄 in progress · ⬜ pending.

## Resume point (updated 2026-07-10)

Slices 1–5 are done and tested (worker-repo slices uncommitted on branch
`worker-recovery`; Slice 4 uncommitted in the sibling `scheduler-sdk` repo).
**Next: Slice 6 (coordinator slow-restart reconciliation).**
The docker-java client migration (TODO #22) is deferred to after the epic.

What exists now:
- `JobLauncher` runs containers detached; `spawn()` = `launchDetached` →
  `followLogs` → `awaitJobTermination`, one `finally { finalizeContainer }`.
  `attachAndWait(jobId, logFile)` re-runs the same watch on an existing container
  and blocks until the job finishes.
- `ContainerState` is a plain enum (`RUNNING, EXITED, ABSENT`) — no exit code in
  detection; recovery never reads it (coarse fail by design, TODO #24).
- `ContainerInspector` seam: `JobLauncher` implements it; `WorkerAgent` delegates
  to it and is what recovery gets, so agent tests stub the probe.
- `WorkerAgent` writes through `WorkerStatusStore` (job entry STARTING at claim,
  task updates, terminal); coordinator's status-stream close ack → `store.ack(jobId)`.
- **Ack integrity:** the close ack is the `StatusUpdateResponse` on the per-job
  `ReportStatus` stream; the worker records it in `onNext` (logged) and
  `CoordinatorStatusStream.awaitCompletion` returns true only if it arrived. A
  stream error before the ack keeps the rows for the register flush — it no
  longer counts as acked. Documented in README ("Rows leave the store only on
  the coordinator's ack").
- `WorkerRecovery.recover()` runs before register: reads store, inspects each
  non-terminal job's container, returns + logs `RecoveryDecision(jobId, state)`.
- **Re-attach (Slice 3):** after register/subscribe and before the pull loop, the
  agent acts on each RUNNING decision: re-open per-job streams, fresh liveness
  monitor, rebind handlers, re-assert job RUNNING (coordinator may still be at
  STARTING — it can't go terminal from there; de-duped if already RUNNING), then
  block on `attachAndWait()` and finish via the normal terminal/upload/cleanup/ack
  path.
- **Lost containers (Slice 5, simplified by design):** EXITED and ABSENT both fail
  best-effort with one coarse reason, `FAILURE_REASON_NOT_FOUND_ON_RECOVERY` — no
  exit-code reading, no outcome reconstruction; the user reads the checkpoint to
  judge progress (`WorkerAgent.failLostJob`). Salvage first (logs via
  `JobLauncher.salvageLogs` for EXITED, plus output upload), status-only report,
  and the exited container is removed only after the coordinator's ack, so a crash
  mid-recovery re-runs to the same result. Scenarios listed in README "Lost
  containers"; precise outcomes are TODO #24.
- Related changes landed alongside the slices: a task that reports FAILED fails
  the job even on container exit 0; liveness is fully worker-local (the monitor
  sends nothing — the worker stamps its `lastLivenessAt` onto forwarded telemetry;
  internal renames `lastActivity*` → `lastLivenessAt`, public proto/UI unchanged;
  cadenced forwarding is TODO #23).
- **SDK reconnect (Slice 4, `scheduler-sdk` repo, simplified by design):** status
  updates go into an in-memory FIFO queue in the SDK; a sender thread delivers
  them in order and pops one only after the worker's ack (worker acks after its
  store write, so ack = durable). On a failed send / missing ack the sender
  reconnects with capped backoff + jitter (0.5s → 30s) and resends from the
  queue head, forever — task threads never block, so tasks finished during a
  worker outage queue up and arrive in order after reconnect (no state-skip,
  no coordinator change). `close()` drains the queue before exit. Design choices
  made with the developer: no dedicated drop-detection (a failed send is the only
  reliable signal — close/error callbacks don't fire on host loss and Python's
  blocking client has none; the 15s liveness ping bounds detection), telemetry
  stays lossy, worker side unchanged (`JobCallbackHandler` already accepts new
  connections — every lifecycle test posts each status on a fresh socket).
  Verified: `JobReporterTest` (embedded WS server killed + restarted mid-job)
  and `test_worker_restart_redelivers_queued_statuses` (mocked socket), plus
  both full SDK suites.
- README: "Worker Failover & Recovery" (Design choices incl. store-ack lifecycle,
  Boot recovery table, "Re-attach", "Lost containers"), "Worker process model
  (docker CLI children)", "Container removal and logs" sections; sdk README
  "SDK delivery guarantees".
- CLAUDE.md: "Keep slices small and reviewable" process rule.

Conventions to keep: slices small and reviewable, design approved before code,
brief active-voice comments, State=enum / Status=object naming, no anonymous
functions passed around, short test names with a 1–2 line scenario comment.

Deferred: docker-java client migration (TODO #22, after the epic); precise
outcomes for lost containers (TODO #24); cadenced telemetry forwarding (TODO #23).

## Goal & scope

When the worker **process** restarts (crash, OOM, upgrade, redeploy, host reboot), it must
come back to a correct, consistent state instead of leaking work or lying to the
coordinator. This is distinct from a coordinator restart (where the worker stays up).

- **In scope** — reading persisted state on boot, deciding the fate of jobs that were
  in flight, cleaning up or re-attaching to their containers, and re-establishing the
  connections a job needs, all before the worker resumes normal operation.
- **Out of scope (separate work)** — checkpoint-based resume of training progress (#7),
  the deployment/supervision layer that relaunches the process (#16), and the SDK-side
  changes needed for a container to reconnect to a restarted worker (noted as a dependency).

## The core problem

- A job container is started as a **foreground `docker run --rm` child** of the worker and
  watched with `Process.waitFor`. When the worker dies, the container is **orphaned**: it
  may still be running under the Docker daemon, may have exited, or may be gone. Its `--rm`
  cleanup does not run if the client process was killed.
- All the worker's live knowledge of the job is **in memory** and is lost: which job was
  running, the process handle used to watch it, the execution-timeout timer, and the
  liveness monitor's counters.
- The job's SDK was connected to the worker over a **WebSocket** (status + telemetry). That
  connection dies with the worker.
- So on restart the worker knows nothing about the job unless it reads it back from the
  status store — and even then it is no longer watching the container.

## What the worker must decide, per in-flight job

For each non-terminal job found in the store on boot, pick one:

- **Re-attach** — find the still-running container, resume watching it (exit code,
  liveness), and re-establish its connections. Correct for long training jobs (no lost
  GPU-hours). **Implemented (Slice 3)**; SDK-side reconnect is the remaining piece (Slice 4).
- **Fail best-effort** — for a container no longer running: salvage outputs/logs, report
  FAILED / `NOT_FOUND_ON_RECOVERY`, never relaunch; the user reads the checkpoint.
  **Implemented (Slice 5)**. (The original kill + reschedule idea was dropped — jobs are
  not idempotent, nothing is ever silently re-run.)

## Checklist — everything worker recovery must account for

**Identity & persisted state**
- [x] Stable worker id from `worker_checkpoint.yaml`, sent on register (#14) — lets the
  coordinator match the restarted worker to its prior jobs.
- [x] Durable per-job status store (Slice 2) — the record of "what I was running."
- [x] **Job start time** — no longer needed: the job run deadline was removed (liveness
  owns kills), so there is no remaining-timeout to compute on re-attach.
- [ ] **Execution phase** (staging inputs / running / uploading outputs) — where the job was
  interrupted changes the cleanup. Decide whether we need to persist it.
- [ ] **Drain state** — if the worker was told to stop taking new jobs and then restarts,
  should it come back still draining (e.g. mid-maintenance)? If yes, persist it (checkpoint,
  not per-job). (Open question from #16.)

**Container reconciliation**
- [x] Detect each container's real state on boot (`docker inspect job-{id}`): running,
  exited, or absent.
- [x] Lost-container path (Slice 5): salvage logs/outputs, report FAILED /
  `NOT_FOUND_ON_RECOVERY`, remove after ack, clean temp dirs.
- [x] Re-attach path (Slice 3): resume watching via `attachAndWait` (no timeout to
  re-arm — the run deadline was removed).
- [x] Idempotency: rows drop and the exited container is removed only after the
  coordinator's ack; re-running recovery reaches the same result.

**Connections to re-establish**
- [x] Job→worker **WebSocket** (SDK to `JobCallbackServer`): SDK reconnects on send
  failure with capped backoff; un-acked status updates queue and redeliver (Slice 4).
- [x] Worker→coordinator **per-job status + telemetry streams**: re-opened per
  re-attached job (`openReportingChannel`).
- [x] **Liveness monitor**: restarts fresh on re-attach; the startup-timeout window resets
  so a briefly-unwatched container isn't instantly judged stalled.

**Coordinator interaction**
- [x] Fast restart (< `heartbeatTimeout`): coordinator never noticed; worker re-declares
  (re-attach re-asserts job RUNNING) and continues.
- [ ] Slow restart (> `heartbeatTimeout`): coordinator's monitor already failed the job as
  HEARTBEAT_LOST and may have rescheduled it. The returning worker must reconcile with that
  — it cannot re-assert a job the coordinator already gave to someone else. (Slice 6.)
- [x] Register with the **reconciled** state (after boot recovery), never the raw stale
  "still RUNNING" state.

**Boot ordering**
- [x] load config → resolve stable id → open status store → **run worker recover()** →
  register → start heartbeat → subscribe → act on recovery decisions → enter pull loop.

**Interim safety (until recover() exists)**
- [x] Superseded — recover() exists and runs on every boot.

## Impact on the Slice 2 schema (the reason we do this first)

Decide now, so we persist the right thing once:
- **Add `started_at`** to the status store? (Needed for remaining-timeout on re-attach.)
- **Add an execution `phase`?** (staging / running / uploading.)
- **Persist drain state** somewhere (checkpoint vs store)?
- Everything else the checklist above implies.

## Dependencies on other work
- #7 checkpoint + artifacts to object store — for progress-preserving resume.
- #16 deployment model + worker lifecycle — supervision that relaunches the process; the
  worker drain/drained state machine.
- SDK-side reconnect (sibling `scheduler-sdk`) — required before re-attach is viable.

## Stage 1 — Job/task recovery with container + WebSocket re-attach

First stage, chosen deliberately: **re-attach to running containers**, not kill-and-restart.
Training jobs are long and their containers produce model/checkpoint files, so a job is not
safe to relaunch — the user would have to clean the output dir by hand. So a worker that
comes back must resume watching the container that is still running, and a container whose
work is genuinely lost is **failed**, never silently re-run.

Spans two repos: the worker (this repo) and the SDK (sibling `scheduler-sdk`), because the
container's SDK holds the other end of the WebSocket and must reconnect.

### Foundation change — run containers detached, watch by name
Today a container is a foreground `docker run --rm` child watched with `Process.waitFor`.
That is exactly what makes a worker crash orphan the container and lose the exit code. Change:
- **Run detached** (`docker run -d`, no `--rm`), watch with `docker wait job-{id}`, and remove
  the container explicitly after finalizing. Now normal running and re-attach use the *same*
  watch mechanism, the container is always daemon-managed, and an exited container survives
  long enough to read its exit code.

### What we persist for this stage
- Job/task state — already in the Slice 2 store. **No schema additions needed.**
- ~~`started_at`~~ — was planned for re-arming the *remaining* execution timeout on
  re-attach; the job run deadline has since been removed (no `jobExecutionTimeoutMinutes`),
  so there is nothing to re-arm. Liveness restarts fresh on re-attach.
- No "phase" column — the situation is derived from job/task state + live container inspection.

### Worker boot `recover()`
Runs before register. For each non-terminal job in the store, inspect `job-{id}`:
- **Running** → re-attach (below).
- **Exited** → finalize: read the exit code, upload outputs/logs, report the terminal state.
- **Absent** → cannot re-attach and must not relaunch → report FAILED (reason: container lost
  on worker restart). User decides what to do with any partial output.

### Re-attach path (running container)
- Resume watching via `docker wait job-{id}` in place of the dead child process.
- Restart the liveness monitor fresh, resetting the startup-timeout window so a briefly
  unwatched container isn't judged stalled immediately. (No run deadline to re-arm — the
  execution timeout was removed.)
- Re-open the per-job coordinator status + telemetry streams.
- Rebind the JobCallbackServer handlers for this job and wait for the SDK to reconnect.

### SDK side (`scheduler-sdk`)
The container and its SDK never died, so the SDK's in-memory task state is intact.
- WebSocket client detects the drop and reconnects to the same worker URL (host:port is
  stable across a worker restart — it's in the container's launch payload).
- **How the SDK detects the worker died** (connection is SDK-initiated, so detection is
  SDK-side too). Options:
  - *Close/error event* — a worker crash resets the TCP connection; the WS client's
    onClose/onError fires. Covers the common case for free.
  - *Send-failure* — the SDK sends status/liveness pings regularly; a failed send is
    treated as a drop. Catches half-open connections the close event misses.
  - *WS ping/pong timeout* — protocol-level keepalive for a fully silent link.
  - Lean: close/error + send-failure (the SDK already sends periodic liveness pings, so
    a dead link surfaces within one ping interval); add ping/pong only if half-open
    links show up in practice.
- **Reconnect loop** — retry with capped backoff + jitter, indefinitely: the SDK cannot
  tell a crashed worker from a restarting one, and the worker kills genuinely stalled
  containers anyway. Status updates made while disconnected are buffered (the SDK's
  acked-WebSocket queue) and delivered after reconnect.
- On reconnect the SDK re-identifies its job and **re-asserts its current task states**, so
  the worker (and coordinator) re-learn any task transition that happened during the gap.
  This is the same "live side re-declares to the restarted watcher" pattern the worker uses
  with the coordinator, one level down.

### Worker JobCallbackServer
- Accept a reconnecting SDK for an in-flight job, re-associate the connection with that job,
  and rebind status/telemetry/liveness handling — the same wiring `runJobContainer` does at
  launch, reused on re-attach.

### Coordinator interaction
- Fast restart (< `heartbeatTimeout`): the coordinator never noticed; re-attach + register
  re-declares and continues.
- Slow restart (> `heartbeatTimeout`): the coordinator's monitor already failed the job. The
  returning worker must detect that the job is already terminal on the coordinator and stop +
  clean up its container rather than resurrect it. (Reconciliation rule to build.)

### Edge cases
- Exited container already removed (shouldn't happen once we drop `--rm`, but host-level
  cleanup could): exit code is unknowable → report FAILED.
- Container gone while job state says a task was mid-run → FAILED, no relaunch (non-idempotent).

### Stage 1 slices
1. ✅ **Detached run + watch-by-name** refactor in `JobLauncher` (drop foreground child and
   `--rm`): `docker run -d`, logs via `docker logs -f` follower thread, exit code via
   `docker wait`, explicit `docker rm` after finalizing. Verified by a real-docker unit
   test + full worker suite incl. the Lightning integration test. (Follow-up landed: the
   job run deadline was removed entirely — no `jobExecutionTimeoutMinutes`; liveness owns
   kills — so the once-planned `started_at` column is no longer needed.)
2. ✅ **`recover()` skeleton** — wired the durable status store into `WorkerAgent`
   (write-through at claim/task/terminal, coarse terminal ack drops rows);
   `JobLauncher.containerState` (docker inspect → Running/Exited/Absent) behind a
   `ContainerInspector` seam; `WorkerRecovery.recover()` runs before register, reads
   the store, inspects each non-terminal job's container, returns + logs the decision
   per job (re-attach / finalize / fail), no action yet. Verified by unit tests
   (`WorkerRecoveryTest`) + a real-docker state-detection test.
3. ✅ **Re-attach path** — act on a RUNNING decision:
   - `JobLauncher.attachAndWait(jobId, logFile)`: skip `docker run`, start the log
     follower and `docker wait` on the existing `job-{id}` (same finalize).
   - `WorkerAgent.recoverJob`: re-open the per-job status + telemetry streams,
     fresh liveness monitor (startup window resets), rebind JobCallbackHandler
     handlers, re-assert job RUNNING (a crash before any task update leaves the
     coordinator at STARTING, which can't go terminal), then block on the
     re-attached wait like a normal job (same terminal/upload/cleanup/ack path).
   - Verified: `reattach` agent test (worker dies mid-job, restarted worker with
     the same store re-attaches and reports COMPLETED; ack drops rows) + a
     real-docker `attach` test in JobLauncherTest.
4. ✅ **SDK WebSocket reconnect + redeliver** (`scheduler-sdk`) — send failure /
   missing ack marks the conn dead; reconnect loop with capped backoff + jitter;
   un-acked status updates queue in order and redeliver after reconnect (subsumes
   the planned "re-assert": every transition is delivered, none skipped). Worker
   side needed no change — `JobCallbackHandler` already accepts new connections.
5. ✅ **Fail paths** — act on EXITED and ABSENT decisions (simplified from the
   original finalize plan, by design — keep the logic easy to reason with):
   - Both → salvage (logs via `salvageLogs` for EXITED + output upload), report
     FAILED / `NOT_FOUND_ON_RECOVERY` on a status-only stream, ack drops rows.
     No exit-code reading; the user reads the checkpoint (precise outcomes: TODO #24).
   - Idempotent: rows leave the store only on ack; the exited container is
     removed only after the ack, so a crash mid-recovery re-runs safely.
   - Verified: `absentOnRecovery` / `exitedOnRecovery` agent tests + a
     real-docker `salvageLogs` test in JobLauncherTest.
6. ⬜ **Coordinator reconciliation** for the slow-restart case — the coordinator
   already failed the job (HEARTBEAT_LOST); the returning worker must detect the
   job is terminal on the coordinator and stop + clean up its container instead
   of re-asserting it.

## Later phases (after Stage 1)
- **Drain/drained state across restart** — persist and resume it (open question from #16).
- **Checkpoint-based resume** (#7) — for the *absent-container* case, let a job resume from
  its last checkpoint instead of failing, once training checkpoints are in the object store.
- **Multi-job recovery** — re-attach a *set* of running jobs when the worker runs more than
  one at a time.

## When we resume coordinator recovery
Return to persistence.md Slices 3–5 once the persisted-state decisions here are locked, so
the Slice 2 schema doesn't need reworking.
