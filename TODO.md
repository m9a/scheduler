# Scheduler — TODO

The task list we've been tracking. Status: ✅ done · 🔄 in progress · ⬜ pending.

| # | Status | Task |
|---|--------|------|
| 1 | ✅ | **Scope & build coordinator HTTP read API for UI** — pull-only JSON `/api/jobs`, `/api/jobs/{id}`, `/api/jobs/{id}/tasks`, `/api/workers`, served from the coordinator. |
| 2 | ✅ | **Build read-only React/TS monitoring UI** — Vite SPA (jobs, tasks, workers; embeds Grafana); coordinator serves the built bundle via `uiDir`. |
| 3 | ⬜ | **Capacity estimation for clients + UI access** — concurrent client gRPC + UI polling the single coordinator can afford; validate JSON serialization isn't the bottleneck before considering binary. |
| 4 | ⬜ | **Explore Caddy sidecar setup** — TLS-only per service (coordinator serves its own UI+API, no `/api` proxy); Cloudflare certs on the Tailnet; bind-address/exposure notes. |
| 5 | 🔄 | **Coordinator state persistence (P0)** — full phased plan in [persistence.md](persistence.md). Phases 1–4 ✅; 5 (reconnect-driven resync), 6 (terminal outbox), 7 (eviction + retention), 8 (read-path split) ⬜. |
| 6 | ⬜ | **Worker churn handling (P0)** — heartbeat-timeout eviction with grace, reschedule on worker death, reconnect tolerance, state reconciliation on reconnect. |
| 7 | ⬜ | **Checkpoint + artifacts to object store** — push/pull training checkpoints so a rescheduled job resumes instead of restarting. |
| 8 | ⬜ | **Colocation of data and execution (job)** — place/stage jobs near their input data. |
| 9 | ⬜ | **Docker layer caching for jobs** — pull only newer layers; pre-pull base images on workers. |
| 10 | 🔄 | **Explicit liveness field on `Report` proto** — add `last_active_ms` instead of overloading `timestamp_ms`. The entry-less-Report overload is gone (the liveness monitor no longer synthesizes reports); the worker now re-stamps `timestamp_ms` on real reports (`WorkerAgent.relayTelemetry`) — a dedicated field is still cleaner. |
| 24 | ⬜ | **Precise outcomes for lost containers (P2)** — recovery fails any non-running container with the coarse `NOT_FOUND_ON_RECOVERY`; the user reads the checkpoint to judge progress. Differentiate the scenarios (see README "Lost containers") and recover the real outcome: read the exit code for an EXITED container; persist the terminal state before container removal so a finished-then-crashed job isn't reported FAILED. |
| 23 | ⬜ | **Cadenced telemetry forwarding + aggregation (P2)** — today the worker forwards each SDK `Report` on arrival, re-stamped with its receive time (`WorkerAgent.relayTelemetry`). Instead buffer per-job reports and flush on a fixed cadence: aggregate/coalesce buffered entries and stamp one worker last-activity timestamp per flush. Cuts per-report gRPC overhead and gives the coordinator a steady last-activity signal. Pairs with #10 (explicit `last_active_ms`). |
| 11 | ⬜ | **Evaluate need for circuit breakers** — around external calls (coordinator↔worker gRPC, MinIO, Docker, SQLite, MLflow). |
| 12 | ⬜ | **Coordinator + worker failover testing** — kill/restart each mid-job; verify rebuild, reconciliation, no orphaned/duplicated jobs. |
| 13 | ⬜ | **Retrying failed jobs** — retry policy, retryable reasons, attempt count in persisted state, checkpoint-based resume. |
| 14 | 🔄 | **Stable worker identity + resync-on-register** — Slice A (worker owns stable id in `worker_checkpoint.yaml`, sent on register) ✅; Slice B (reconnect-driven resync reconciliation) ⬜ — now persistence.md Phase 5. |
| 21 | 🔄 | **Worker failover & recovery (EPIC, P0)** — full phased plan in [worker-recovery.md](worker-recovery.md). Done **before** finishing coordinator recovery: the worker's own restart needs decide what state we persist (Slice 2 schema), and that same state drives coordinator reconciliation — settling it once avoids reworking the schema. Covers: boot recover() reading the status store, orphaned-container reconciliation (kill+reschedule now, re-attach later), connection re-establishment, and the returning-worker-vs-coordinator reconciliation. Pulls in #6, #16, #18, #19, #20, #12, #7. |
| 22 | ⬜ | **Use a Docker client library instead of shelling out (P2)** — `JobLauncher`/`WorkerMetrics` build `docker …` command lines and parse stdout; replace with docker-java (or the engine HTTP API over the socket) for typed errors, no parsing, and structured events. Do it after worker recovery lands so re-attach logic is designed against the same client. |
| 20 | ⬜ | **Registration thundering-herd after outage (P1)** — when the coordinator returns, every worker re-registers and replays all its unacked status updates at once; that burst could overwhelm the coordinator. Stagger/backoff registration (jitter the reconnect, or have the coordinator pace/batch the resync replay) so N workers don't hammer it simultaneously. Related to #18 (resubscribe backoff). |
| 18 | ⬜ | **Command-stream resubscribe backoff + jitter (P2)** — replace the fixed 2s resubscribe delay with exponential backoff + jitter, so workers don't hammer a down coordinator or reconnect in a thundering herd when it returns. |
| 19 | ⬜ | **Worker tolerates coordinator-down on pull/heartbeat (P1)** — today `register`/`pullJob` use the blocking stub and throw `UNAVAILABLE` (crashing `run()`); wrap the main loop so the worker rides out a coordinator restart instead of relying on the supervisor to relaunch it. |
| 17 | ⬜ | **Integration tests go through `scheduler-client` (P2)** — all integration tests should submit/drive via the real `SchedulerClient` SDK, not generated gRPC stubs, so they exercise the user-facing path. Convert `WorkerJobLifecycleTest` (and any other stub-driven tests) accordingly. |
| 16 | ⬜ | **Deployment model + worker lifecycle (P1)** — supervision/restart for coordinator+worker, and the worker drain/drained state machine. Open questions captured below. |
| 25 | ✅ | **Register reconciliation (P1)** — boot register sends every stored row (`known_jobs`); coordinator ingests via the normal status path, answers `acked_job_ids` (worker drops rows) and `job_ids_to_kill`. Task PENDING→terminal edge added for latest-wins replay. Remaining Phase 5 piece — re-register on reconnect (without a worker restart) + re-opening per-job streams — stays under #5. |
| 26 | ⬜ | **Partition reconciliation (P2)** — network partition with both sides alive: heartbeats stop, the coordinator fails the jobs, but the worker never restarts so register reconciliation never runs; the zombie job runs to completion. Reconcile on heartbeat resume — coordinator pushes a kill for terminal jobs when a "lost" worker's heartbeats return. Rare on our small single-VPN clusters; documented in README "known gaps". |
| 15 | ✅ | **Coordinator→worker command streams (P0)** — keep existing unary + client-stream RPCs; added two server-streaming push channels split by originator: `SystemCommands` (resync, drain) and `JobCommands` (cancel, preempt). Coordinator push registry + worker subscribe/dispatch done. Job claim stays pull-based. Unblocks persistence P5 (resync). |

## Deployment model + worker lifecycle — open questions (task #16)

Exact questions to resolve:

- On the infra side, if the coordinator or worker dies, what tries to bring it back?
- It's time to think about the deployment model.
- Is there a state machine for the worker, and should we persist its current state
  (e.g. Draining or Drained), so when it is brought up it remembers it was being
  evicted or drained? It could continue to run but in a mode where it only continues
  to drain if it was draining.
- After draining, should it shut itself down? If so, how will it clear its drained
  state once maintenance is over or the network issue is repaired?
- What is the course of action if the healthcheck monitor detects a failure and asks
  the worker to evict or drain itself?

## Coordinator→worker command streams — phase detail (task #15)

Transport is right (gRPC, worker dials coordinator over the tailnet). Keep the
existing unary + client-stream RPCs; the gap is a coordinator→worker **push**
channel. Add it as **two server-streaming RPCs split by originator** — system vs
job commands — so the planes read cleanly and authorize/audit separately. gRPC,
not WebSocket (Java↔Java; typed protos/deadlines/auth). One HTTP/2 connection
carries all streams.

- Phase A ✅ — proto: `SystemCommands(SubscribeRequest) returns (stream SystemCommand)`
  (`Resync`/`Drain`) and `JobCommands(...) returns (stream JobCommand)`
  (`Cancel`/`Preempt`). Removed the now-redundant `HeartbeatResponse.should_drain`.
- Phase B ✅ — coordinator: `WorkerCommandStreams` registry stashes the per-worker
  outbound observer on subscribe (each guarded — gRPC forbids concurrent `onNext`),
  dropped on stream close; `WorkerHandler` push helpers `requestResync` / `drain` /
  `cancelJob` / `preemptJob` (return false if no open stream).
- Phase C ✅ — worker: `CoordinatorClient` subscribes to both streams with
  reconnect-after-delay; `WorkerAgent` dispatches — resync → re-`register`; drain →
  stop pulling (`draining` flag); cancel/preempt → `stopContainer` for the running job.
- Liveness = heartbeat + command-stream health. Job claim **stays pull-based**
  (no-orphaned-assignment).
- Follow-ups (now persistence.md): resync re-declares current jobs (Phase 5);
  cancel/preempt map to proper `CANCELLED`/preempted terminal states rather than the
  container's exit code.
