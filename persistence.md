# Coordinator State Persistence & Recovery

Resumable plan for the coordinator persistence epic (TODO #5). This file is the
single source of truth for the work; `TODO.md` links here. Written to survive a
session clear — it carries the decisions, not just the task list.

Status: ✅ done · 🔄 in progress · ⬜ pending.

## Goal

One coordinator mirrors its state to embedded SQLite, so a restart (crash, upgrade,
redeploy) recovers instead of losing jobs. Workers re-establish their state on
reconnect. No external DB, no orchestrator (see CLAUDE.md non-goals).

## Key decisions (context to keep)

- **One coordinator, embedded SQLite.** ACID durability, no network hop. Behind
  `JobStore` / `WorkerStore` interfaces; swapping to Postgres is one new impl.
- **Two stores, complementary — not redundant.** The coordinator store is the durable
  *system of record*: QUEUED jobs (which exist nowhere else), job *definitions*
  (artifactUri/params/resources — the worker flush carries status, not the definition),
  and terminal *history* for the UI/`getJob` (workers prune terminals after ack). The
  worker store is a transient *outbox* that re-asserts in-flight state and replays
  terminals not yet delivered. On boot the coordinator rebuilds the queue + job skeleton
  from its store; the worker flush then *freshens* in-flight state. Both store **one row
  per job** (latest-wins), not a per-transition log — per-state durations, if wanted
  later, are timestamp columns, not event-sourcing.
- **Worker owns job/task state.** The coordinator writes state in exactly one case:
  failing a job on heartbeat loss. Otherwise it applies and de-dupes what the worker
  sends.
- **Stable worker id** in `worker_checkpoint.yaml`, sent on every register — lets the
  coordinator match a reconnecting worker to the jobs it owns.
- **One idempotent `register` call — no "re-registration".** First boot and every
  reconnect run the *same* `register(known_jobs)` RPC + handler; only the payload
  differs (empty on a fresh worker, current snapshots on reconnect). The coordinator
  upserts the worker row (keyed by stable id, refreshing `lastHeartbeat`), so a seeded
  worker that re-registers just refreshes its own row — no duplicate. A seeded worker
  that never registers within `reregistrationTimeoutSeconds` is evicted by the monitor.
- **Reconnect is the universal resync trigger.** Coordinator-restart, network blip,
  and worker-restart all reduce to "(re)connected." The worker always sends its full
  current state on register. Within a healthy gRPC stream, delivery is ordered and
  reliable — only a *break* loses updates, so register-resync is both necessary and
  sufficient for in-flight state. (This replaces the earlier coordinator-driven
  `Resync`-command approach.)
- **Worker durable status store.** The worker mirrors the status it forwards to an
  embedded SQLite store — a *latest-wins snapshot per active job* (job state + task
  states), not an append log. On reconnect it re-flushes every active job's snapshot;
  the coordinator re-applies the current state idempotently. This covers the common
  outage (a coordinator upgrade that's slow or fails to come up): in-flight state is
  re-asserted, and a job that *finished* during the outage has its terminal status
  replayed instead of looking orphaned.
- **Ack is terminal-only and coarse.** Non-terminal jobs need no ack — they're
  re-asserted on every reconnect flush, so a missed non-terminal update (only possible
  on a stream break, which always triggers a reconnect) self-heals. Only a terminal
  status needs an ack, so the worker knows when to prune a job that's no longer active.
  No per-update acks, no batch ids, no bidi stream.
- **Push channel** (task #15, done): coordinator→worker server-streams. One pair per
  worker (system + job commands). Status/telemetry are one stream *per job*.
- **Multi-job worker (future).** A worker may run several jobs at once. The wire model
  already fits — declared jobs is a list, reconciliation is set-based, job commands
  target by `jobId`. The real change is worker execution concurrency + tracking a
  *set* of running jobs (today it tracks one `currentJobId`).

## Recovery model (why the phases are shaped this way)

- **QUEUED recovery is safe alone** — claim is pull-based with no ack, so a queued job
  was never handed out. On boot, just re-queue it.
- **A coordinator restart is not a worker or job death.** Workers and their containers
  (incl. live PyTorch training jobs) keep running through the outage. The goal is to
  **reattach** to real state, not assume-dead and restart — restarting a live training
  job throws away GPU-hours and progress.
- **In-flight (STARTING/RUNNING) recovery needs the worker to re-assert ownership.** On
  reconnect the worker re-declares its current jobs; the coordinator reconciles.
- **Heartbeat monitor as the boot guard.** The monitor only acts on workers in the
  registry. After restart, seed the registry from the store and delay the monitor's
  first scan by `reregistrationTimeoutSeconds`, so workers can reconnect before any
  in-flight job is failed.
- **Reconnect detection is worker-side.** The worker notices the link dropped (command
  stream `onError`, or `ManagedChannel` leaving `READY`) and runs its reconnect routine
  — register-with-state, then resume. The coordinator does not need to ask.

## Phases

### Phase 1 — JobStore ✅
`JobStore` + `SqliteJobStore` (jobs table, prepared statements, proto-number enums,
tasks as a JSON blob), `TaskStatus.restore()`.

### Phase 2 — write-through ✅
`JobManager` writes through on every state transition. `coordinator.dbPath` config.

### Phase 3 — persist worker registry ✅
`WorkerStore` + `SqliteWorkerStore` (workers table in the same db; `busy_timeout` on
both connections), `InMemoryWorkerStore` for tests. The registry writes through on
register, deletes on eviction. Persists id + placement info; `lastHeartbeat` is
overridden at boot.

### Phase 4 — boot rebuild + seed registry ✅
`JobManager.recover()` reloads non-terminal jobs, re-queues `QUEUED`, rebuilds the
`jobId → worker` map. `WorkerRegistry.seed(boot)` loads persisted workers. The
heartbeat monitor's first scan is delayed by `reregistrationTimeoutSeconds` (new
config). Boot order in `Coordinator.main`: recover → seed → start (delayed) monitor →
start gRPC server.

> **Paused — worker recovery first.** Slices 3–5 below are on hold until the worker
> failover epic ([worker-recovery.md](worker-recovery.md)) locks down what the worker
> persists for its *own* restart. That same state drives coordinator reconciliation, so
> settling it there first avoids reworking the Slice 2 schema. Slice 2 (worker status
> store) is done; the schema may still gain columns (e.g. `started_at`) from that work.

### Phase 5 — reconnect-driven resync ⬜
Collapse coordinator-restart / blip / startup into one reconnect path that always
re-asserts full state. The reconnect payload **is** the worker's flush of its durable
status store (Phase 6) — Phases 5 and 6 are one mechanism split across the wire and the
worker's local store.
- **Proto:** `RegisterWorkerRequest` gains `repeated StatusUpdate known_jobs` — the
  worker's current status snapshot per job it knows about (non-terminal current state
  plus any terminal-unacked). Reuses the one canonical status type (CLAUDE.md "one status
  message"); no separate declared-jobs message. Registration *is* the resync payload.
  `RegisterWorkerResponse` gains `repeated string acked_job_ids` — the terminal jobs the
  coordinator persisted in this call; the worker prunes exactly those.
- **Worker:** one `connect` routine (same on first boot and reconnect) —
  `register(known_jobs)` → start heartbeat → subscribe to command streams → prune
  `acked_job_ids`. Triggered on startup and whenever the link re-establishes (watch
  `ManagedChannel` `TRANSIENT_FAILURE → READY`). **Must run off the main loop** — while a
  job runs the loop is blocked in `executeJob`/`waitFor`, so a reconnect fires from the
  command-stream reconnect path, not the pull loop. `register` runs **before** resuming
  `PullJob`.
- **Coordinator:** ingest each re-sent snapshot via the existing idempotent
  `handleStatusUpdate` (same-state = no-op), persisting terminals so they can be acked.
  Then reconcile assigned-vs-known (see slices for the two directions): a job the
  coordinator assigned to this worker that the worker no longer lists (and isn't already
  terminal) is an orphan; a job the worker lists that the coordinator doesn't know is the
  reverse. Return `acked_job_ids` for the terminals persisted.
- **Resuming live reporting** — on reconnect re-open the per-job status **and** telemetry
  streams so the in-flight job resumes reporting; they don't reconnect today (opened once
  per job in `runJobContainer`). Without this an in-flight job goes silent after a
  coordinator restart even though it's running.
- **Remove the `Resync` command** — unused; reconnect-resync replaces it. Keep `Drain`
  (system stream) and `Cancel`/`Preempt` (job stream). (This also drops the worker's
  resync dispatch + `requestResync` helper added in #15.)

### Phase 6 — worker durable status store ⬜
The worker mirrors the status it forwards so nothing is lost across a coordinator
outage. Settled model (no per-update acks, no batch ids, no bidi):
- **Embedded SQLite on the worker** — `WorkerStatusStore` + Sqlite impl, mirroring the
  coordinator's `SqliteJobStore`. Holds a **latest-wins snapshot per active job**: one
  job row + its task rows, overwritten in place as updates flow through.
- **Write-through:** every status update the worker forwards is written to the store
  first (job state + task section). **Telemetry and liveness pings are NOT persisted** —
  lossy by design, separate stream.
- **Ack = terminal-only, coarse.** A terminal status is pruned when the coordinator
  confirms it applied it — via the live `ReportStatus` final response, or, if that path
  broke mid-outage, via `RegisterWorkerResponse.acked_job_ids` on the next reconnect
  flush. Non-terminal jobs need no ack: re-asserted on every reconnect flush, so a missed
  non-terminal update self-heals.
- **Retention bound.** Terminal rows are normally pruned on ack; a `worker` config
  `statusRetentionDays` (default 7) also prunes terminal-unacked rows older than the
  window, so a permanently-dead coordinator can't grow the worker DB without bound.
  Non-terminal rows are bounded by concurrent jobs (= 1 today).

Recovery flow (why this works):
- *Coordinator restart, worker stays up* — worker keeps running the job, re-registers
  with its snapshot, coordinator re-applies current state, live reporting resumes.
- *Job finished during the outage* — its terminal status sits unacked in the store, gets
  replayed on reconnect, coordinator records it instead of seeing it orphaned.

### Phase 5/6 implementation slices (reviewable steps)
Build in order; each is 1–2 classes per CLAUDE.md.
1. **Proto** — `RegisterWorkerRequest.known_jobs` (`repeated StatusUpdate`),
   `RegisterWorkerResponse.acked_job_ids` (`repeated string`); remove `Resync` + the
   worker's resync dispatch + `requestResync`. Regenerate.
2. **Worker status store** — `WorkerStatusStore` + Sqlite impl. See "Worker persistence
   — data model" below for the schema and API (`update` / `loadAllJobs` / `ack` / `prune`).
3. **Worker reconnect routine** — single `(re)connect` (register-with-`known_jobs` →
   heartbeat → resubscribe → prune `acked_job_ids`), fired off the main loop from the
   command-stream reconnect path.
4. **Coordinator ingest + ack** — ingest re-sent snapshots via `handleStatusUpdate`,
   persist terminals, return `acked_job_ids`.
5. **Re-open per-job streams** — on reconnect re-establish the per-job status + telemetry
   streams so the in-flight job resumes reporting.
6. **Orphan decision (coordinator-knows, worker-doesn't)** — fail-only vs fail+requeue vs
   grace-then-fail. Lean: fail with a distinct reason; requeue deferred to #6/#13.
7. **Reverse decision (worker-knows, coordinator-doesn't)** — terminal → ack + ignore
   (idempotent); non-terminal RUNNING unknown to coordinator → adopt from the snapshot
   (worker is the live source of truth) and log loudly.

### Worker persistence — data model
The worker has no domain status type (it only relays `StatusUpdate` protos), so the store
persists `StatusUpdate` *fields as columns* and rebuilds `StatusUpdate`s on load — no
proto↔domain conversion (CLAUDE.md "one status message").

- One table, `job_status`, one row per `(job_id, task_idx)`.
- `task_idx = -1` is the **job entry** — registers ownership + job state even before any
  task reports. `task_idx >= 0` is a **task entry**.
- Task columns (`task_name`, `task_state`, `error_message`) are **NULL** on the job entry.
- **Every forwarded update is written**, latest-wins per row — not just terminals.
- `completed_at` is set **only** on the job entry, **only** once the job is terminal — it
  is the retention marker, nothing more.
- No upfront task manifest (tasks are absent until they report, per the canonical state
  model), so task entries are upserted incrementally.

```sql
CREATE TABLE job_status (
  job_id         TEXT    NOT NULL,
  task_idx       INTEGER NOT NULL,   -- >=0 task entry; -1 job entry
  job_state      INTEGER NOT NULL,   -- proto enum number
  task_name      TEXT,               -- NULL on job entry
  task_state     INTEGER,            -- NULL on job entry
  error_message  TEXT,
  failure_reason INTEGER,            -- proto enum number, nullable
  failure_detail TEXT,
  updated_at     INTEGER NOT NULL,   -- epoch millis
  completed_at   INTEGER,            -- set on job entry once terminal; retention marker
  PRIMARY KEY (job_id, task_idx)
);
```

API (`WorkerStatusStore`):
- `update(StatusUpdate)` — write-through the latest status; job section → `-1` row, task
  section → `task_idx` row (latest-wins).
- `loadAllJobs()` — all still-unacked updates rebuilt as `StatusUpdate`s (the table only
  ever holds unacked rows), task entries ordered before the job entry per job (so task
  state lands before a terminal trips the guard). This is the `known_jobs` payload.
- `ack(jobId)` — coordinator confirmed the terminal; drop all the job's rows.
- `prune(Duration retentionPeriod)` — drop job entries terminal longer than the period,
  bounding a permanently-dead coordinator.

A job with only the `-1` entry (no task rows yet) flushes as a single job-level
`StatusUpdate` (jobId + job_state); task entries appear once tasks report.

### Coordinator vs worker state — roles & reconciliation
Both stores hold job/task state; they are **complementary, not duplicated**.

- **Coordinator store = system of record.** Owns QUEUED jobs (exist nowhere else), job
  *definitions*, and terminal *history* for the UI/`getJob`. Survives with no workers.
- **Worker store = outbox.** Only the jobs this worker runs or finished-but-unacked.
  Pruned aggressively (on ack / retention). Job: re-assert live state, replay missed
  outcomes.

Reconciliation rules (on register):
- Coordinator ingests each `known_jobs` update via `handleStatusUpdate` (idempotent;
  same-state = no-op).
- Worker lists a job, coordinator agrees → refresh (no-op if equal).
- Worker lists a terminal → coordinator records it, returns it in `acked_job_ids`, worker
  prunes.
- Coordinator assigned a job the worker no longer lists → **orphan** (Slice 6).
- Worker lists a job the coordinator doesn't know → **reverse** (Slice 7).
- Queue rebuilt from the coordinator store alone; running jobs from the coordinator
  skeleton + worker freshness.

Message exchange after a coordinator failover:

```
Worker (up, running job J)                Coordinator (restarting)
      │                                          │ boot: recover() from SQLite;
      │                                          │ seed registry; hold monitor
      │  command stream onError ──────────►      │ (reregistrationTimeoutSeconds)
      │  channel TRANSIENT_FAILURE → READY       │ up
      │                                          │
      │  register(worker_id, known_jobs=[J]) ───►│ ingest J (idempotent);
      │                                          │ reconcile assigned-vs-known
      │  ◄─ RegisterWorkerResponse(acked_job_ids)│
      │  prune acked terminals                   │
      │  restart heartbeat; resubscribe streams  │
      │  re-open per-job status+telemetry ──────►│ live reporting resumes
      │  resume PullJob                          │
```

### Phase 7 — eviction + retention ⬜
Evict terminal jobs from memory after they're persisted. Periodic sweep deletes
terminal jobs older than `retentionDays` (default 7) to bound the `jobs` table
(`JobStore.deleteTerminalCompletedBefore` already exists).

### Phase 8 — read-path split ⬜
List/history reads from SQLite (`listAll`); active-job detail from memory; `getJob`
falls back to the store. Decide where `jobCountsByState` reads from once terminal jobs
are evicted (memory = active only, or query the DB).

## Related tasks (in TODO.md)
- #6 worker churn — reschedule on worker death, reconnect tolerance.
- #12 coordinator + worker failover testing (kill/restart mid-job).
- #13 retrying failed jobs (attempt count in persisted state).
- #15 coordinator→worker command streams (✅, enables Drain/Cancel/Preempt).
- #18 resubscribe backoff + jitter.
- #19 worker tolerates coordinator-down on pull/heartbeat.
- #20 registration thundering-herd after outage (pace/stagger the reconnect flush).

## Open decisions
- Scope of Phase 5: whether re-opening the per-job status/telemetry streams (resume live
  reporting) lands in this slice or a follow-up (see TODO #20 thundering-herd is related).
- Multi-job worker execution model (when we move off single-job).
- `jobCountsByState` source after eviction (memory vs DB).

## Future README section — "Coordinator recovery after failover/crash/upgrade"
Write once Phases 5–8 land (don't document ahead of the code). Cover: what's persisted;
how the coordinator rebuilds in-memory state from SQLite on boot; the reconnect routine
+ `reregistrationTimeoutSeconds` window; assigned-vs-declared reconciliation; how the
worker gathers terminal vs non-terminal state (live containers for running, durable
outbox for terminal-unacked); the terminal-job retention/pruning window.
