// Presentation helpers shared across views.

// Strips the proto enum prefix for display: "JOB_STATE_RUNNING" → "RUNNING",
// "TASK_STATE_COMPLETED" → "COMPLETED". The raw value stays the wire contract;
// this only affects what the operator reads.
export function shortState(state: string): string {
  return state.replace(/^(JOB|TASK)_STATE_/, "");
}

export function fmtTime(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
}

export function fmtAgo(iso: string | null): string {
  if (!iso) return "—";
  const ms = Date.now() - new Date(iso).getTime();
  if (Number.isNaN(ms)) return iso;
  const s = Math.round(ms / 1000);
  if (s < 60) return `${s}s ago`;
  const m = Math.round(s / 60);
  if (m < 60) return `${m}m ago`;
  return `${Math.round(m / 60)}h ago`;
}

// CSS class suffix per lifecycle family, so the stylesheet can colour states
// without enumerating every enum value.
export function stateClass(state: string): string {
  const s = shortState(state);
  if (["RUNNING", "STARTING"].includes(s)) return "state-active";
  if (["COMPLETED"].includes(s)) return "state-ok";
  if (["FAILED", "TIMEOUT", "KILLED"].includes(s)) return "state-bad";
  if (["QUEUED", "PENDING"].includes(s)) return "state-idle";
  return "state-other";
}
