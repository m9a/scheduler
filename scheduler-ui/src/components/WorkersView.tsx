import { api, type Worker } from "../api.ts";
import { usePolling } from "../usePolling.ts";
import { fmtAgo, fmtTime } from "../format.ts";

const POLL_MS = 3000;

// Worker fleet health: resources advertised at registration and heartbeat age.
// A stale "Last heartbeat" is the operator's signal a worker may be dead before
// the coordinator's monitor evicts it.
export function WorkersView() {
  const { data: workers, error, loading } = usePolling<Worker[]>(api.listWorkers, POLL_MS);

  if (error) return <p className="error">Failed to load workers: {error}</p>;
  if (loading && !workers) return <p className="muted">Loading…</p>;
  if (workers && workers.length === 0) return <p className="muted">No workers registered.</p>;

  return (
    <table>
      <thead>
        <tr><th>Host</th><th>CPU</th><th>Mem (MB)</th><th>GPU</th><th>Capabilities</th><th>Registered</th><th>Last heartbeat</th></tr>
      </thead>
      <tbody>
        {workers!.map((w) => (
          <tr key={w.id}>
            <td>{w.hostname}<div className="muted small">{w.id}</div></td>
            <td>{w.cpuCores}</td>
            <td>{w.memoryMb}</td>
            <td>{w.gpu ? "yes" : "no"}</td>
            <td>{w.capabilities.length ? w.capabilities.join(", ") : "—"}</td>
            <td>{fmtTime(w.registeredAt)}</td>
            <td>{fmtAgo(w.lastHeartbeat)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
