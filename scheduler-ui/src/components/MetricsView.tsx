// Metrics are owned by Grafana, not rebuilt here (decision: embed existing
// Grafana). Set VITE_GRAFANA_URL at build to the Grafana dashboard URL reachable
// over the Tailnet; without it we show a hint rather than a broken iframe.
const grafanaUrl = import.meta.env.VITE_GRAFANA_URL as string | undefined;

export function MetricsView() {
  if (!grafanaUrl) {
    return (
      <p className="muted">
        Metrics are served by Grafana. Set <code>VITE_GRAFANA_URL</code> at build time
        to embed the dashboard here.
      </p>
    );
  }
  return <iframe className="grafana" title="Grafana" src={grafanaUrl} />;
}
