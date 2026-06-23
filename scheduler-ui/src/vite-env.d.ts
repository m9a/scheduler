/// <reference types="vite/client" />

// Build-time config injected by Vite (VITE_-prefixed env vars). Both optional;
// see api.ts and MetricsView.tsx for the fallbacks.
interface ImportMetaEnv {
  readonly VITE_API_BASE?: string;
  readonly VITE_GRAFANA_URL?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
