import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// In dev, the Vite dev server hosts the UI and forwards /api/* to the
// coordinator's HTTP server (default :8080), which serves the read API under
// /api. No path rewrite: the coordinator already expects the /api prefix. In
// prod the coordinator serves both the built UI and /api itself (same origin),
// so this proxy only exists to bridge the two dev servers. Override the target
// with COORDINATOR_HTTP when the coordinator runs elsewhere.
const coordinatorHttp = process.env.COORDINATOR_HTTP ?? "http://localhost:8080";

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api": {
        target: coordinatorHttp,
        changeOrigin: true,
      },
    },
  },
});
