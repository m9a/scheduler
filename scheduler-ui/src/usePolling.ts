import { useCallback, useEffect, useRef, useState } from "react";

export type PollState<T> = {
  data: T | null;
  error: string | null;
  loading: boolean;
};

// Re-runs `fetcher` every `intervalMs` and on dependency change. Used instead of
// SSE/WebSocket because the read API is pull-only by design — the UI polls.
// Keeps the last good data visible while a refresh is in flight, and skips
// setState after unmount so a slow response can't write to a dead component.
export function usePolling<T>(
  fetcher: () => Promise<T>,
  intervalMs: number,
  deps: unknown[] = [],
): PollState<T> & { refresh: () => void } {
  const [state, setState] = useState<PollState<T>>({ data: null, error: null, loading: true });
  const alive = useRef(true);

  // eslint-disable-next-line react-hooks/exhaustive-deps
  const run = useCallback(fetcher, deps);

  const tick = useCallback(async () => {
    try {
      const data = await run();
      if (alive.current) setState({ data, error: null, loading: false });
    } catch (e) {
      if (alive.current) {
        setState((prev) => ({ ...prev, error: (e as Error).message, loading: false }));
      }
    }
  }, [run]);

  useEffect(() => {
    alive.current = true;
    tick();
    const handle = setInterval(tick, intervalMs);
    return () => {
      alive.current = false;
      clearInterval(handle);
    };
  }, [tick, intervalMs]);

  return { ...state, refresh: tick };
}
