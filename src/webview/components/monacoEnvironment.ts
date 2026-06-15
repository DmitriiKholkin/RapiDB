/**
 * Monaco editor worker bootstrap.
 *
 * Monaco looks for a `MonacoEnvironment.getWorker()` factory on the
 * `self` global. Inside the VS Code webview we never need real workers
 * (the editor is single-threaded and language services run in-process),
 * so we hand Monaco a no-op `Worker` backed by a Blob URL.
 *
 * Setting `MonacoEnvironment` is idempotent — the guard flag on the
 * `window` object keeps multiple `MonacoEditor` mounts from clobbering
 * each other's environment.
 */
type MonacoHostWindow = Window & {
  __rapidbMonacoEnvSet?: boolean;
};

type MonacoHostGlobal = typeof globalThis & {
  MonacoEnvironment?: {
    getWorker(): Worker;
  };
};

const ENV_FLAG = "__rapidbMonacoEnvSet" as const;

const NOOP_WORKER_SOURCE = "self.onmessage=function(){}";

/**
 * Install the no-op worker factory exactly once per window. Safe to call
 * repeatedly — the second call is a no-op.
 */
export function ensureMonacoEnvironment(): void {
  const w = window as MonacoHostWindow;
  if (w[ENV_FLAG]) {
    return;
  }
  w[ENV_FLAG] = true;

  const g = self as MonacoHostGlobal;
  g.MonacoEnvironment = {
    getWorker(): Worker {
      const blob = new Blob([NOOP_WORKER_SOURCE], {
        type: "application/javascript",
      });
      const url = URL.createObjectURL(blob);
      const worker = new Worker(url);
      URL.revokeObjectURL(url);
      return worker;
    },
  };
}
