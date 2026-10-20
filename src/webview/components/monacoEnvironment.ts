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
