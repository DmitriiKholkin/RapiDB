import { AsyncLocalStorage } from "node:async_hooks";

interface MetadataContext {
  signal: AbortSignal;
  deadline: number;
  cleanups: Set<() => void>;
}

const metadataContext = new AsyncLocalStorage<MetadataContext>();

/** Internal scope: metadata signatures stay unchanged, including nested calls. */
export async function runWithMetadataCancellation<T>(
  signal: AbortSignal,
  deadline: number,
  run: () => Promise<T>,
): Promise<T> {
  const context: MetadataContext = { signal, deadline, cleanups: new Set() };
  const cleanup = () => {
    for (const close of context.cleanups) {
      try {
        close();
      } catch {
        /* Cleanup must not replace the operation error. */
      }
    }
    context.cleanups.clear();
  };
  signal.addEventListener("abort", cleanup, { once: true });
  try {
    return await metadataContext.run(context, run);
  } finally {
    signal.removeEventListener("abort", cleanup);
    cleanup();
  }
}

export function getMetadataContext(): MetadataContext | undefined {
  const context = metadataContext.getStore();
  context?.signal.throwIfAborted();
  return context;
}

export function getMetadataSdkOptions(): {
  signal?: AbortSignal;
  timeoutMS?: number;
} {
  const context = getMetadataContext();
  return context
    ? {
        signal: context.signal,
        ...(Number.isFinite(context.deadline)
          ? { timeoutMS: Math.max(1, Math.ceil(context.deadline - Date.now())) }
          : {}),
      }
    : {};
}

/** Cancel only the native resource owned by this metadata request. */
export async function withMetadataResource<T>(
  cancel: () => void | Promise<void>,
  run: () => Promise<T>,
): Promise<T> {
  const context = getMetadataContext();
  if (!context) return run();
  const onAbort = () => {
    void Promise.resolve()
      .then(cancel)
      .catch(() => undefined);
  };
  context.signal.addEventListener("abort", onAbort, { once: true });
  try {
    const result = await run();
    context.signal.throwIfAborted();
    return result;
  } finally {
    context.signal.removeEventListener("abort", onAbort);
  }
}
