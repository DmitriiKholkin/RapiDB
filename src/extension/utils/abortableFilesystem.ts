export function runAbortableFilesystemOperation<T>(
  signal: AbortSignal,
  operation: () => Promise<T>,
  disposeLate?: (value: T) => Promise<void>,
): Promise<T> {
  if (signal.aborted)
    return Promise.reject(new DOMException("Aborted", "AbortError"));
  return new Promise<T>((resolve, reject) => {
    let finished = false;
    let aborted = false;
    const onAbort = () => {
      if (finished) return;
      finished = true;
      aborted = true;
      reject(new DOMException("Aborted", "AbortError"));
    };
    signal.addEventListener("abort", onAbort, { once: true });
    Promise.resolve()
      .then(() => {
        signal.throwIfAborted();
        return operation();
      })
      .then(
        (value) => {
          signal.removeEventListener("abort", onAbort);
          if (aborted) {
            if (disposeLate) void disposeLate(value).catch(() => undefined);
            return;
          }
          finished = true;
          resolve(value);
        },
        (error: unknown) => {
          signal.removeEventListener("abort", onAbort);
          if (!finished) {
            finished = true;
            reject(error);
          }
        },
      );
  });
}
