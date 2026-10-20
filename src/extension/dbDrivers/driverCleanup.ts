const DRIVER_CLOSE_BUDGET_MS = 1_000;

export async function closeDriverResource(
  close: () => Promise<void>,
  discard: () => void,
): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let closed = false;
  try {
    closed = await Promise.race([
      Promise.resolve()
        .then(close)
        .then(() => true),
      new Promise<false>((resolve) => {
        timer = setTimeout(() => resolve(false), DRIVER_CLOSE_BUDGET_MS);
      }),
    ]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
    if (!closed) discard();
  }
}
