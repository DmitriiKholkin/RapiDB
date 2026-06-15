/**
 * Bounded-concurrency helpers built on plain `Promise.all`.
 *
 * Why this lives in its own module:
 *  - the helper is pure (no `vscode`, no I/O) and easy to unit test;
 *  - both variants share the same worker loop, so the logic is
 *    extracted once and the public functions decide only how failures
 *    are surfaced.
 *
 * Both helpers:
 *  - return an array of the same length and order as `items`;
 *  - run at most `limit` operations in parallel;
 *  - honor an `AbortSignal` checked at the top of every iteration;
 *  - share a single counter across workers, so the iteration order is
 *    deterministic up to scheduling.
 */

interface PoolOptions<T, R> {
  items: T[];
  limit: number;
  fn: (item: T) => Promise<R>;
  signal: AbortSignal | undefined;
  /**
   * Called for every failed item. May mutate `results[index]` to
   * record a per-item fallback (settled variant) or collect the error
   * for an aggregate throw (strict variant).
   */
  onError: (results: R[], index: number, error: unknown) => void;
}

async function runPool<T, R>({
  items,
  limit,
  fn,
  signal,
  onError,
}: PoolOptions<T, R>): Promise<R[]> {
  if (items.length === 0) {
    return [];
  }

  const results: R[] = new Array(items.length);
  // Shared across workers so the iteration order is deterministic.
  let nextIndex = 0;

  const workers = Array.from(
    { length: Math.min(limit, items.length) },
    async () => {
      while (true) {
        // Check for abort before processing the next item.
        if (signal?.aborted) {
          throw new DOMException("Operation aborted", "AbortError");
        }

        const currentIndex = nextIndex++;
        if (currentIndex >= items.length) {
          break;
        }

        try {
          results[currentIndex] = await fn(items[currentIndex]);
        } catch (error) {
          onError(results, currentIndex, error);
        }
      }
    },
  );

  await Promise.all(workers);
  return results;
}

function throwAggregateError(
  errors: Array<{ index: number; error: unknown }>,
): never {
  const firstError = errors[0];
  const cause =
    firstError.error instanceof Error
      ? firstError.error
      : new Error(String(firstError.error));
  // We deliberately do NOT mutate the original Error's `.message` —
  // that would leak the pMapWithLimit prefix into every other consumer
  // that later inspects the same error instance.
  const aggregate = new Error(
    `pMapWithLimit: ${errors.length} operation(s) failed. First error at index ${firstError.index}: ${cause.message}`,
    { cause },
  );
  aggregate.name = cause.name;
  throw aggregate;
}

/**
 * Maps over `items` with a concurrency cap, rejecting as soon as one
 * operation fails (after the in-flight workers complete).
 *
 * @throws Aggregate `Error` whose `.cause` is the first failure.
 */
export async function pMapWithLimit<T, R>(
  items: T[],
  limit: number,
  fn: (item: T) => Promise<R>,
  signal?: AbortSignal,
): Promise<R[]> {
  const errors: Array<{ index: number; error: unknown }> = [];
  const results = await runPool<T, R>({
    items,
    limit,
    fn,
    signal,
    onError: (_r, i, e) => {
      errors.push({ index: i, error: e });
    },
  });

  if (errors.length > 0) {
    throwAggregateError(errors);
  }

  return results;
}

/**
 * Maps over `items` with a concurrency cap, returning per-item
 * outcomes. Successful items appear as their result; failed items
 * appear as the `Error` they threw. Never rejects.
 */
export async function pMapWithLimitAllSettled<T, R>(
  items: T[],
  limit: number,
  fn: (item: T) => Promise<R>,
  signal?: AbortSignal,
): Promise<Array<R | Error>> {
  return runPool<T, R | Error>({
    items,
    limit,
    fn,
    signal,
    onError: (results, index, error) => {
      results[index] =
        error instanceof Error ? error : new Error(String(error));
    },
  });
}
