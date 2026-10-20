interface PoolOptions<T, R> {
  items: T[];
  limit: number;
  fn: (item: T) => Promise<R>;
  signal: AbortSignal | undefined;
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
  let nextIndex = 0;

  const workers = Array.from(
    { length: Math.min(limit, items.length) },
    async () => {
      while (true) {
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
  const aggregate = new Error(
    `pMapWithLimit: ${errors.length} operation(s) failed. First error at index ${firstError.index}: ${cause.message}`,
    { cause },
  );
  aggregate.name = cause.name;
  throw aggregate;
}

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
