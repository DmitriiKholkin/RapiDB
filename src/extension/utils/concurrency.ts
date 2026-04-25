/**
 * Runs `fn` over `items` with at most `limit` concurrent executions.
 * Results are returned in the same order as `items`.
 *
 * If an individual call to `fn` throws, that slot receives an empty-array
 * cast — callers that depend on specific types must handle this themselves.
 */
export async function pMapWithLimit<T, R>(
  items: T[],
  limit: number,
  fn: (item: T) => Promise<R>,
): Promise<R[]> {
  const results: R[] = new Array(items.length);
  const queue = items.map((item, i) => ({ item, i }));

  const workers = Array.from(
    { length: Math.min(limit, queue.length) },
    async () => {
      while (queue.length > 0) {
        const next = queue.shift();
        if (!next) break;
        results[next.i] = await fn(next.item).catch(() => [] as unknown as R);
      }
    },
  );

  await Promise.all(workers);
  return results;
}
