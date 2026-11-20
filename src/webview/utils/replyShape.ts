export function isArrayOf<T>(
  value: unknown,
  isItem: (item: unknown) => item is T,
): value is T[] {
  if (!Array.isArray(value)) return false;
  for (const item of value) {
    if (!isItem(item)) return false;
  }
  return true;
}

export function isString(value: unknown): value is string {
  return typeof value === "string";
}
