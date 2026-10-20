const RAW_NUMERIC_TOKEN_RE = /^-?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?$/;

export function serializeArrayPreservingRawTokens(
  value: readonly unknown[],
  numericStringTokens = true,
): string {
  const parts: string[] = [];
  for (const entry of value) {
    if (entry === null || entry === undefined) {
      parts.push("null");
      continue;
    }
    if (Array.isArray(entry)) {
      parts.push(serializeArrayPreservingRawTokens(entry, numericStringTokens));
      continue;
    }
    if (typeof entry === "string") {
      if (numericStringTokens && RAW_NUMERIC_TOKEN_RE.test(entry)) {
        parts.push(entry);
        continue;
      }
      parts.push(JSON.stringify(entry));
      continue;
    }
    if (typeof entry === "number" || typeof entry === "boolean") {
      parts.push(JSON.stringify(entry));
      continue;
    }
    parts.push(JSON.stringify(entry));
  }
  return `[${parts.join(",")}]`;
}
