/**
 * Helpers for serializing arrays whose numeric elements are stored as raw
 * token strings (e.g. parsed PostgreSQL `numeric[]` results, where the
 * driver returns `["13000.0", "42.5"]` to preserve trailing zeros).
 *
 * The default `JSON.stringify` would quote those numbers, which is why we
 * serialize them ourselves: any string element that already looks like a
 * numeric literal is emitted verbatim, while everything else is encoded
 * with `JSON.stringify`.
 */
const RAW_NUMERIC_TOKEN_RE = /^-?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?$/;

export function serializeArrayPreservingRawTokens(
  value: readonly unknown[],
): string {
  const parts: string[] = [];
  for (const entry of value) {
    if (entry === null || entry === undefined) {
      parts.push("null");
      continue;
    }
    if (Array.isArray(entry)) {
      parts.push(serializeArrayPreservingRawTokens(entry));
      continue;
    }
    if (typeof entry === "string") {
      if (RAW_NUMERIC_TOKEN_RE.test(entry)) {
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
