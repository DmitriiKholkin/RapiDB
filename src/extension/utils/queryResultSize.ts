// Count UTF-8 JSON bytes without allocating an escaped copy. For JSON stored
// *as a string* in the result grid, account for the second layer of escaping.
export function jsonStringByteSize(
  value: string,
  limit: number,
  embedded = false,
): number {
  if (value.length + 2 > limit) return Infinity;
  let bytes = embedded ? 6 : 2;
  for (let i = 0; i < value.length; i++) {
    const ch = value.charCodeAt(i);
    if (ch === 0x22 || ch === 0x5c) bytes += embedded ? 4 : 2;
    else if (ch < 0x20) {
      const shortEscape =
        ch === 8 || ch === 9 || ch === 10 || ch === 12 || ch === 13;
      bytes += (shortEscape ? 2 : 6) + (embedded ? 1 : 0);
    } else if (ch < 0x80) bytes++;
    else if (ch < 0x800) bytes += 2;
    else if (ch >= 0xd800 && ch <= 0xdfff) {
      const next = value.charCodeAt(i + 1);
      if (ch <= 0xdbff && next >= 0xdc00 && next <= 0xdfff) {
        bytes += 4;
        i++;
      } else bytes += embedded ? 7 : 6;
    } else bytes += 3;
    if (bytes > limit) return Infinity;
  }
  return bytes <= limit ? bytes : Infinity;
}

// The PostgreSQL webview DTO consists of ordinary arrays/records and scalar
// display values. Bound the whole envelope, not just the row arrays, without
// allocating another full JSON string. Undefined object fields and numeric
// values are deliberately overcounted. Reject unsupported shapes/depth.
export function queryResultJsonFits(value: unknown, limit: number): boolean {
  let remaining = limit;
  const charge = (bytes: number) => {
    remaining -= bytes;
    return remaining >= 0;
  };
  function visit(entry: unknown, depth: number): boolean {
    if (depth > 32) return false;
    if (entry === undefined || entry === null) return charge(4);
    if (typeof entry === "string")
      return charge(jsonStringByteSize(entry, remaining));
    if (typeof entry === "number") return charge(25);
    if (typeof entry === "boolean") return charge(5);
    if (typeof entry !== "object") return false;
    // An invalid Date can survive normalization; it is JSON null.
    if (entry instanceof Date) return charge(32);
    if (!charge(2)) return false;
    if (Array.isArray(entry)) {
      for (const item of entry) {
        if (!charge(1) || !visit(item, depth + 1)) return false;
      }
    } else {
      const prototype: unknown = Object.getPrototypeOf(entry);
      if (prototype !== Object.prototype && prototype !== null) return false;
      for (const key in entry) {
        if (
          Object.hasOwn(entry, key) &&
          (!charge(jsonStringByteSize(key, remaining) + 2) ||
            !visit((entry as Record<string, unknown>)[key], depth + 1))
        )
          return false;
      }
    }
    return true;
  }
  return visit(value, 0);
}
export const POSTGRES_QUERY_RESULT_IPC_BYTES = 8 * 1024 * 1024;
