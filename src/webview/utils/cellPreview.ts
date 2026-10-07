import type { TypeCategory } from "../../shared/tableTypes";
import { formatBinaryValueForViewer } from "./valueFormatting";

// Grid-only presentation budget. Never use these previews for data operations.
export const CELL_PREVIEW_LIMIT = 1024;
export const CELL_PREVIEW_NOTICE =
  "Preview only; open the cell for the full value.";
const MAX_DEPTH = 16;
const CIRCULAR_VALUE = new Error("Circular cell value");

function prefix(text: string, length: number): string {
  let end = Math.min(text.length, length);
  if (
    end > 0 &&
    end < text.length &&
    text.charCodeAt(end - 1) >= 0xd800 &&
    text.charCodeAt(end - 1) <= 0xdbff &&
    text.charCodeAt(end) >= 0xdc00 &&
    text.charCodeAt(end) <= 0xdfff
  ) {
    end--;
  }
  return text.slice(0, end);
}

export function getCellPreview(
  value: unknown,
  category?: TypeCategory,
): { text: string; truncated: boolean } {
  let text = "";
  let truncated = false;
  let visited = 0;
  const ancestors = new Set<object>();

  function append(part: string): void {
    const remaining = CELL_PREVIEW_LIMIT - text.length;
    text += prefix(part, remaining);
    if (part.length > remaining) truncated = true;
  }

  function quoted(part: string): void {
    // Bound the input before JSON escaping, not just the resulting text.
    const bounded = prefix(part, CELL_PREVIEW_LIMIT - text.length);
    append(JSON.stringify(bounded));
    if (bounded.length < part.length) truncated = true;
  }

  function write(current: unknown, depth: number): void {
    if (truncated) return;
    if (++visited > CELL_PREVIEW_LIMIT || depth > MAX_DEPTH) {
      truncated = true;
      return;
    }
    if (current === null || current === undefined) {
      append("null");
    } else if (typeof current === "string") {
      quoted(current);
    } else if (current instanceof Date) {
      append(JSON.stringify(current));
    } else if (typeof current !== "object") {
      append(JSON.stringify(current) ?? "null");
    } else {
      if (ancestors.has(current)) {
        throw CIRCULAR_VALUE;
      }
      ancestors.add(current);
      if (Array.isArray(current)) {
        append("[");
        for (let i = 0; i < current.length && !truncated; i++) {
          if (i) append(",");
          write(current[i], depth + 1);
        }
        if (!truncated) append("]");
      } else if (
        ArrayBuffer.isView(current) &&
        !(current instanceof DataView)
      ) {
        // Do not call Buffer.toJSON(): it copies the entire byte array.
        const bytes = current as unknown as ArrayLike<number>;
        const isBuffer = current.constructor.name === "Buffer";
        append(isBuffer ? '{"type":"Buffer","data":[' : "{");
        for (let i = 0; i < bytes.length && !truncated; i++) {
          if (i) append(",");
          if (!isBuffer) append(`"${i}":`);
          write(bytes[i], depth + 1);
        }
        if (!truncated) append(isBuffer ? "]}" : "}");
      } else {
        append("{");
        let first = true;
        for (const key in current) {
          if (truncated) break;
          if (++visited > CELL_PREVIEW_LIMIT) {
            truncated = true;
            break;
          }
          // Stable ordinary objects enumerate own keys before prototype keys.
          // JSON ignores inherited keys, so stop rather than scanning them all.
          if (!Object.hasOwn(current, key)) break;
          const item = (current as Record<string, unknown>)[key];
          if (
            item === undefined ||
            typeof item === "function" ||
            typeof item === "symbol"
          )
            continue;
          if (!first) append(",");
          first = false;
          quoted(key);
          append(":");
          write(item, depth + 1);
        }
        if (!truncated) append("}");
      }
      ancestors.delete(current);
    }
  }

  function writeFallback(current: unknown, depth: number): void {
    if (truncated) return;
    if (++visited > CELL_PREVIEW_LIMIT || depth > MAX_DEPTH) {
      truncated = true;
      return;
    }
    if (current === null || current === undefined) return;
    if (Array.isArray(current)) {
      // Match ordinary Array#toString, including its empty circular entries,
      // without joining or coercing the entire array first.
      if (ancestors.has(current)) return;
      ancestors.add(current);
      for (let i = 0; i < current.length && !truncated; i++) {
        if (i) append(",");
        writeFallback(current[i], depth + 1);
      }
      ancestors.delete(current);
    } else if (current instanceof Date) {
      append(Date.prototype.toString.call(current));
    } else if (typeof current === "object") {
      // Default object coercion is constant; never invoke custom toString.
      append("[object Object]");
    } else {
      append(String(current));
    }
  }

  if (value !== null && value !== undefined) {
    if (typeof value === "string") {
      append(value);
    } else if (value instanceof Date) {
      append(
        Number.isNaN(value.getTime()) ? String(value) : value.toISOString(),
      );
    } else if (typeof value === "object") {
      try {
        write(value, 0);
      } catch (error) {
        if (error === CIRCULAR_VALUE) {
          text = "";
          truncated = false;
          visited = 0;
          ancestors.clear();
          writeFallback(value, 0);
        } else {
          // Unexpected objects are presentation-only; avoid unbounded coercion.
          text = "[Object]";
          truncated = true;
        }
      }
    } else {
      append(String(value));
    }
  }

  if (category === "binary" && !truncated) {
    text = formatBinaryValueForViewer(text);
  }
  if (truncated) text = `${prefix(text, CELL_PREVIEW_LIMIT - 1)}…`;
  return { text, truncated };
}

export function getCellPreviewTitle(value: unknown): string {
  const preview = getCellPreview(value);
  return preview.truncated
    ? `${preview.text}\n${CELL_PREVIEW_NOTICE}`
    : preview.text;
}
