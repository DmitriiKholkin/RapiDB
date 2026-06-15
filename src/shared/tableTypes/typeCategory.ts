/**
 * Type-category inference for arbitrary JS values.
 *
 * The inference chain is intentionally ordered from cheapest to most
 * expensive check: primitive type, then instanceof, then string
 * regex matchers, then JSON.parse as a last resort.
 */
import type { TypeCategory } from "./types";

/** UUIDv1..v8 — the version nibble is `1..8` and the variant nibble is `8/9/a/b`. */
const UUID_VALUE_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const INTEGER_VALUE_RE = /^[+-]?\d+$/;
const DECIMAL_VALUE_RE = /^[+-]?(?:\d+\.\d*|\d*\.\d+|\d+(?:[eE][+-]?\d+))$/;
const DATE_VALUE_RE = /^\d{4}-\d{2}-\d{2}$/;
const TIME_VALUE_RE = /^\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?$/;
const DATETIME_VALUE_RE =
  /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?: ?(?:Z|[+-]\d{2}:\d{2}))?$/i;
const HEX_BINARY_DIGITS_RE = /^[0-9a-f]+$/i;
/** OGC / PostGIS WKT prefix list. */
const SPATIAL_VALUE_RE =
  /^(?:srid=\d+;)?\s*(?:point|linestring|polygon|multipoint|multilinestring|multipolygon|geometrycollection|circularstring|compoundcurve|curvepolygon|multicurve|multisurface|polyhedralsurface|tin|triangle)\s*\(/i;

/** Hex prefixes accepted by `isPrefixedHexBinaryValue`. */
const HEX_PREFIXES = ["\\x", "\\X", "0x", "0X"] as const;
const HEX_PREFIX_LENGTH = 2;

/**
 * Normalizes a binary value string to a `0x...` prefix.
 * Strips alternative prefixes (`\x`, `0X`, etc.).
 */
export function normalizeBinaryHexDisplayPrefix(value: string): string {
  for (const prefix of HEX_PREFIXES) {
    if (value.startsWith(prefix)) {
      return `0x${value.slice(HEX_PREFIX_LENGTH)}`;
    }
  }
  return value;
}

function isPrefixedHexBinaryValue(value: string): boolean {
  for (const prefix of HEX_PREFIXES) {
    if (value.startsWith(prefix)) {
      return (
        value.length > HEX_PREFIX_LENGTH &&
        HEX_BINARY_DIGITS_RE.test(value.slice(HEX_PREFIX_LENGTH))
      );
    }
  }
  return false;
}

function isJsonLikeContainer(trimmed: string): boolean {
  return trimmed.startsWith("{") || trimmed.startsWith("[");
}

/** Parses a JSON-looking string and returns the category, or `null`. */
function inferJsonLikeCategory(trimmed: string): TypeCategory | null {
  try {
    const parsed = JSON.parse(trimmed) as unknown;
    return Array.isArray(parsed) ? "array" : "json";
  } catch {
    return null;
  }
}

/**
 * Infers the most specific `TypeCategory` for a value, or `null` when
 * the value is untyped/ambiguous.
 */
export function inferValueCategory(value: unknown): TypeCategory | null {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === "boolean") {
    return "boolean";
  }
  if (typeof value === "bigint") {
    return "integer";
  }
  if (typeof value === "number") {
    return Number.isInteger(value) ? "integer" : "float";
  }
  if (value instanceof Date) {
    return "datetime";
  }
  if (Array.isArray(value)) {
    return "array";
  }
  if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) {
    return "binary";
  }
  if (typeof value === "object") {
    return "json";
  }
  if (typeof value !== "string") {
    return null;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }

  if (isPrefixedHexBinaryValue(trimmed)) return "binary";
  if (UUID_VALUE_RE.test(trimmed)) return "uuid";
  if (SPATIAL_VALUE_RE.test(trimmed)) return "spatial";
  if (DATETIME_VALUE_RE.test(trimmed)) return "datetime";
  if (DATE_VALUE_RE.test(trimmed)) return "date";
  if (TIME_VALUE_RE.test(trimmed)) return "time";
  if (INTEGER_VALUE_RE.test(trimmed)) return "integer";
  if (DECIMAL_VALUE_RE.test(trimmed)) return "decimal";

  if (isJsonLikeContainer(trimmed)) {
    return inferJsonLikeCategory(trimmed);
  }

  return null;
}

/**
 * Returns the first non-null inferred category across `values`.
 * Returns `null` for an empty array or if no value matches.
 */
export function inferQueryColumnCategory(
  values: readonly unknown[],
): TypeCategory | null {
  for (const value of values) {
    const category = inferValueCategory(value);
    if (category) {
      return category;
    }
  }
  return null;
}
