/**
 * Persisted-edit value canonicalizers and diagnostic helpers.
 * Extracted from BaseDBDriver for single-responsibility adherence.
 *
 * Each canonicalizer converts a user-supplied value into a normalized
 * string representation used for edit-verification comparison.
 */
import { NULL_SENTINEL } from "../../shared/tableTypes";
import { formatDatetimeForDisplay } from "../utils/dateUtils";
import { hexFromBuffer, isHexLike, parseHexToBuffer } from "./hexUtils";
import {
  canonicalizeExactNumeric,
  numberToDecimalString,
  parseDecimalString,
} from "./numericUtils";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Sentinel token used to represent SQL NULL in persisted-edit canonicalization. */
export const PERSISTED_EDIT_NULL_TOKEN =
  "\x00__RAPIDB_PERSISTED_EDIT_NULL__\x00";

const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface CanonicalPersistedEditValue {
  canonical: string;
}

export type PersistedEditCanonicalizer = (
  value: unknown,
) => CanonicalPersistedEditValue | null;

// ---------------------------------------------------------------------------
// Null handling (shared by every canonicalizer)
// ---------------------------------------------------------------------------

export function canonicalizeNullishPersistedEditValue(
  value: unknown,
): CanonicalPersistedEditValue | null {
  if (value === NULL_SENTINEL || value === null) {
    return { canonical: PERSISTED_EDIT_NULL_TOKEN };
  }
  return null;
}

// ---------------------------------------------------------------------------
// Type-specific canonicalizers
// ---------------------------------------------------------------------------

export function canonicalizeTextPersistedEditValue(
  value: unknown,
): CanonicalPersistedEditValue | null {
  const nullish = canonicalizeNullishPersistedEditValue(value);
  if (nullish) {
    return nullish;
  }
  if (typeof value === "string") {
    return { canonical: value };
  }
  if (
    typeof value === "number" ||
    typeof value === "boolean" ||
    typeof value === "bigint"
  ) {
    return { canonical: String(value) };
  }
  return null;
}

export function canonicalizeBooleanPersistedEditValue(
  value: unknown,
): CanonicalPersistedEditValue | null {
  const nullish = canonicalizeNullishPersistedEditValue(value);
  if (nullish) {
    return nullish;
  }
  if (value === true || value === 1) {
    return { canonical: "true" };
  }
  if (value === false || value === 0) {
    return { canonical: "false" };
  }
  if (typeof value !== "string") {
    return null;
  }
  const normalized = value.trim().toLowerCase();
  if (["true", "t", "1"].includes(normalized)) {
    return { canonical: "true" };
  }
  if (["false", "f", "0"].includes(normalized)) {
    return { canonical: "false" };
  }
  return null;
}

export function canonicalizeUuidPersistedEditValue(
  value: unknown,
): CanonicalPersistedEditValue | null {
  const nullish = canonicalizeNullishPersistedEditValue(value);
  if (nullish) {
    return nullish;
  }
  if (typeof value !== "string") {
    return null;
  }
  const normalized = value.trim().toLowerCase();
  if (!UUID_RE.test(normalized)) {
    return null;
  }
  return { canonical: normalized };
}

// ---------------------------------------------------------------------------
// JSON canonicalization (stable key ordering)
// ---------------------------------------------------------------------------

/** Recursively sort object keys for stable JSON serialization. */
export function stableJsonValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => stableJsonValue(item));
  }
  if (value !== null && typeof value === "object") {
    const proto = Object.getPrototypeOf(value);
    if (proto === Object.prototype || proto === null) {
      return Object.fromEntries(
        Object.keys(value)
          .sort()
          .map((key) => [
            key,
            stableJsonValue((value as Record<string, unknown>)[key]),
          ]),
      );
    }
  }
  return value;
}

export function canonicalizeJsonPersistedEditValue(
  value: unknown,
): CanonicalPersistedEditValue | null {
  const nullish = canonicalizeNullishPersistedEditValue(value);
  if (nullish) {
    return nullish;
  }
  let parsed = value;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed === "") {
      return null;
    }
    try {
      parsed = JSON.parse(trimmed) as unknown;
    } catch {
      return null;
    }
  }
  try {
    return { canonical: JSON.stringify(stableJsonValue(parsed)) };
  } catch {
    return null;
  }
}

export function canonicalizeJsonArrayPersistedEditValue(
  value: unknown,
): CanonicalPersistedEditValue | null {
  const nullish = canonicalizeNullishPersistedEditValue(value);
  if (nullish) {
    return nullish;
  }
  let parsed = value;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed === "") {
      return null;
    }
    try {
      parsed = JSON.parse(trimmed) as unknown;
    } catch {
      return null;
    }
  }
  if (!Array.isArray(parsed)) {
    return null;
  }
  try {
    return { canonical: JSON.stringify(stableJsonValue(parsed)) };
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Binary / hex canonicalization
// ---------------------------------------------------------------------------

function toPersistedEditBuffer(value: unknown): Buffer | null {
  if (Buffer.isBuffer(value)) {
    return value;
  }
  if (value instanceof Uint8Array) {
    return Buffer.from(value);
  }
  if (value instanceof ArrayBuffer) {
    return Buffer.from(new Uint8Array(value));
  }
  return null;
}

export function canonicalizeBinaryPersistedEditValue(
  value: unknown,
): CanonicalPersistedEditValue | null {
  const nullish = canonicalizeNullishPersistedEditValue(value);
  if (nullish) {
    return nullish;
  }
  const buffer = toPersistedEditBuffer(value);
  if (buffer) {
    return { canonical: hexFromBuffer(buffer).toLowerCase() };
  }
  if (typeof value !== "string") {
    return null;
  }
  if (value === "") {
    return { canonical: "" };
  }
  if (!isHexLike(value)) {
    return null;
  }
  return {
    canonical: hexFromBuffer(parseHexToBuffer(value)).toLowerCase(),
  };
}

// ---------------------------------------------------------------------------
// Numeric canonicalization
// ---------------------------------------------------------------------------

export function canonicalizeApproximateNumericPersistedEditValue(
  value: unknown,
  significantDigits: number,
): CanonicalPersistedEditValue | null {
  const nullish = canonicalizeNullishPersistedEditValue(value);
  if (nullish) {
    return nullish;
  }
  const raw = parseDecimalString(value);
  if (raw === null) {
    return null;
  }
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return {
    canonical: Number.parseFloat(
      parsed.toPrecision(significantDigits),
    ).toString(),
  };
}

/**
 * Detect when an approximate-numeric value would suffer precision loss
 * at the given significant-digits limit. Returns the rounded value or null.
 */
export function findApproximateNumericPrecisionLoss(
  value: unknown,
  significantDigits: number,
): {
  roundedValue: string;
} | null {
  if (typeof value !== "string") {
    return null;
  }
  const raw = parseDecimalString(value);
  if (raw === null) {
    return null;
  }
  const requested = canonicalizeExactNumeric(raw, null);
  if (!requested) {
    return null;
  }
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  const roundedNumber = Number.parseFloat(
    parsed.toPrecision(significantDigits),
  );
  const rounded = canonicalizeExactNumeric(
    numberToDecimalString(roundedNumber),
    null,
  );
  if (!rounded || requested.canonical === rounded.canonical) {
    return null;
  }
  return {
    roundedValue: rounded.canonical,
  };
}

// ---------------------------------------------------------------------------
// Diagnostic helpers
// ---------------------------------------------------------------------------

/** Format a value for human-readable diagnostic messages. */
export function formatDiagnosticValue(value: unknown): string {
  if (value === null) return "NULL";
  if (value === undefined) return "<missing>";
  if (typeof value === "string") return JSON.stringify(value);
  if (value instanceof Date) {
    const formatted = formatDatetimeForDisplay(value) ?? value.toISOString();
    return JSON.stringify(formatted);
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

export interface ExactNumericConstraint {
  precision: number | null;
  scale: number | null;
}

/**
 * Build a human-readable validation message when an exact-numeric value
 * violates its column precision/scale constraint.
 */
export function buildValidationMessage(
  columnName: string,
  constraint: ExactNumericConstraint,
  canonical: {
    scaleOverflow: boolean;
    integerDigits: number;
    fractionDigits: number;
    canonical: string;
  },
): string | null {
  if (canonical.scaleOverflow && constraint.scale !== null) {
    return `Column "${columnName}" accepts at most ${constraint.scale} fractional digit${constraint.scale === 1 ? "" : "s"}.`;
  }
  if (constraint.precision !== null) {
    if (constraint.scale !== null) {
      const allowedIntegerDigits = Math.max(
        constraint.precision - constraint.scale,
        0,
      );
      if (canonical.integerDigits > allowedIntegerDigits) {
        return `Column "${columnName}" exceeds precision ${constraint.precision} with scale ${constraint.scale}.`;
      }
    } else if (
      canonical.integerDigits + canonical.fractionDigits >
      constraint.precision
    ) {
      return `Column "${columnName}" exceeds precision ${constraint.precision}.`;
    }
  }
  return null;
}
