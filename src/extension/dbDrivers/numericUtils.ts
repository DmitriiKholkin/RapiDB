/**
 * Numeric utility functions for display normalization, decimal parsing,
 * and exact-numeric constraint validation.
 * Extracted from BaseDBDriver for single-responsibility adherence.
 */
import { NULL_SENTINEL } from "../../shared/tableTypes";

/** Normalize a number for display — non-finite values become their string representation. */
export function normalizeNumericDisplayValue(value: number): number | string {
  return Number.isFinite(value) ? value : String(value);
}

export interface ExactNumericConstraint {
  precision: number | null;
  scale: number | null;
}

export interface CanonicalExactNumericValue {
  canonical: string;
  integerDigits: number;
  fractionDigits: number;
  scaleOverflow: boolean;
}

/** Parse precision/scale from a native type string like DECIMAL(10,2). */
export function parseTypePrecisionScale(
  nativeType: string,
): ExactNumericConstraint {
  const match = /\((\d+)(?:\s*,\s*(-?\d+))?\)/.exec(nativeType);
  if (!match) {
    return { precision: null, scale: null };
  }
  return {
    precision: Number.parseInt(match[1], 10),
    scale: match[2] === undefined ? null : Number.parseInt(match[2], 10),
  };
}

/** Convert a number to a decimal string, expanding scientific notation if needed. */
export function numberToDecimalString(value: number): string {
  const raw = value.toString();
  if (!/[eE]/.test(raw)) {
    return raw;
  }
  const [mantissa, exponentText] = raw.toLowerCase().split("e");
  const exponent = Number.parseInt(exponentText, 10);
  const sign = mantissa.startsWith("-") ? "-" : "";
  const unsignedMantissa = mantissa.replace(/^[+-]/, "");
  const [integerPart, fractionPart = ""] = unsignedMantissa.split(".");
  const digits = `${integerPart}${fractionPart}`;
  const decimalIndex = integerPart.length + exponent;
  if (decimalIndex <= 0) {
    return `${sign}0.${"0".repeat(Math.abs(decimalIndex))}${digits}`;
  }
  if (decimalIndex >= digits.length) {
    return `${sign}${digits}${"0".repeat(decimalIndex - digits.length)}`;
  }
  return `${sign}${digits.slice(0, decimalIndex)}.${digits.slice(decimalIndex)}`;
}

/** Parse a value into a normalized decimal string, or null for NULL/empty/invalid. */
export function parseDecimalString(value: unknown): string | null {
  if (value === null || value === undefined || value === NULL_SENTINEL) {
    return null;
  }
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? numberToDecimalString(value) : null;
  }
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed === "" ? null : trimmed;
}

/**
 * Canonicalize a numeric value against an optional scale constraint.
 * Returns the canonical string, digit counts, and whether scale was exceeded.
 */
export function canonicalizeExactNumeric(
  value: unknown,
  scale: number | null,
): CanonicalExactNumericValue | null {
  const raw = parseDecimalString(value);
  if (raw === null) {
    return null;
  }
  const match = /^([+-])?(?:(\d+)(?:\.(\d*))?|\.(\d+))$/.exec(raw);
  if (!match) {
    return null;
  }
  const sign = match[1] === "-" ? "-" : "";
  const integerPart = match[2] ?? "0";
  const fractionPart = match[3] ?? match[4] ?? "";
  const normalizedInteger = integerPart.replace(/^0+(?=\d)/, "");
  if (scale !== null) {
    if (scale < 0) {
      return null;
    }
    const overflowDigits = fractionPart.slice(scale);
    const scaleOverflow = /[1-9]/.test(overflowDigits);
    const normalizedFraction = fractionPart.slice(0, scale).padEnd(scale, "0");
    const isZero =
      normalizedInteger.replace(/^0+/, "") === "" &&
      /^0*$/.test(normalizedFraction);
    const integerDigits =
      normalizedInteger.replace(/^0+/, "") === ""
        ? 1
        : normalizedInteger.replace(/^0+/, "").length;
    return {
      canonical:
        scale === 0
          ? isZero
            ? "0"
            : `${sign}${normalizedInteger.replace(/^0+/, "") || "0"}`
          : `${isZero ? "" : sign}${normalizedInteger.replace(/^0+/, "") || "0"}.${normalizedFraction}`,
      integerDigits,
      fractionDigits: normalizedFraction.length,
      scaleOverflow,
    };
  }
  const trimmedFraction = fractionPart.replace(/0+$/, "");
  const normalizedInt = normalizedInteger.replace(/^0+/, "") || "0";
  const isZero = normalizedInt === "0" && trimmedFraction === "";
  return {
    canonical:
      `${isZero ? "" : sign}${normalizedInt}${trimmedFraction ? `.${trimmedFraction}` : ""}` ||
      "0",
    integerDigits: normalizedInt === "0" ? 1 : normalizedInt.length,
    fractionDigits: trimmedFraction.length,
    scaleOverflow: false,
  };
}
