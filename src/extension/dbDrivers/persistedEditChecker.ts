import { formatDatetimeForDisplay } from "../utils/dateUtils";
import {
  canonicalizeJsonPreservingRawNumbers,
  parseJsonPreservingRawNumbers,
  serializeCanonicalJson,
} from "../utils/jsonCanonical";
import type { ColumnTypeMeta, ValueSemantics } from "./types";

/**
 * Options for persisted edit checks.
 */
export interface PersistedEditCheckOptions {
  persistedValue: unknown;
}

/**
 * Result of a persisted edit check.
 */
export interface PersistedEditCheckResult {
  ok: boolean;
  shouldVerify: boolean;
  message?: string;
}

/**
 * Canonicalizer function for persisted edit values.
 */
export type PersistedEditCanonicalizer = (
  value: unknown,
) => { canonical: string } | null;

// Sentinel for null values in persisted edits
const PERSISTED_EDIT_NULL_TOKEN = "\x00__RAPIDB_PERSISTED_EDIT_NULL__\x00";

// UUID regex pattern
const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

/**
 * Interface for exact numeric constraints.
 */
interface ExactNumericConstraint {
  precision: number | null;
  scale: number | null;
}

/**
 * Checks and validates persisted edit values against expected values.
 * Encapsulates all logic for verifying that database edits were persisted correctly.
 */
export class PersistedEditChecker {
  /**
   * Checks if a value is nullish in the context of persisted edits.
   */
  canonicalizeNullishPersistedEditValue(
    value: unknown,
  ): { canonical: string } | null {
    if (value === null || value === undefined) {
      return { canonical: PERSISTED_EDIT_NULL_TOKEN };
    }
    if (typeof value === "string" && value === PERSISTED_EDIT_NULL_TOKEN) {
      return { canonical: PERSISTED_EDIT_NULL_TOKEN };
    }
    return null;
  }

  /**
   * Canonicalizes an exact numeric value for comparison.
   */
  canonicalizeExactNumeric(
    value: unknown,
    scale: number | null,
  ): { canonical: string } | null {
    if (value === null || value === undefined) {
      return null;
    }
    if (typeof value === "number") {
      const str = scale !== null ? value.toFixed(scale) : String(value);
      return { canonical: str };
    }
    if (typeof value === "string") {
      const num = Number(value);
      if (!Number.isFinite(num)) {
        return null;
      }
      const str = scale !== null ? num.toFixed(scale) : String(num);
      return { canonical: str };
    }
    if (typeof value === "bigint") {
      const str =
        scale !== null ? `${value}.${"0".repeat(scale)}` : String(value);
      return { canonical: str };
    }
    return null;
  }

  /**
   * Canonicalizes a text value for comparison.
   */
  canonicalizeTextPersistedEditValue(
    value: unknown,
  ): { canonical: string } | null {
    const nullish = this.canonicalizeNullishPersistedEditValue(value);
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

  /**
   * Canonicalizes a boolean value for comparison.
   */
  canonicalizeBooleanPersistedEditValue(
    value: unknown,
  ): { canonical: string } | null {
    const nullish = this.canonicalizeNullishPersistedEditValue(value);
    if (nullish) {
      return nullish;
    }
    if (typeof value === "boolean") {
      return { canonical: value ? "true" : "false" };
    }
    if (typeof value === "number") {
      if (value === 0) return { canonical: "false" };
      if (value === 1) return { canonical: "true" };
    }
    if (typeof value === "string") {
      const lower = value.trim().toLowerCase();
      if (lower === "true" || lower === "1") return { canonical: "true" };
      if (lower === "false" || lower === "0") return { canonical: "false" };
    }
    return null;
  }

  /**
   * Canonicalizes a UUID value for comparison.
   */
  canonicalizeUuidPersistedEditValue(
    value: unknown,
  ): { canonical: string } | null {
    const nullish = this.canonicalizeNullishPersistedEditValue(value);
    if (nullish) {
      return nullish;
    }
    if (typeof value === "string") {
      const lower = value.trim().toLowerCase();
      if (UUID_RE.test(lower)) {
        return { canonical: lower };
      }
    }
    return null;
  }

  /**
   * Canonicalizes a JSON value for comparison.
   */
  canonicalizeJsonPersistedEditValue(
    value: unknown,
  ): { canonical: string } | null {
    const nullish = this.canonicalizeNullishPersistedEditValue(value);
    if (nullish) {
      return nullish;
    }
    if (typeof value === "string") {
      const trimmed = value.trim();
      if (trimmed === "") {
        return null;
      }
      const canonical = canonicalizeJsonPreservingRawNumbers(trimmed);
      if (canonical === null) {
        try {
          return { canonical: JSON.stringify(JSON.parse(trimmed)) };
        } catch {
          return null;
        }
      }
      return { canonical };
    }
    if (typeof value === "number" || typeof value === "boolean") {
      return { canonical: JSON.stringify(value) };
    }
    if (value !== null && typeof value === "object") {
      try {
        return { canonical: JSON.stringify(value) };
      } catch {
        return null;
      }
    }
    return null;
  }

  /**
   * Canonicalizes a JSON array value for comparison.
   */
  canonicalizeJsonArrayPersistedEditValue(
    value: unknown,
  ): { canonical: string } | null {
    const nullish = this.canonicalizeNullishPersistedEditValue(value);
    if (nullish) {
      return nullish;
    }
    if (typeof value === "string") {
      const trimmed = value.trim();
      if (trimmed === "") {
        return null;
      }
      const parsed = parseJsonPreservingRawNumbers(trimmed);
      if (parsed === undefined) {
        return null;
      }
      if (!Array.isArray(parsed)) {
        return null;
      }
      return { canonical: serializeCanonicalJson(parsed) };
    }
    if (Array.isArray(value)) {
      try {
        return { canonical: JSON.stringify(value) };
      } catch {
        return null;
      }
    }
    return null;
  }

  /**
   * Canonicalizes a binary value for comparison.
   */
  canonicalizeBinaryPersistedEditValue(
    value: unknown,
  ): { canonical: string } | null {
    const nullish = this.canonicalizeNullishPersistedEditValue(value);
    if (nullish) {
      return nullish;
    }
    if (typeof value === "string") {
      if (value.startsWith("0x") || value.startsWith("0X")) {
        const hex = value.slice(2);
        if (/^[0-9a-fA-F]*$/.test(hex) && hex.length % 2 === 0) {
          return { canonical: hex.toLowerCase() };
        }
      }
      if (/^[0-9a-fA-F]+$/.test(value) && value.length % 2 === 0) {
        return { canonical: value.toLowerCase() };
      }
    }
    if (Buffer.isBuffer(value)) {
      return { canonical: value.toString("hex") };
    }
    return null;
  }

  /**
   * Canonicalizes an approximate numeric value for comparison.
   */
  canonicalizeApproximateNumericPersistedEditValue(
    value: unknown,
    significantDigits: number,
  ): { canonical: string } | null {
    const nullish = this.canonicalizeNullishPersistedEditValue(value);
    if (nullish) {
      return nullish;
    }
    if (typeof value === "number") {
      const str = value.toPrecision(significantDigits);
      return { canonical: str };
    }
    if (typeof value === "string") {
      const num = Number(value);
      if (!Number.isFinite(num)) {
        return null;
      }
      const str = num.toPrecision(significantDigits);
      return { canonical: str };
    }
    return null;
  }

  /**
   * Finds precision loss in approximate numeric values.
   */
  findApproximateNumericPrecisionLoss(
    value: unknown,
    significantDigits: number,
  ): { roundedValue: string } | null {
    if (typeof value === "number") {
      const str = value.toPrecision(significantDigits);
      const num = Number(str);
      if (num !== value) {
        return { roundedValue: str };
      }
    }
    if (typeof value === "string") {
      const num = Number(value);
      if (Number.isFinite(num)) {
        const str = num.toPrecision(significantDigits);
        const rounded = Number(str);
        if (rounded !== num) {
          return { roundedValue: str };
        }
      }
    }
    return null;
  }

  /**
   * Parses exact numeric constraint from native type string.
   */
  parseExactNumericConstraint(nativeType: string): ExactNumericConstraint {
    const match = /decimal\((\d+)(?:,\s*(\d+))?\)/i.exec(nativeType);
    if (!match) {
      return { precision: null, scale: null };
    }
    return {
      precision: Number(match[1]),
      scale: match[2] !== undefined ? Number(match[2]) : null,
    };
  }

  /**
   * Formats a value for diagnostic messages.
   */
  formatDiagnosticValue(value: unknown): string {
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
    return String(value);
  }

  /**
   * Builds a validation message for exact numeric constraints.
   */
  buildValidationMessage(
    columnName: string,
    constraint: ExactNumericConstraint,
    _expected: { canonical: string },
  ): string | null {
    if (constraint.scale !== null && constraint.scale > 0) {
      return `Column "${columnName}" expects a decimal number with at most ${constraint.precision} digits and ${constraint.scale} decimal places.`;
    }
    if (constraint.precision !== null) {
      return `Column "${columnName}" expects an integer with at most ${constraint.precision} digits.`;
    }
    return null;
  }

  /**
   * Checks if a column has boolean semantics.
   */
  hasBooleanSemantics(column: ColumnTypeMeta): boolean {
    return (
      column.valueSemantics === ("boolean" as ValueSemantics) ||
      column.category === "boolean"
    );
  }

  /**
   * Checks if a column is a datetime type with time component.
   */
  isDatetimeWithTime(nativeType: string): boolean {
    const lower = nativeType.toLowerCase();
    return (
      lower.includes("timestamp") ||
      lower.includes("datetime") ||
      lower.includes("time")
    );
  }

  /**
   * Checks if a value looks like a date input.
   */
  looksLikeDateInput(value: string): boolean {
    return /^\d{4}-\d{2}-\d{2}(?:[ T].*)?$/.test(value.trim());
  }

  /**
   * Generic check for normalized persisted edit values.
   */
  checkNormalizedPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    options: PersistedEditCheckOptions | undefined,
    canonicalize: PersistedEditCanonicalizer,
    invalidMessage?: string,
  ): PersistedEditCheckResult | null {
    const expected = canonicalize(expectedValue);
    if (!expected) {
      return invalidMessage
        ? {
            ok: false,
            shouldVerify: false,
            message: invalidMessage,
          }
        : null;
    }
    if (options === undefined) {
      return {
        ok: true,
        shouldVerify: true,
      };
    }
    const actual = canonicalize(options.persistedValue);
    if (!actual || actual.canonical !== expected.canonical) {
      return {
        ok: false,
        shouldVerify: true,
        message: `${column.name} stored ${this.formatDiagnosticValue(options.persistedValue)} instead of ${this.formatDiagnosticValue(expectedValue)}`,
      };
    }
    return {
      ok: true,
      shouldVerify: true,
    };
  }

  /**
   * Checks text persisted edit values.
   */
  checkTextPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null {
    return this.checkNormalizedPersistedEdit(
      column,
      expectedValue,
      options,
      (value) => {
        const nullish = this.canonicalizeNullishPersistedEditValue(value);
        if (nullish) {
          return nullish;
        }
        if (typeof value === "string") {
          return { canonical: value.trimEnd() };
        }
        if (
          typeof value === "number" ||
          typeof value === "boolean" ||
          typeof value === "bigint"
        ) {
          return { canonical: String(value).trimEnd() };
        }
        return null;
      },
      `Column "${column.name}" expects a text value.`,
    );
  }

  /**
   * Checks fixed-width char persisted edit values.
   */
  checkFixedWidthCharPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null {
    return this.checkNormalizedPersistedEdit(
      column,
      expectedValue,
      options,
      (value) => {
        const nullish = this.canonicalizeNullishPersistedEditValue(value);
        if (nullish) {
          return nullish;
        }
        if (typeof value === "string") {
          return { canonical: value.trimEnd() };
        }
        if (
          typeof value === "number" ||
          typeof value === "boolean" ||
          typeof value === "bigint"
        ) {
          return { canonical: String(value).trimEnd() };
        }
        return null;
      },
      `Column "${column.name}" expects a text value.`,
    );
  }

  /**
   * Checks boolean persisted edit values.
   */
  checkBooleanPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null {
    return this.checkNormalizedPersistedEdit(
      column,
      expectedValue,
      options,
      (value) => this.canonicalizeBooleanPersistedEditValue(value),
      `Column "${column.name}" expects true or false.`,
    );
  }

  /**
   * Checks UUID persisted edit values.
   */
  checkUuidPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null {
    return this.checkNormalizedPersistedEdit(
      column,
      expectedValue,
      options,
      (value) => this.canonicalizeUuidPersistedEditValue(value),
      `Column "${column.name}" expects a valid UUID.`,
    );
  }

  /**
   * Checks JSON persisted edit values.
   */
  checkJsonPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null {
    return this.checkNormalizedPersistedEdit(
      column,
      expectedValue,
      options,
      (value) => this.canonicalizeJsonPersistedEditValue(value),
      `Column "${column.name}" expects valid JSON.`,
    );
  }

  /**
   * Checks JSON array persisted edit values.
   */
  checkJsonArrayPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null {
    return this.checkNormalizedPersistedEdit(
      column,
      expectedValue,
      options,
      (value) => this.canonicalizeJsonArrayPersistedEditValue(value),
      `Column "${column.name}" expects a JSON array value.`,
    );
  }

  /**
   * Checks binary persisted edit values.
   */
  checkBinaryPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null {
    return this.checkNormalizedPersistedEdit(
      column,
      expectedValue,
      options,
      (value) => this.canonicalizeBinaryPersistedEditValue(value),
      `Column "${column.name}" expects a hex value like 0xDEADBEEF.`,
    );
  }

  /**
   * Checks approximate numeric persisted edit values.
   */
  checkApproximateNumericPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    significantDigits: number,
    options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null {
    if (options === undefined) {
      const precisionLoss = this.findApproximateNumericPrecisionLoss(
        expectedValue,
        significantDigits,
      );
      if (precisionLoss) {
        return {
          ok: false,
          shouldVerify: false,
          message:
            `Column "${column.name}" exceeds the reliable precision of this approximate numeric type ` +
            `(${significantDigits} significant digits) and would round to ${precisionLoss.roundedValue}.`,
        };
      }
    }
    return this.checkNormalizedPersistedEdit(
      column,
      expectedValue,
      options,
      (value) =>
        this.canonicalizeApproximateNumericPersistedEditValue(
          value,
          significantDigits,
        ),
      `Column "${column.name}" expects a numeric value.`,
    );
  }

  /**
   * Checks exact numeric persisted edit values.
   */
  checkExactNumericPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    constraint: ExactNumericConstraint | null,
    options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null {
    if (!constraint) {
      return null;
    }
    const expectedNullish =
      this.canonicalizeNullishPersistedEditValue(expectedValue);
    if (expectedNullish) {
      if (options === undefined) {
        return {
          ok: true,
          shouldVerify: true,
        };
      }
      const actualNullish = this.canonicalizeNullishPersistedEditValue(
        options.persistedValue,
      );
      if (
        !actualNullish ||
        actualNullish.canonical !== expectedNullish.canonical
      ) {
        return {
          ok: false,
          shouldVerify: true,
          message: `${column.name} stored ${this.formatDiagnosticValue(options.persistedValue)} instead of ${this.formatDiagnosticValue(expectedValue)}`,
        };
      }
      return {
        ok: true,
        shouldVerify: true,
      };
    }
    const expected = this.canonicalizeExactNumeric(
      expectedValue,
      constraint.scale,
    );
    if (!expected) {
      return null;
    }
    if (options === undefined) {
      const message = this.buildValidationMessage(
        column.name,
        constraint,
        expected,
      );
      if (message) {
        return {
          ok: false,
          shouldVerify: false,
          message,
        };
      }
      return {
        ok: true,
        shouldVerify: true,
      };
    }
    const actual = this.canonicalizeExactNumeric(
      options.persistedValue,
      constraint.scale,
    );
    if (!actual || actual.canonical !== expected.canonical) {
      return {
        ok: false,
        shouldVerify: true,
        message: `${column.name} stored ${this.formatDiagnosticValue(options.persistedValue)} instead of ${this.formatDiagnosticValue(expectedValue)}`,
      };
    }
    return {
      ok: true,
      shouldVerify: true,
    };
  }
}
