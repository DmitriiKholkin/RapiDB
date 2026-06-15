/**
 * Tiny pure helpers shared by all per-domain validation modules.
 */

import type { ConnectionConfig } from "../connectionConfig";

/**
 * Returns true when the value is "present" for validation purposes.
 *   - strings: non-empty after trim
 *   - everything else: neither `undefined` nor `null`
 */
export function hasValue(value: unknown): boolean {
  if (typeof value === "string") {
    return value.trim().length > 0;
  }
  return value !== undefined && value !== null;
}

/** True if at least one field in `fields` has a value on `config`. */
export function isSatisfied(
  config: Partial<ConnectionConfig>,
  fields: readonly (keyof ConnectionConfig)[],
): boolean {
  return fields.some((field) => hasValue(config[field]));
}

/** True for strictly positive integers (used for port validation). */
export function isPositiveInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value > 0;
}
