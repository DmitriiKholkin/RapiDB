/**
 * Pure date/datetime formatting and parsing utilities.
 *
 * Extracted from BaseDBDriver to enable reuse across drivers and services
 * without requiring a class inheritance hierarchy. All functions in this
 * module are pure (same input → same output, no side effects).
 */

const pad2 = (n: number): string => String(n).padStart(2, "0");

/** Datetime prefix used as a carrier when normalizing a bare offset. */
const DATETIME_CARRIER_PREFIX = "2000-01-01 00:00:00";
const DATETIME_CARRIER_PREFIX_LENGTH = DATETIME_CARRIER_PREFIX.length;

/**
 * Normalize a bare timezone offset (`+05`, `+0500`, `+05:00`, `Z`) by
 * routing it through `normalizeSqlDatetimeOffsetSpacing`. The carrier
 * date is purely structural — it is sliced off before returning.
 */
function normalizeTimezoneOffset(offset: string): string {
  if (offset === "Z") {
    return "Z";
  }
  return normalizeSqlDatetimeOffsetSpacing(
    `${DATETIME_CARRIER_PREFIX}${offset}`,
  ).slice(DATETIME_CARRIER_PREFIX_LENGTH);
}

function trimTrailingZerosFromFraction(rawFrac: string | undefined): string {
  if (!rawFrac || rawFrac.length <= 1) {
    return "";
  }
  const digits = rawFrac.slice(1).replace(/0+$/, "");
  return digits.length > 0 ? `.${digits}` : "";
}

// ── Display formatting ──────────────────────────────────────────────────────

/**
 * Format a Date or ISO-like string for human-readable display.
 *
 * Returns null if the input cannot be interpreted as a valid datetime.
 * For Date objects, uses UTC fields. For strings, normalizes fractional
 * seconds and timezone offset spacing.
 */
export function formatDatetimeForDisplay(val: unknown): string | null {
  if (val instanceof Date) {
    if (Number.isNaN(val.getTime())) return null;
    const ms = val.getUTCMilliseconds();
    const frac = ms > 0 ? `.${String(ms).padStart(3, "0")}` : "";
    return (
      `${val.getUTCFullYear()}-${pad2(val.getUTCMonth() + 1)}-${pad2(val.getUTCDate())} ` +
      `${pad2(val.getUTCHours())}:${pad2(val.getUTCMinutes())}:${pad2(val.getUTCSeconds())}${frac}`
    );
  }
  if (typeof val === "string") {
    const m =
      /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(\.\d+)?([+-]\d{2}(?::?\d{2})?|Z)?$/.exec(
        val,
      );
    if (!m) {
      return null;
    }
    const [, date, time, rawFrac, tz] = m;
    const fracStr = trimTrailingZerosFromFraction(rawFrac);
    const normalizedTimezone = tz ? normalizeTimezoneOffset(tz) : "";
    return `${date} ${time}${fracStr}${normalizedTimezone}`;
  }
  return null;
}

/**
 * Convert an ISO datetime string to a local date string (YYYY-MM-DD).
 * Returns null if the input is not a valid ISO datetime.
 */
export function isoToLocalDateStr(iso: string): string | null {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
}

// ── Timezone handling ───────────────────────────────────────────────────────

/**
 * Check whether a datetime string contains an explicit timezone indicator
 * (Z, +HH, +HH:MM, -HH, -HH:MM).
 */
export function hasExplicitTimezone(value: string): boolean {
  return /[zZ]|[+-]\d{2}(?::?\d{2})?$/.test(value);
}

/**
 * Normalize the spacing of SQL datetime offset strings.
 *
 * Converts variants like "2024-01-15 12:00:00+05", "+0500", "+05" into
 * the canonical "+05:00" form.
 */
export function normalizeSqlDatetimeOffsetSpacing(value: string): string {
  const compact = value.replace(/ ([+-]\d{2}(?::?\d{2})?)$/, "$1");
  if (/[+-]\d{2}:\d{2}$/.test(compact)) {
    return compact;
  }
  if (/[+-]\d{4}$/.test(compact)) {
    return compact.replace(/([+-]\d{2})(\d{2})$/, "$1:$2");
  }
  if (/[+-]\d{2}$/.test(compact)) {
    return `${compact}:00`;
  }
  return compact;
}

// ── Validation ──────────────────────────────────────────────────────────────

/** Regex for date-only strings (YYYY-MM-DD). */
export const DATE_ONLY_RE = /^\d{4}-\d{2}-\d{2}$/;

/** Regex for ISO-like datetime strings. */
export const ISO_DATETIME_RE = /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}/;

/** Regex for SQL-style datetime strings (YYYY-MM-DD HH:MM:SS). */
export const DATETIME_SQL_RE =
  /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?: ?(?:Z|[+-]\d{2}(?::?\d{2})?))?$/i;

/**
 * Validate that a date-only string (YYYY-MM-DD) represents a real calendar date.
 */
export function isValidDateOnly(value: string): boolean {
  if (!DATE_ONLY_RE.test(value)) return false;
  const parsed = new Date(`${value}T00:00:00Z`);
  if (Number.isNaN(parsed.getTime())) return false;
  const [year, month, day] = value.split("-").map(Number);
  return (
    parsed.getUTCFullYear() === year &&
    parsed.getUTCMonth() + 1 === month &&
    parsed.getUTCDate() === day
  );
}

/**
 * Validate that the individual date/time parts of a datetime string are valid.
 */
export function hasValidDateTimeParts(value: string): boolean {
  const match =
    /^(\d{4}-\d{2}-\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?: ?(?:Z|[+-]\d{2}(?::?\d{2})?))?$/i.exec(
      value,
    );
  if (!match) return false;
  const [, date, rawHours, rawMinutes, rawSeconds] = match;
  const hours = Number(rawHours);
  const minutes = Number(rawMinutes);
  const seconds = Number(rawSeconds);
  return isValidDateOnly(date) && hours < 24 && minutes < 60 && seconds < 60;
}

/**
 * Normalize a user-provided date filter value into a canonical date string.
 *
 * Handles ISO datetimes, SQL datetimes, and date-only strings.
 * Returns null if the input is not a recognizable date value.
 */
export function normalizeDateFilterValue(value: string): string | null {
  const normalized = value.trim();
  const normalizedSql = normalizeSqlDatetimeOffsetSpacing(normalized);
  if (DATE_ONLY_RE.test(normalized)) {
    return isValidDateOnly(normalized) ? normalized : null;
  }
  if (ISO_DATETIME_RE.test(normalized)) {
    if (!hasValidDateTimeParts(normalized)) {
      return null;
    }
    if (!hasExplicitTimezone(normalized)) {
      const dateOnly = normalized.slice(0, 10);
      return isValidDateOnly(dateOnly) ? dateOnly : null;
    }
    return isoToLocalDateStr(normalized);
  }
  if (DATETIME_SQL_RE.test(normalizedSql)) {
    if (!hasValidDateTimeParts(normalizedSql)) {
      return null;
    }
    if (hasExplicitTimezone(normalizedSql)) {
      return isoToLocalDateStr(normalizedSql.replace(" ", "T"));
    }
    const dateOnly = normalizedSql.slice(0, 10);
    return isValidDateOnly(dateOnly) ? dateOnly : null;
  }
  return null;
}

/**
 * Quick heuristic check: does the value look like a date input?
 * Used to decide whether to show a "not a valid date" error.
 */
export function looksLikeDateInput(value: string): boolean {
  return /^\d{4}-\d{2}-\d{2}(?:[ T].*)?$/.test(value.trim());
}
