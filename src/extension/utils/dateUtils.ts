const pad2 = (n: number): string => String(n).padStart(2, "0");

const DATETIME_CARRIER_PREFIX = "2000-01-01 00:00:00";
const DATETIME_CARRIER_PREFIX_LENGTH = DATETIME_CARRIER_PREFIX.length;

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

export function isoToLocalDateStr(iso: string): string | null {
  const normalized = normalizeSqlDatetimeOffsetSpacing(iso.trim()).replace(
    / ([zZ])$/,
    "$1",
  );
  if (ISO_DATETIME_RE.test(normalized)) {
    if (!hasValidDateTimeParts(normalized)) return null;
    const dateOnly = normalized.slice(0, 10);
    if (!isValidDateOnly(dateOnly)) return null;
    if (!hasExplicitTimezone(normalized)) return dateOnly;
  }
  const d = new Date(normalized.replace(" ", "T"));
  if (Number.isNaN(d.getTime())) return null;
  return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
}

export function hasExplicitTimezone(value: string): boolean {
  return /[zZ]|[+-]\d{2}(?::?\d{2})?$/.test(value);
}

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

export const DATE_ONLY_RE = /^\d{4}-\d{2}-\d{2}$/;

export const ISO_DATETIME_RE = /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}/;

export const DATETIME_SQL_RE =
  /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?: ?(?:Z|[+-]\d{2}(?::?\d{2})?))?$/i;

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

export function looksLikeDateInput(value: string): boolean {
  return /^\d{4}-\d{2}-\d{2}(?:[ T].*)?$/.test(value.trim());
}
