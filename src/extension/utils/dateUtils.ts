const pad2 = (n: number): string => String(n).padStart(2, "0");

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

function hasExplicitTimezone(value: string): boolean {
  return /[zZ]|[+-]\d{2}(?::?\d{2})?$/.test(value);
}

function normalizeSqlDatetimeOffsetSpacing(value: string): string {
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

const DATE_ONLY_RE = /^\d{4}-\d{2}-\d{2}$/;

const ISO_DATETIME_RE = /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}/;

function isValidDateOnly(value: string): boolean {
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

function hasValidDateTimeParts(value: string): boolean {
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
