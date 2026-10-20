const NEEDS_QUOTING = /[",\r\n\t]/;
const FORMULA_AFTER_IGNORABLE_PREFIX =
  /^(?:[\p{White_Space}\p{Cc}\p{Cf}]|\u034F)*[=+\-@]/u;
const LEADING_CONTROL_PREFIX = /^[\t\r]/;

export function csvCell(value: unknown, trustedNumeric = false): string {
  if (value == null) {
    return "";
  }
  const raw = String(value);
  const s =
    typeof value === "string" &&
    !trustedNumeric &&
    (FORMULA_AFTER_IGNORABLE_PREFIX.test(raw) ||
      LEADING_CONTROL_PREFIX.test(raw))
      ? `'${raw}`
      : raw;
  if (s === "") {
    return "";
  }
  return NEEDS_QUOTING.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}
