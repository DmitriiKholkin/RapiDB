import { escape as escapeMysqlValue, format } from "mysql2";

export function formatMysqlStringLiteral(value: string): string {
  return `_utf8mb4 X'${Buffer.from(value, "utf8").toString("hex")}'`;
}

function formatValue(value: unknown, timezone?: string): string {
  if (typeof value === "string") return formatMysqlStringLiteral(value);
  if (Array.isArray(value)) {
    return value
      .map((entry) =>
        Array.isArray(entry)
          ? `(${formatValue(entry, timezone)})`
          : formatValue(entry, timezone),
      )
      .join(", ");
  }
  if (typeof value === "bigint") return String(value);
  if (value instanceof Uint8Array && !Buffer.isBuffer(value)) {
    return escapeMysqlValue(Buffer.from(value));
  }
  if (
    value === null ||
    value === undefined ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    value instanceof Date ||
    Buffer.isBuffer(value)
  ) {
    return escapeMysqlValue(value, false, timezone);
  }
  throw new Error(
    "Unsupported MySQL parameter value; serialize JSON values before binding.",
  );
}

export function formatMysqlQuery(
  sql: string,
  values?: unknown,
  timezone?: string,
): string {
  if (values === null || values === undefined) return sql;
  const parameters = Array.isArray(values) ? values : [values];
  return format(
    sql,
    parameters.map((value) => {
      const literal = formatValue(value, timezone);
      return { toSqlString: () => literal };
    }),
  );
}
