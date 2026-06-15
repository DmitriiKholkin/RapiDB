import { formatDatetimeForDisplay } from "../utils/dateUtils";

/**
 * Materializes SQL preview strings by replacing parameter placeholders
 * with actual values for display purposes.
 */
export class SqlPreviewMaterializer {
  /**
   * Materializes a SQL preview string by replacing parameter placeholders
   * with actual values.
   */
  materializePreviewSql(
    sql: string,
    params?: readonly unknown[],
    formatLiteral?: (value: unknown) => string,
  ): string {
    if (!params || params.length === 0) {
      return sql;
    }
    const formatter =
      formatLiteral ??
      ((value: unknown) => this.formatGenericPreviewSqlLiteral(value));
    if (sql.includes("?")) {
      return materializeSequentialPreviewSql(sql, params, formatter);
    }
    if (/\$\d+/.test(sql)) {
      return materializeIndexedPreviewSql(sql, params, "$", formatter);
    }
    if (/:\d+/.test(sql)) {
      return materializeIndexedPreviewSql(sql, params, ":", formatter);
    }
    return sql;
  }

  /**
   * Formats a value as a SQL literal for preview purposes.
   */
  formatGenericPreviewSqlLiteral(value: unknown): string {
    if (value === null || value === undefined) {
      return "NULL";
    }
    if (typeof value === "number") {
      return String(value);
    }
    if (typeof value === "boolean") {
      return value ? "TRUE" : "FALSE";
    }
    if (typeof value === "bigint") {
      return String(value);
    }
    if (value instanceof Date) {
      const formatted = formatDatetimeForDisplay(value);
      return formatted !== null ? `'${formatted}'` : `'${String(value)}'`;
    }
    if (typeof value === "string") {
      return `'${escapePreviewSqlString(value)}'`;
    }
    if (Buffer.isBuffer(value)) {
      return `X'${value.toString("hex")}'`;
    }
    if (value instanceof Uint8Array) {
      return `X'${Buffer.from(new Uint8Array(value)).toString("hex")}'`;
    }
    if (ArrayBuffer.isView(value)) {
      return `X'${Buffer.from(value.buffer, value.byteOffset, value.byteLength).toString("hex")}'`;
    }
    try {
      return `'${escapePreviewSqlString(JSON.stringify(value))}'`;
    } catch {
      return `'${escapePreviewSqlString(String(value))}'`;
    }
  }
}

/**
 * Escapes a string for use in SQL preview literals.
 */
function escapePreviewSqlString(value: string): string {
  return value.replace(/'/g, "''");
}

/**
 * Materializes sequential placeholder SQL preview.
 */
function materializeSequentialPreviewSql(
  sql: string,
  params: readonly unknown[],
  formatLiteral: (value: unknown) => string,
): string {
  const placeholderCount = (sql.match(/\?/g) ?? []).length;
  if (placeholderCount !== params.length) {
    throw new Error(
      `[RapiDB] Preview parameter mismatch: SQL has ${placeholderCount} placeholder(s) but ${params.length} value(s) were supplied.`,
    );
  }
  let index = 0;
  return sql.replace(/\?/g, () => formatLiteral(params[index++]));
}

/**
 * Materializes indexed placeholder SQL preview.
 */
function materializeIndexedPreviewSql(
  sql: string,
  params: readonly unknown[],
  marker: "$" | ":",
  formatLiteral: (value: unknown) => string,
): string {
  const placeholderPattern = marker === "$" ? /\$(\d+)/g : /:(\d+)/g;
  return sql.replace(placeholderPattern, (match, rawIndex: string) => {
    const paramIndex = Number.parseInt(rawIndex, 10) - 1;
    if (paramIndex < 0 || paramIndex >= params.length) {
      throw new Error(
        `[RapiDB] Preview parameter mismatch: ${match} is out of range for ${params.length} value(s).`,
      );
    }
    return formatLiteral(params[paramIndex]);
  });
}
