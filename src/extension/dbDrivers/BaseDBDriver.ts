import type {
  ColumnMeta,
  ColumnTypeMeta,
  DatabaseInfo,
  FilterConditionResult,
  FilterOperator,
  ForeignKeyMeta,
  IDBDriver,
  IndexMeta,
  PaginationResult,
  PersistedEditCheckOptions,
  PersistedEditCheckResult,
  QueryResult,
  SchemaInfo,
  TableInfo,
  TransactionOperation,
  TypeCategory,
  ValueSemantics,
} from "./types";
import {
  DATE_ONLY_RE,
  DATETIME_SQL_RE,
  filterOperatorsForCategory,
  ISO_DATETIME_RE,
  NULL_SENTINEL,
} from "./types";

// ─── Shared datetime formatting helper ───

const pad2 = (n: number) => String(n).padStart(2, "0");

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
      /^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})(\.\d+)?([+-]\d{2}(:\d{2})?|Z)?$/.exec(
        val,
      );
    if (m) {
      const [, date, time, rawFrac, tz] = m;
      let fracStr = "";
      if (rawFrac && rawFrac.length > 1) {
        const digits = rawFrac.slice(1).slice(0, 3).padEnd(3, "0");
        const msNum = parseInt(digits, 10);
        if (msNum > 0) {
          fracStr = `.${String(msNum).padStart(3, "0").replace(/0+$/, "")}`;
        }
      }
      return `${date} ${time}${fracStr}${tz ?? ""}`;
    }
  }
  return null;
}

export function isoToLocalDateStr(iso: string): string | null {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
}

export function hexFromBuffer(val: Buffer): string {
  return val.length === 0 ? "" : `\\x${val.toString("hex")}`;
}

function escapePreviewSqlString(value: string): string {
  return value.replace(/'/g, "''");
}

function formatPreviewSqlLiteral(value: unknown): string {
  if (value === null || value === undefined || value === NULL_SENTINEL) {
    return "NULL";
  }

  if (typeof value === "string") {
    return `'${escapePreviewSqlString(value)}'`;
  }

  if (typeof value === "number") {
    return Number.isFinite(value) ? String(value) : "NULL";
  }

  if (typeof value === "bigint") {
    return value.toString();
  }

  if (typeof value === "boolean") {
    return value ? "TRUE" : "FALSE";
  }

  if (value instanceof Date) {
    const formatted = formatDatetimeForDisplay(value) ?? value.toISOString();
    return `'${escapePreviewSqlString(formatted)}'`;
  }

  if (Buffer.isBuffer(value)) {
    return `X'${value.toString("hex")}'`;
  }

  if (value instanceof ArrayBuffer) {
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

function materializeSequentialPreviewSql(
  sql: string,
  params: readonly unknown[],
): string {
  const placeholderCount = (sql.match(/\?/g) ?? []).length;
  if (placeholderCount !== params.length) {
    throw new Error(
      `[RapiDB] Preview parameter mismatch: SQL has ${placeholderCount} placeholder(s) but ${params.length} value(s) were supplied.`,
    );
  }

  let index = 0;
  return sql.replace(/\?/g, () => formatPreviewSqlLiteral(params[index++]));
}

function materializeIndexedPreviewSql(
  sql: string,
  params: readonly unknown[],
  marker: "$" | ":",
): string {
  const placeholderPattern = marker === "$" ? /\$(\d+)/g : /:(\d+)/g;

  return sql.replace(placeholderPattern, (match, rawIndex: string) => {
    const paramIndex = Number.parseInt(rawIndex, 10) - 1;
    if (paramIndex < 0 || paramIndex >= params.length) {
      throw new Error(
        `[RapiDB] Preview parameter mismatch: ${match} is out of range for ${params.length} value(s).`,
      );
    }
    return formatPreviewSqlLiteral(params[paramIndex]);
  });
}

export function parseHexToBuffer(value: string): Buffer {
  const stripped =
    value.startsWith("\\x") ||
    value.startsWith("\\X") ||
    value.startsWith("0x") ||
    value.startsWith("0X")
      ? value.slice(2)
      : value;
  if (/^[0-9a-fA-F]*$/.test(stripped)) {
    if (stripped.length % 2 !== 0) {
      throw new Error(
        `Invalid hex value: odd number of hex digits in "${value}". ` +
          "Each byte requires exactly 2 hex digits.",
      );
    }
    return Buffer.from(stripped, "hex");
  }
  throw new Error(`Invalid hex string: "${value}"`);
}

export function isHexLike(value: string): boolean {
  if (
    value.startsWith("\\x") ||
    value.startsWith("\\X") ||
    value.startsWith("0x") ||
    value.startsWith("0X")
  ) {
    return /^[0-9a-fA-F]*$/.test(value.slice(2));
  }
  return /^[0-9a-fA-F]+$/.test(value) && value.length % 2 === 0;
}

function normalizeBooleanFilterValue(value: string): "true" | "false" | null {
  const normalized = value.trim().toLowerCase();
  if (normalized === "true" || normalized === "1") return "true";
  if (normalized === "false" || normalized === "0") return "false";
  return null;
}

function normalizeDateFilterValue(value: string): string | null {
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

function looksLikeDateInput(value: string): boolean {
  return /^\d{4}-\d{2}-\d{2}(?:[ T].*)?$/.test(value.trim());
}

export function hasExplicitTimezone(value: string): boolean {
  return /[zZ]|[+-]\d{2}:\d{2}$/.test(value);
}

export function normalizeSqlDatetimeOffsetSpacing(value: string): string {
  return value.replace(/ ([+-]\d{2}:\d{2})$/, "$1");
}

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
    /^(\d{4}-\d{2}-\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?: ?(?:Z|[+-]\d{2}:\d{2}))?$/i.exec(
      value,
    );
  if (!match) return false;

  const [, date, rawHours, rawMinutes, rawSeconds] = match;
  const hours = Number(rawHours);
  const minutes = Number(rawMinutes);
  const seconds = Number(rawSeconds);

  return isValidDateOnly(date) && hours < 24 && minutes < 60 && seconds < 60;
}

function invalidFilterInputError(columnName: string, expected: string): Error {
  return new Error(`[RapiDB Filter] Column ${columnName} expects ${expected}.`);
}

const PERSISTED_EDIT_NULL_TOKEN = "\x00__RAPIDB_PERSISTED_EDIT_NULL__\x00";
const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

interface ExactNumericConstraint {
  precision: number | null;
  scale: number | null;
}

interface CanonicalPersistedEditValue {
  canonical: string;
}

type PersistedEditCanonicalizer = (
  value: unknown,
) => CanonicalPersistedEditValue | null;

interface CanonicalExactNumericValue {
  canonical: string;
  integerDigits: number;
  fractionDigits: number;
  scaleOverflow: boolean;
}

function parseTypePrecisionScale(nativeType: string): ExactNumericConstraint {
  const match = /\((\d+)(?:\s*,\s*(-?\d+))?\)/.exec(nativeType);
  if (!match) {
    return { precision: null, scale: null };
  }

  return {
    precision: Number.parseInt(match[1], 10),
    scale: match[2] === undefined ? null : Number.parseInt(match[2], 10),
  };
}

function numberToDecimalString(value: number): string {
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

function parseDecimalString(value: unknown): string | null {
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

function canonicalizeExactNumeric(
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

function formatDiagnosticValue(value: unknown): string {
  if (value === null) return "NULL";
  if (value === undefined) return "<missing>";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }

  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function buildValidationMessage(
  columnName: string,
  constraint: ExactNumericConstraint,
  canonical: CanonicalExactNumericValue,
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

function canonicalizeNullishPersistedEditValue(
  value: unknown,
): CanonicalPersistedEditValue | null {
  if (value === NULL_SENTINEL || value === null) {
    return { canonical: PERSISTED_EDIT_NULL_TOKEN };
  }

  return null;
}

function canonicalizeTextPersistedEditValue(
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

function canonicalizeBooleanPersistedEditValue(
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

function canonicalizeUuidPersistedEditValue(
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

function stableJsonValue(value: unknown): unknown {
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

function canonicalizeJsonPersistedEditValue(
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

function canonicalizeJsonArrayPersistedEditValue(
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

function canonicalizeBinaryPersistedEditValue(
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

function canonicalizeApproximateNumericPersistedEditValue(
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

function findApproximateNumericPrecisionLoss(
  value: unknown,
  significantDigits: number,
): { roundedValue: string } | null {
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

// ─── Abstract base driver ───

export abstract class BaseDBDriver implements IDBDriver {
  // ── Abstract methods each driver MUST implement ──
  abstract connect(): Promise<void>;
  abstract disconnect(): Promise<void>;
  abstract isConnected(): boolean;
  abstract listDatabases(): Promise<DatabaseInfo[]>;
  abstract listSchemas(database: string): Promise<SchemaInfo[]>;
  abstract listObjects(database: string, schema: string): Promise<TableInfo[]>;
  abstract describeTable(
    database: string,
    schema: string,
    table: string,
  ): Promise<ColumnMeta[]>;
  abstract getIndexes(
    database: string,
    schema: string,
    table: string,
  ): Promise<IndexMeta[]>;
  abstract getForeignKeys(
    database: string,
    schema: string,
    table: string,
  ): Promise<ForeignKeyMeta[]>;
  abstract getCreateTableDDL(
    database: string,
    schema: string,
    table: string,
  ): Promise<string>;
  abstract getRoutineDefinition(
    database: string,
    schema: string,
    name: string,
    kind: "function" | "procedure",
  ): Promise<string>;
  abstract query(sql: string, params?: unknown[]): Promise<QueryResult>;
  abstract runTransaction(operations: TransactionOperation[]): Promise<void>;

  // ── Abstract: each driver maps its native types to TypeCategory ──
  abstract mapTypeCategory(nativeType: string): TypeCategory;

  // ── Abstract: each driver determines value semantics ──
  protected abstract getValueSemantics(
    nativeType: string,
    category: TypeCategory,
  ): ValueSemantics;

  isBooleanType(nativeType: string): boolean {
    return (
      this.getValueSemantics(nativeType, this.mapTypeCategory(nativeType)) ===
      "boolean"
    );
  }

  isBitType(nativeType: string): boolean {
    return (
      this.getValueSemantics(nativeType, this.mapTypeCategory(nativeType)) ===
      "bit"
    );
  }

  // ── Abstract: each driver determines datetime-with-time detection ──
  abstract isDatetimeWithTime(nativeType: string): boolean;

  // ─── describeColumns: wraps describeTable + enriches ───

  async describeColumns(
    database: string,
    schema: string,
    table: string,
  ): Promise<ColumnTypeMeta[]> {
    const cols = await this.describeTable(database, schema, table);
    return cols.map((c) => this.enrichColumn(c));
  }

  protected enrichColumn(col: ColumnMeta): ColumnTypeMeta {
    const category = this.mapTypeCategory(col.type);
    const valueSemantics = this.getValueSemantics(col.type, category);
    const filterable = this.isFilterable(col.type, category);
    const filterOperators: FilterOperator[] = filterable
      ? filterOperatorsForCategory(category)
      : col.nullable
        ? ["is_null", "is_not_null"]
        : [];
    return {
      ...col,
      category,
      nativeType: col.type,
      filterable,
      filterOperators,
      valueSemantics,
    };
  }

  protected isFilterable(_nativeType: string, category: TypeCategory): boolean {
    return category !== "lob";
  }

  // ─── SQL helpers ───

  quoteIdentifier(name: string): string {
    return `"${name.replace(/"/g, '""')}"`;
  }

  qualifiedTableName(_database: string, schema: string, table: string): string {
    const parts: string[] = [];
    if (schema) parts.push(this.quoteIdentifier(schema));
    parts.push(this.quoteIdentifier(table));
    return parts.join(".");
  }

  buildPagination(
    offset: number,
    limit: number,
    _paramIndex: number,
  ): PaginationResult {
    return {
      sql: `LIMIT ? OFFSET ?`,
      params: [limit, offset],
    };
  }

  buildOrderByDefault(cols: ColumnTypeMeta[]): string {
    const pkCols = cols
      .filter((c) => c.isPrimaryKey)
      .sort((left, right) => {
        const leftOrdinal = left.primaryKeyOrdinal ?? Number.MAX_SAFE_INTEGER;
        const rightOrdinal = right.primaryKeyOrdinal ?? Number.MAX_SAFE_INTEGER;
        return leftOrdinal - rightOrdinal;
      });
    if (pkCols.length === 0) return "";
    return `ORDER BY ${pkCols.map((c) => this.quoteIdentifier(c.name)).join(", ")}`;
  }

  buildInsertValueExpr(_column: ColumnTypeMeta, _paramIndex: number): string {
    return "?";
  }

  buildSetExpr(column: ColumnTypeMeta, _paramIndex: number): string {
    return `${this.quoteIdentifier(column.name)} = ?`;
  }

  materializePreviewSql(sql: string, params?: readonly unknown[]): string {
    if (!params || params.length === 0) {
      return sql;
    }

    if (sql.includes("?")) {
      return materializeSequentialPreviewSql(sql, params);
    }

    if (/\$\d+/.test(sql)) {
      return materializeIndexedPreviewSql(sql, params, "$");
    }

    if (/:\d+/.test(sql)) {
      return materializeIndexedPreviewSql(sql, params, ":");
    }

    return sql;
  }

  // ─── Type-aware data helpers ───

  protected hasBooleanSemantics(
    column: Pick<ColumnTypeMeta, "valueSemantics">,
  ): boolean {
    return column.valueSemantics === "boolean";
  }

  protected hasBitSemantics(
    column: Pick<ColumnTypeMeta, "valueSemantics">,
  ): boolean {
    return column.valueSemantics === "bit";
  }

  protected parseBooleanInput(value: string): boolean | null {
    const lower = value.trim().toLowerCase();
    if (lower === "true" || lower === "1") return true;
    if (lower === "false" || lower === "0") return false;
    return null;
  }

  coerceInputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (value === null || value === undefined || value === "") return value;
    if (value === NULL_SENTINEL) return null;
    if (typeof value !== "string") return value;

    const booleanValue = this.parseBooleanInput(value);

    if (this.hasBooleanSemantics(column) && booleanValue !== null) {
      return booleanValue
        ? this.coerceBooleanTrue()
        : this.coerceBooleanFalse();
    }

    if (this.hasBitSemantics(column)) {
      const bitValue = this.coerceBitInputValue(value, column);
      if (bitValue !== undefined) {
        return bitValue;
      }
    }

    if (column.category === "binary" && isHexLike(value)) {
      return parseHexToBuffer(value);
    }

    return value;
  }

  protected coerceBooleanTrue(): unknown {
    return true;
  }
  protected coerceBooleanFalse(): unknown {
    return false;
  }

  protected coerceBitInputValue(
    _value: string,
    _column: ColumnTypeMeta,
  ): unknown | undefined {
    return undefined;
  }

  formatOutputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (value === null || value === undefined) return value;

    if (Buffer.isBuffer(value)) return hexFromBuffer(value);
    if (typeof value === "bigint") return value.toString();

    if (value instanceof Date) {
      if (column.category === "date") {
        return isoToLocalDateStr(value.toISOString()) ?? value;
      }
      return formatDatetimeForDisplay(value) ?? value;
    }

    if (value !== null && typeof value === "object") {
      return JSON.stringify(value);
    }

    if (this.isDatetimeWithTime(column.nativeType)) {
      const formatted = formatDatetimeForDisplay(value);
      if (formatted !== null) return formatted;
    }

    return value;
  }

  checkPersistedEdit(
    _column: ColumnTypeMeta,
    _expectedValue: unknown,
    _options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null {
    return null;
  }

  protected parseExactNumericConstraint(
    nativeType: string,
  ): ExactNumericConstraint {
    return parseTypePrecisionScale(nativeType);
  }

  protected checkExactNumericPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    constraint: ExactNumericConstraint | null,
    options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null {
    if (!constraint) {
      return null;
    }

    const expectedNullish =
      canonicalizeNullishPersistedEditValue(expectedValue);
    if (expectedNullish) {
      if (options === undefined) {
        return {
          ok: true,
          shouldVerify: true,
        };
      }

      const actualNullish = canonicalizeNullishPersistedEditValue(
        options.persistedValue,
      );
      if (
        !actualNullish ||
        actualNullish.canonical !== expectedNullish.canonical
      ) {
        return {
          ok: false,
          shouldVerify: true,
          message: `${column.name} stored ${formatDiagnosticValue(options.persistedValue)} instead of ${formatDiagnosticValue(expectedValue)}`,
        };
      }

      return {
        ok: true,
        shouldVerify: true,
      };
    }

    const expected = canonicalizeExactNumeric(expectedValue, constraint.scale);
    if (!expected) {
      return null;
    }

    if (options === undefined) {
      const message = buildValidationMessage(column.name, constraint, expected);
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

    const actual = canonicalizeExactNumeric(
      options.persistedValue,
      constraint.scale,
    );
    if (!actual || actual.canonical !== expected.canonical) {
      return {
        ok: false,
        shouldVerify: true,
        message: `${column.name} stored ${formatDiagnosticValue(options.persistedValue)} instead of ${formatDiagnosticValue(expectedValue)}`,
      };
    }

    return {
      ok: true,
      shouldVerify: true,
    };
  }

  protected checkNormalizedPersistedEdit(
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
        message: `${column.name} stored ${formatDiagnosticValue(options.persistedValue)} instead of ${formatDiagnosticValue(expectedValue)}`,
      };
    }

    return {
      ok: true,
      shouldVerify: true,
    };
  }

  protected checkTextPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null {
    return this.checkNormalizedPersistedEdit(
      column,
      expectedValue,
      options,
      canonicalizeTextPersistedEditValue,
      `Column "${column.name}" expects a text value.`,
    );
  }

  protected checkBooleanPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null {
    return this.checkNormalizedPersistedEdit(
      column,
      expectedValue,
      options,
      canonicalizeBooleanPersistedEditValue,
      `Column "${column.name}" expects true or false.`,
    );
  }

  protected checkUuidPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null {
    return this.checkNormalizedPersistedEdit(
      column,
      expectedValue,
      options,
      canonicalizeUuidPersistedEditValue,
      `Column "${column.name}" expects a valid UUID.`,
    );
  }

  protected checkJsonPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null {
    return this.checkNormalizedPersistedEdit(
      column,
      expectedValue,
      options,
      canonicalizeJsonPersistedEditValue,
      `Column "${column.name}" expects valid JSON.`,
    );
  }

  protected checkJsonArrayPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null {
    return this.checkNormalizedPersistedEdit(
      column,
      expectedValue,
      options,
      canonicalizeJsonArrayPersistedEditValue,
      `Column "${column.name}" expects a JSON array value.`,
    );
  }

  protected checkBinaryPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null {
    return this.checkNormalizedPersistedEdit(
      column,
      expectedValue,
      options,
      canonicalizeBinaryPersistedEditValue,
      `Column "${column.name}" expects a hex value like \\xDEADBEEF.`,
    );
  }

  protected checkApproximateNumericPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    significantDigits: number,
    options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null {
    if (options === undefined) {
      const precisionLoss = findApproximateNumericPrecisionLoss(
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
        canonicalizeApproximateNumericPersistedEditValue(
          value,
          significantDigits,
        ),
      `Column "${column.name}" expects a numeric value.`,
    );
  }

  normalizeFilterValue(
    column: ColumnTypeMeta,
    operator: FilterOperator,
    value: string | [string, string] | undefined,
  ): string | [string, string] | undefined {
    if (operator === "is_null" || operator === "is_not_null") {
      return undefined;
    }

    if (value === undefined) {
      return undefined;
    }

    if (operator === "between") {
      if (!Array.isArray(value)) {
        return value;
      }

      return [
        this.normalizeScalarFilterValue(column, value[0], operator),
        this.normalizeScalarFilterValue(column, value[1], operator),
      ];
    }

    if (typeof value !== "string") {
      return value;
    }

    return this.normalizeScalarFilterValue(column, value, operator);
  }

  protected normalizeScalarFilterValue(
    column: ColumnTypeMeta,
    rawValue: string,
    operator: FilterOperator,
  ): string {
    const value = rawValue.trim();
    if (value === "") {
      throw invalidFilterInputError(column.name, "a filter value");
    }

    if (this.hasBooleanSemantics(column)) {
      const normalized = normalizeBooleanFilterValue(value);
      if (!normalized) {
        throw invalidFilterInputError(column.name, "true or false");
      }
      return normalized;
    }

    if (this.isNumericCategory(column.category)) {
      if (operator === "in") {
        const values = value
          .split(",")
          .map((part) => part.trim())
          .filter(Boolean);
        if (
          values.length === 0 ||
          values.some((part) => !Number.isFinite(Number(part)))
        ) {
          throw invalidFilterInputError(column.name, "comma-separated numbers");
        }
        return values.join(", ");
      }

      const numericValue = Number(value);
      if (Number.isNaN(numericValue) || !Number.isFinite(numericValue)) {
        throw invalidFilterInputError(column.name, "a number");
      }
      return value;
    }

    if (column.category === "date") {
      if (operator === "like") {
        return value;
      }

      if (operator === "in") {
        const values = value
          .split(",")
          .map((part) => part.trim())
          .filter(Boolean);
        if (values.length === 0) {
          throw invalidFilterInputError(column.name, "comma-separated dates");
        }

        const normalizedValues = values.map((part) => {
          const normalizedDate = normalizeDateFilterValue(part);
          if (!normalizedDate) {
            throw invalidFilterInputError(column.name, "a valid date");
          }
          return normalizedDate;
        });

        return normalizedValues.join(", ");
      }

      const normalizedDate = normalizeDateFilterValue(value);
      if (normalizedDate) {
        return normalizedDate;
      }

      if (looksLikeDateInput(value)) {
        throw invalidFilterInputError(column.name, "a valid date");
      }
    }

    return value;
  }

  // ─── Filter condition building (default: CAST AS TEXT LIKE) ───

  buildFilterCondition(
    column: ColumnTypeMeta,
    operator: FilterOperator,
    value: string | [string, string] | undefined,
    paramIndex: number,
  ): FilterConditionResult | null {
    const col = this.quoteIdentifier(column.name);

    // Null checks
    if (operator === "is_null") return { sql: `${col} IS NULL`, params: [] };
    if (operator === "is_not_null")
      return { sql: `${col} IS NOT NULL`, params: [] };

    if (!column.filterable) return null;
    if (value === undefined) return null;

    const val = typeof value === "string" ? value.trim() : value;

    // Boolean
    if (
      this.hasBooleanSemantics(column) &&
      (operator === "eq" || operator === "neq")
    ) {
      const strVal = (typeof val === "string" ? val : val[0]).toLowerCase();
      if (strVal === "true" || strVal === "false") {
        return this.buildBooleanFilter(
          col,
          operator,
          strVal === "true",
          paramIndex,
        );
      }
    }

    // Numeric exact match
    if (
      this.isNumericCategory(column.category) &&
      typeof val === "string" &&
      !Number.isNaN(Number(val)) &&
      val !== ""
    ) {
      return this.buildNumericFilter(col, column, operator, val, paramIndex);
    }

    // Between
    if (operator === "between" && Array.isArray(val)) {
      return this.buildBetweenFilter(col, column, val, paramIndex);
    }

    // Default: text-based comparison
    return this.buildTextFilter(
      col,
      column,
      operator,
      typeof val === "string" ? val : val[0],
      paramIndex,
    );
  }

  protected isNumericCategory(cat: TypeCategory): boolean {
    return cat === "integer" || cat === "float" || cat === "decimal";
  }

  protected buildBooleanFilter(
    col: string,
    operator: FilterOperator,
    isTrue: boolean,
    _paramIndex: number,
  ): FilterConditionResult {
    const op = operator === "neq" ? "!=" : "=";
    return { sql: `${col} ${op} ?`, params: [isTrue ? 1 : 0] };
  }

  protected buildNumericFilter(
    col: string,
    _column: ColumnTypeMeta,
    operator: FilterOperator,
    val: string,
    _paramIndex: number,
  ): FilterConditionResult {
    const num = Number(val);
    const sqlOp = this.sqlOperator(operator);
    return { sql: `${col} ${sqlOp} ?`, params: [num] };
  }

  protected buildBetweenFilter(
    col: string,
    _column: ColumnTypeMeta,
    val: [string, string],
    _paramIndex: number,
  ): FilterConditionResult {
    return { sql: `${col} BETWEEN ? AND ?`, params: [val[0], val[1]] };
  }

  protected buildTextFilter(
    col: string,
    _column: ColumnTypeMeta,
    operator: FilterOperator,
    val: string,
    _paramIndex: number,
  ): FilterConditionResult {
    const sqlOp = this.sqlOperator(operator);
    if (operator === "like" || operator === "ilike") {
      return { sql: `CAST(${col} AS CHAR) LIKE ?`, params: [`%${val}%`] };
    }
    // NOTE: `eq` and `neq` are not in TEXT_OPS so they will never appear as
    // filter-operator choices for text columns in the UI.  These branches are
    // a defensive fallback for callers that invoke buildFilterCondition
    // directly with an arbitrary operator (e.g. unit-tests, future drivers).
    if (operator === "eq") {
      return { sql: `CAST(${col} AS CHAR) LIKE ?`, params: [`%${val}%`] };
    }
    if (operator === "neq") {
      return { sql: `CAST(${col} AS CHAR) NOT LIKE ?`, params: [`%${val}%`] };
    }
    if (operator === "in") {
      const parts = val.split(",").map((s) => s.trim());
      return {
        sql: `${col} IN (${parts.map(() => "?").join(", ")})`,
        params: parts,
      };
    }
    return { sql: `${col} ${sqlOp} ?`, params: [val] };
  }

  protected sqlOperator(op: FilterOperator): string {
    switch (op) {
      case "eq":
        return "=";
      case "neq":
        return "!=";
      case "gt":
        return ">";
      case "gte":
        return ">=";
      case "lt":
        return "<";
      case "lte":
        return "<=";
      default:
        return "=";
    }
  }
}
