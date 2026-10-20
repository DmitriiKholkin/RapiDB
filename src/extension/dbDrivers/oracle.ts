import oracledb from "oracledb";
import type { DdlOnlyDbObjectKind } from "../../shared/dbObjectKinds";
import type { OperationCancellationContext } from "../../shared/safetyContracts";
import type { ConnectionConfig } from "../connectionManager";
import { getSshTcpForwardTransport } from "../driverRuntimeConfig";
import {
  canonicalizeJsonNumericValues,
  isRawNumberToken,
  parseJsonPreservingRawNumbers,
} from "../utils/jsonCanonical";
import { ORACLE_READ_ONLY_ENFORCEMENT_MESSAGE } from "../utils/readOnlyGuards";
import { oracleAlternativeQuoteEnd } from "../utils/sqlStatementScan";
import { BaseDBDriver, formatDatetimeForDisplay } from "./BaseDBDriver";
import { BoundedQueryRows, queryCollectionLimit } from "./boundedQueryRows";
import { literalContainsPattern } from "./literalContains";
import {
  getMetadataContext,
  withMetadataResource,
} from "./metadataCancellation";
import {
  indexedPlaceholderOffsets,
  replaceIndexedPlaceholders,
} from "./sqlPlaceholders";
import {
  escapeSqlPreviewStringLiteral,
  formatHexSqlPreviewLiteral,
  formatSqlPreviewStringLiteral,
} from "./sqlPreviewLiterals";
import {
  type DriverTimeoutSettingsProvider,
  throwIfTransactionCancelled,
} from "./timeout";
import {
  TransactionIdentityStore,
  verifyTransaction,
} from "./transactionVerification";
import type {
  ColumnMeta,
  ColumnTypeMeta,
  DatabaseInfo,
  DriverEntityManifest,
  FilterConditionResult,
  FilterOperator,
  ForeignKeyMeta,
  IndexMeta,
  MutationSqlExpression,
  PaginationResult,
  PersistedEditCheckOptions,
  PersistedEditCheckResult,
  QueryResult,
  SchemaInfo,
  TableInfo,
  TypeCategory,
  ValueSemantics,
} from "./types";
import {
  assertTransactionAffectedRows,
  DATETIME_SQL_RE,
  NULL_SENTINEL,
} from "./types";

const ORACLE_ENTITY_MANIFEST: DriverEntityManifest = {
  dbObjectKinds: [
    "table",
    "view",
    "materializedView",
    "function",
    "procedure",
    "sequence",
    "type",
  ],
  tableSections: {
    columns: "supported",
    constraints: "supported",
    indexes: "supported",
    triggers: "supported",
  },
  tableSectionOverridesByObjectKind: {
    view: {
      constraints: "not_applicable",
      indexes: "not_applicable",
    },
    materializedView: {
      constraints: "not_applicable",
      triggers: "not_applicable",
    },
  },
};

function oracleFullType(
  dataType: string,
  dataPrecision: number | null,
  dataScale: number | null,
  dataLength: number,
  charLength: number | null = null,
  charUsed: string | null = null,
): string {
  const trimmedType = dataType.trim();
  const normalizedType = trimmedType.toUpperCase();
  if (["NCHAR", "NVARCHAR2"].includes(normalizedType)) {
    return `${trimmedType}(${charLength ?? dataLength})`;
  }
  if (["CHAR", "VARCHAR2"].includes(normalizedType)) {
    const usesCharacterLength = charUsed === "C";
    const length = usesCharacterLength
      ? (charLength ?? dataLength)
      : dataLength;
    const lengthSemantics = usesCharacterLength
      ? " CHAR"
      : charUsed === "B"
        ? " BYTE"
        : "";
    return `${trimmedType}(${length}${lengthSemantics})`;
  }
  if (normalizedType === "NUMBER") {
    if (dataPrecision !== null && dataScale !== null) {
      return `NUMBER(${dataPrecision},${dataScale})`;
    }
    if (dataPrecision !== null) {
      return `NUMBER(${dataPrecision})`;
    }
    return "NUMBER";
  }
  if (normalizedType === "FLOAT") {
    return dataPrecision !== null ? `FLOAT(${dataPrecision})` : "FLOAT";
  }
  if (normalizedType === "RAW") {
    return `${trimmedType}(${dataLength})`;
  }
  return trimmedType;
}
oracledb.fetchAsString = [oracledb.CLOB];
oracledb.fetchAsBuffer = [oracledb.BLOB];
const pad2 = (n: number) => String(n).padStart(2, "0");
const ORACLE_DDL_TRANSFORM_BLOCK = `
BEGIN
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'PRETTY', TRUE);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'TABLESPACE', FALSE);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'CONSTRAINTS_AS_ALTER', FALSE);
END;`;
const ORACLE_INTERVAL_DS_NANOS_PER_SECOND = 1000000000n;
const ORACLE_INTERVAL_DS_NANOS_PER_MINUTE =
  60n * ORACLE_INTERVAL_DS_NANOS_PER_SECOND;
const ORACLE_INTERVAL_DS_NANOS_PER_HOUR =
  60n * ORACLE_INTERVAL_DS_NANOS_PER_MINUTE;
const ORACLE_INTERVAL_DS_NANOS_PER_DAY =
  24n * ORACLE_INTERVAL_DS_NANOS_PER_HOUR;
const ORACLE_MAX_SAFE_INTEGER = BigInt(Number.MAX_SAFE_INTEGER);
const ORACLE_MIN_SAFE_INTEGER = BigInt(Number.MIN_SAFE_INTEGER);
const ORACLE_PREVIEW_LITERAL_CHUNK_SIZE = 500;
function isOracleInvalidIdentifierError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false;
  }
  const message = error.message.toUpperCase();
  return (
    message.includes("ORA-00904") || message.includes("INVALID IDENTIFIER")
  );
}
function normalizeOracleNumberValue(
  value: unknown,
  metaData: Pick<oracledb.Metadata<unknown>, "scale">,
): string | number | null {
  if (value === null) {
    return null;
  }
  if (typeof value !== "string") {
    return typeof value === "number" && Number.isFinite(value) ? value : null;
  }
  const trimmed = value.trim();
  if (trimmed === "") {
    return value;
  }
  if (metaData.scale === 0 && /^[+-]?\d+$/.test(trimmed)) {
    try {
      const parsed = BigInt(trimmed);
      if (
        parsed >= ORACLE_MIN_SAFE_INTEGER &&
        parsed <= ORACLE_MAX_SAFE_INTEGER
      ) {
        return Number(trimmed);
      }
    } catch {
      return trimmed;
    }
  }
  if (typeof metaData.scale === "number" && metaData.scale > 0) {
    if (!/[eE]/.test(trimmed)) {
      const sign = /^[+-]/.test(trimmed) ? trimmed[0] : "";
      const unsigned = sign ? trimmed.slice(1) : trimmed;
      const [integerPart, fractionPart = ""] = unsigned.split(".");
      return `${sign}${integerPart === "" ? "0" : integerPart}.${fractionPart.padEnd(metaData.scale, "0")}`;
    }
  }
  return trimmed;
}
function oracleFloatPrecision(nativeType?: string): number | null {
  if (!nativeType) return null;
  const normalized = nativeType.toUpperCase().trim();
  if (normalized === "BINARY_FLOAT") return 7;
  if (normalized === "BINARY_DOUBLE") return 15;
  const match = /^FLOAT(?:\((\d+)\))?$/.exec(normalized);
  if (!match?.[1]) return null;
  const precision = Number.parseInt(match[1], 10);
  if (precision <= 24) return 7;
  if (precision <= 53) return 15;
  return null;
}
function normalizeOracleFloatValue(value: unknown): string | null {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return null;
  }
  return value.toString();
}
function oracleFloatFilterTolerance(nativeType?: string): number {
  const precision = oracleFloatPrecision(nativeType);
  if (precision === 7) return 1e-6;
  if (precision === 15) return 1e-12;
  return 1e-9;
}
function toOracleDdlText(value: string | Buffer | null): string | null {
  if (typeof value === "string") {
    const text = value.trim();
    return text ? text : null;
  }
  if (Buffer.isBuffer(value)) {
    const text = value.toString("utf8").trim();
    return text ? text : null;
  }
  return null;
}
function oracleTimestampPreviewLiteral(value: Date): string {
  const year = value.getUTCFullYear();
  const month = pad2(value.getUTCMonth() + 1);
  const day = pad2(value.getUTCDate());
  const hours = pad2(value.getUTCHours());
  const minutes = pad2(value.getUTCMinutes());
  const seconds = pad2(value.getUTCSeconds());
  const milliseconds = value.getUTCMilliseconds();
  const fraction =
    milliseconds > 0
      ? `.${String(milliseconds).padStart(3, "0").replace(/0+$/, "")}`
      : "";
  return `TIMESTAMP '${year}-${month}-${day} ${hours}:${minutes}:${seconds}${fraction}'`;
}
function oracleTimestampPreviewText(value: Date, useUtc: boolean): string {
  const year = useUtc ? value.getUTCFullYear() : value.getFullYear();
  const month = pad2((useUtc ? value.getUTCMonth() : value.getMonth()) + 1);
  const day = pad2(useUtc ? value.getUTCDate() : value.getDate());
  const hours = pad2(useUtc ? value.getUTCHours() : value.getHours());
  const minutes = pad2(useUtc ? value.getUTCMinutes() : value.getMinutes());
  const seconds = pad2(useUtc ? value.getUTCSeconds() : value.getSeconds());
  const milliseconds = useUtc
    ? value.getUTCMilliseconds()
    : value.getMilliseconds();
  const fraction =
    milliseconds > 0
      ? `.${String(milliseconds).padStart(3, "0").replace(/0+$/, "")}`
      : "";
  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}${fraction}`;
}
function splitOraclePreviewLiteral(value: string): string[] {
  const characters = Array.from(value);
  if (characters.length === 0) {
    return [""];
  }
  const chunks: string[] = [];
  for (
    let index = 0;
    index < characters.length;
    index += ORACLE_PREVIEW_LITERAL_CHUNK_SIZE
  ) {
    chunks.push(
      characters
        .slice(index, index + ORACLE_PREVIEW_LITERAL_CHUNK_SIZE)
        .join(""),
    );
  }
  return chunks;
}
function formatOracleLobPreviewExpression(
  value: string,
  options: {
    converter: "TO_CLOB" | "TO_NCLOB";
    useNationalCharacterSet?: boolean;
  },
): string {
  return splitOraclePreviewLiteral(value)
    .map(
      (chunk) =>
        `${options.converter}(${formatSqlPreviewStringLiteral(chunk, options.useNationalCharacterSet ? "N" : "")})`,
    )
    .join(" || ");
}
function formatOracleXmlPreviewExpression(value: string): string {
  return `XMLTYPE(${formatOracleLobPreviewExpression(value, { converter: "TO_CLOB" })})`;
}
function isUnavailableOracleDdl(ddl: string): boolean {
  return ddl.startsWith("-- ");
}
function readOracleIntervalPart(value: unknown, key: string): number | null {
  if (!value || typeof value !== "object") return null;
  const part = (value as Record<string, unknown>)[key];
  return typeof part === "number" && Number.isInteger(part) ? part : null;
}
function normalizeOracleIntervalYMValue(value: unknown): string | null {
  const years = readOracleIntervalPart(value, "years");
  const months = readOracleIntervalPart(value, "months");
  if (years === null || months === null) return null;
  const totalMonths = years * 12 + months;
  const negative = totalMonths < 0;
  const absMonths = Math.abs(totalMonths);
  const normalizedYears = Math.floor(absMonths / 12);
  const normalizedMonths = absMonths % 12;
  return `${negative ? "-" : ""}${normalizedYears}-${pad2(normalizedMonths)}`;
}
function normalizeOracleIntervalDSValue(value: unknown): string | null {
  const days = readOracleIntervalPart(value, "days");
  const hours = readOracleIntervalPart(value, "hours");
  const minutes = readOracleIntervalPart(value, "minutes");
  const seconds = readOracleIntervalPart(value, "seconds");
  const fseconds = readOracleIntervalPart(value, "fseconds");
  if (
    days === null ||
    hours === null ||
    minutes === null ||
    seconds === null ||
    fseconds === null
  ) {
    return null;
  }
  const totalNanos =
    BigInt(days) * ORACLE_INTERVAL_DS_NANOS_PER_DAY +
    BigInt(hours) * ORACLE_INTERVAL_DS_NANOS_PER_HOUR +
    BigInt(minutes) * ORACLE_INTERVAL_DS_NANOS_PER_MINUTE +
    BigInt(seconds) * ORACLE_INTERVAL_DS_NANOS_PER_SECOND +
    BigInt(fseconds);
  const negative = totalNanos < 0n;
  let remaining = negative ? -totalNanos : totalNanos;
  const normalizedDays = remaining / ORACLE_INTERVAL_DS_NANOS_PER_DAY;
  remaining %= ORACLE_INTERVAL_DS_NANOS_PER_DAY;
  const normalizedHours = remaining / ORACLE_INTERVAL_DS_NANOS_PER_HOUR;
  remaining %= ORACLE_INTERVAL_DS_NANOS_PER_HOUR;
  const normalizedMinutes = remaining / ORACLE_INTERVAL_DS_NANOS_PER_MINUTE;
  remaining %= ORACLE_INTERVAL_DS_NANOS_PER_MINUTE;
  const normalizedSeconds = remaining / ORACLE_INTERVAL_DS_NANOS_PER_SECOND;
  const normalizedFseconds = remaining % ORACLE_INTERVAL_DS_NANOS_PER_SECOND;
  const frac =
    normalizedFseconds > 0n
      ? `.${normalizedFseconds.toString().padStart(9, "0").replace(/0+$/, "")}`
      : "";
  return (
    `${negative ? "-" : ""}${normalizedDays.toString()} ` +
    `${pad2(Number(normalizedHours))}:${pad2(Number(normalizedMinutes))}:` +
    `${pad2(Number(normalizedSeconds))}${frac}`
  );
}
function normalizeOracleIntervalValue(value: unknown): string | null {
  return (
    normalizeOracleIntervalYMValue(value) ??
    normalizeOracleIntervalDSValue(value)
  );
}
function normalizeOracleXmlValue(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  return trimmed.replace(/>\s+</g, "><");
}
function oracleXmlFilterExpr(column: ColumnTypeMeta): string {
  const col = `"${column.name.replace(/"/g, '""')}"`;
  return `REGEXP_REPLACE(XMLSERIALIZE(CONTENT ${col} AS CLOB), '>\\s+<', '><')`;
}
type OracleLobLike = {
  getData?: () => Promise<string | Buffer | null>;
  destroy?: () => void;
  close?: () => Promise<void>;
};
function isOracleLobLike(value: unknown): value is OracleLobLike {
  return !!value && typeof value === "object";
}
function oracleErrorMessage(error: unknown): string {
  if (error instanceof Error && error.message) {
    return error.message;
  }
  return String(error);
}

type OracleStructuredConstraintKind = "json" | "xml";

function parseOracleStructuredConstraintKind(
  expression: string | null | undefined,
): OracleStructuredConstraintKind | null {
  if (!expression) {
    return null;
  }
  const normalized = expression.toUpperCase();
  if (/\bIS\s+JSON\b/.test(normalized)) {
    return "json";
  }
  if (/\bIS\s+XML\b/.test(normalized)) {
    return "xml";
  }
  return null;
}

function parseOracleRoutineIdentity(value: string | undefined): {
  kind: "procedure" | "package";
} | null {
  if (!value) {
    return null;
  }

  const match = /^oracle:(procedure|package)$/i.exec(value.trim());
  if (!match) {
    return null;
  }

  return {
    kind: match[1].toLowerCase() as "procedure" | "package",
  };
}
function replacePositionalParams(
  sql: string,
  params: unknown[],
): {
  sql: string;
  binds: unknown[];
} {
  let resultSql = "";
  let idx = 0;
  let i = 0;
  const len = sql.length;
  while (i < len) {
    const char = sql[i];
    const nextChar = sql[i + 1];
    const alternativeEnd = oracleAlternativeQuoteEnd(sql, i);
    if (alternativeEnd !== undefined) {
      resultSql += sql.slice(i, alternativeEnd);
      i = alternativeEnd;
      continue;
    }
    if (char === '"') {
      let end = i + 1;
      while (end < len) {
        if (sql[end++] === '"') {
          if (sql[end] !== '"') break;
          end++;
        }
      }
      resultSql += sql.slice(i, end);
      i = end;
      continue;
    }
    if (char === "'") {
      resultSql += char;
      i++;
      while (i < len) {
        if (sql[i] === "'" && sql[i + 1] === "'") {
          resultSql += "''";
          i += 2;
        } else if (sql[i] === "'") {
          resultSql += "'";
          i++;
          break;
        } else {
          resultSql += sql[i];
          i++;
        }
      }
      continue;
    }
    if (char === "/" && nextChar === "*") {
      const endIdx = sql.indexOf("*/", i + 2);
      if (endIdx === -1) {
        resultSql += sql.slice(i);
        i = len;
      } else {
        resultSql += sql.slice(i, endIdx + 2);
        i = endIdx + 2;
      }
      continue;
    }
    if (char === "-" && nextChar === "-") {
      const endIdx = sql.indexOf("\n", i + 2);
      if (endIdx === -1) {
        resultSql += sql.slice(i);
        i = len;
      } else {
        resultSql += sql.slice(i, endIdx + 1);
        i = endIdx + 1;
      }
      continue;
    }
    if (char === "?") {
      idx++;
      resultSql += `:${idx}`;
      i++;
      continue;
    }
    resultSql += char;
    i++;
  }
  if (idx === 0) {
    return { sql, binds: params };
  }
  if (idx !== params.length) {
    throw new Error(
      `[RapiDB] Oracle parameter mismatch: SQL has ${idx} placeholder(s) but ${params.length} value(s) were supplied.`,
    );
  }
  return { sql: resultSql, binds: params };
}
function parseOracleNumberScale(nativeType: string): number | null {
  const match = /^NUMBER\s*\(\s*\d+\s*,\s*(-?\d+)\s*\)$/i.exec(
    nativeType.trim(),
  );
  if (!match) return null;
  return Number(match[1]);
}
function oracleTypeName(nativeType: string): string {
  return nativeType
    .toUpperCase()
    .replace(/\s+IS\s+(?:JSON|XML)\s*$/, "")
    .replace(/\(\s*\d+(?:\s*,\s*-?\d+)?\s*\)/g, "")
    .replace(/\s+/g, " ")
    .trim();
}
function oracleObjectJsonReturnType(nativeType: string): string {
  const type = oracleTypeName(nativeType);
  if (
    [
      "NUMBER",
      "INTEGER",
      "SMALLINT",
      "PLS_INTEGER",
      "BINARY_INTEGER",
      "FLOAT",
      "BINARY_FLOAT",
      "BINARY_DOUBLE",
    ].includes(type)
  ) {
    return "NUMBER";
  }
  if (["CHAR", "NCHAR", "VARCHAR2", "NVARCHAR2"].includes(type)) {
    return "VARCHAR2(4000)";
  }
  throw new TypeError(
    `Oracle object attribute type "${nativeType}" is not supported for editing yet.`,
  );
}
function oracleObjectJsonPath(attributeName: string): string {
  const escapedName = attributeName.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
  return `'$."${escapedName.replace(/'/g, "''")}"'`;
}
function oracleObjectJsonAttributeExpr(
  paramIndex: number,
  attribute: NonNullable<ColumnMeta["objectAttributes"]>[number],
): string {
  const returnType = oracleObjectJsonReturnType(attribute.nativeType);
  return `JSON_VALUE(:${paramIndex}, ${oracleObjectJsonPath(attribute.name)} RETURNING ${returnType})`;
}
function parseOracleObjectRecord(
  value: unknown,
  attributes: NonNullable<ColumnMeta["objectAttributes"]>,
  columnName: string,
): Record<string, unknown> {
  let parsed: unknown = value;
  if (typeof parsed === "string") {
    try {
      JSON.parse(parsed);
      parsed = parseJsonPreservingRawNumbers(parsed);
    } catch {
      throw new TypeError(
        `Column "${columnName}" expects a valid JSON object value.`,
      );
    }
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new TypeError(
      `Column "${columnName}" expects a JSON object with its declared Oracle attributes.`,
    );
  }
  const record = parsed as Record<string, unknown>;
  const expectedNames = attributes.map((attribute) => attribute.name);
  const actualNames = Object.keys(record);
  if (
    expectedNames.length === 0 ||
    actualNames.length !== expectedNames.length ||
    expectedNames.some((name) => !Object.hasOwn(record, name))
  ) {
    throw new TypeError(
      `Column "${columnName}" requires exactly these Oracle object attributes: ${expectedNames.join(", ")}.`,
    );
  }
  const normalized: Record<string, unknown> = Object.create(null);
  for (const attribute of attributes) {
    const rawEntry = record[attribute.name];
    const entry = isRawNumberToken(rawEntry) ? rawEntry.raw : rawEntry;
    const type = oracleTypeName(attribute.nativeType);
    oracleObjectJsonReturnType(attribute.nativeType);
    if (entry !== null) {
      const isNumeric = [
        "NUMBER",
        "INTEGER",
        "SMALLINT",
        "PLS_INTEGER",
        "BINARY_INTEGER",
        "FLOAT",
        "BINARY_FLOAT",
        "BINARY_DOUBLE",
      ].includes(type);
      const valid = isNumeric
        ? (typeof entry === "number" &&
            Number.isFinite(entry) &&
            (!Number.isInteger(entry) || Number.isSafeInteger(entry))) ||
          (typeof entry === "string" &&
            /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?$/i.test(
              entry.trim(),
            ) &&
            Number.isFinite(Number(entry)))
        : typeof entry === "string";
      if (!valid) {
        throw new TypeError(
          `Attribute "${attribute.name}" in column "${columnName}" has an invalid ${attribute.nativeType} value.`,
        );
      }
    }
    normalized[attribute.name] = entry;
  }
  return normalized;
}
function canonicalOracleObjectNumber(value: number | string): string {
  const text = String(value).trim();
  const match = /^([+-]?)(\d*)(?:\.(\d*))?(?:[eE]([+-]?\d+))?$/.exec(text);
  if (!match) throw new TypeError("Invalid Oracle object NUMBER value.");
  const sign = match[1] === "-" ? "-" : "";
  const integer = match[2] || "0";
  const fraction = match[3] ?? "";
  const exponent = Number(match[4] ?? 0);
  if (!Number.isSafeInteger(exponent)) {
    throw new TypeError("Oracle object NUMBER exponent is out of range.");
  }
  let digits = `${integer}${fraction}`.replace(/^0+/, "");
  if (digits === "") return "0e0";
  let scale = fraction.length - exponent;
  while (digits.endsWith("0")) {
    digits = digits.slice(0, -1);
    scale--;
  }
  return `${sign}${digits}e${-scale}`;
}
function canonicalOracleObjectValue(
  value: unknown,
  column: ColumnTypeMeta,
): string {
  const record = parseOracleObjectRecord(
    value,
    column.objectAttributes ?? [],
    column.name,
  );
  return JSON.stringify(
    (column.objectAttributes ?? []).map((attribute) => {
      const entry = record[attribute.name];
      if (entry === null) return [attribute.name, null];
      const type = oracleTypeName(attribute.nativeType);
      const isNumeric = [
        "NUMBER",
        "INTEGER",
        "SMALLINT",
        "PLS_INTEGER",
        "BINARY_INTEGER",
        "FLOAT",
        "BINARY_FLOAT",
        "BINARY_DOUBLE",
      ].includes(type);
      return [
        attribute.name,
        isNumeric
          ? `NUMBER:${canonicalOracleObjectNumber(entry as number | string)}`
          : `STRING:${String(entry)}`,
      ];
    }),
  );
}
const ORACLE_TEXT_ELEMENT_TYPES = ["CHAR", "NCHAR", "VARCHAR2", "NVARCHAR2"];
function parseOracleVarrayTextValues(
  value: unknown,
  column: ColumnTypeMeta,
): Array<string | null> {
  let parsed: unknown = value;
  if (typeof parsed === "string") {
    try {
      parsed = JSON.parse(parsed);
    } catch {
      throw new TypeError(
        `Column "${column.name}" expects a valid JSON array value.`,
      );
    }
  }
  if (!Array.isArray(parsed)) {
    throw new TypeError(
      `Column "${column.name}" expects a JSON array for its Oracle VARRAY value.`,
    );
  }
  if (
    !ORACLE_TEXT_ELEMENT_TYPES.includes(
      oracleTypeName(column.collectionElementType ?? ""),
    )
  ) {
    throw new TypeError(
      `Oracle VARRAY editing currently supports character elements only; "${column.collectionElementType ?? "unknown"}" is not supported.`,
    );
  }
  return parsed.map((entry) => {
    if (entry === null || entry === "") return null;
    if (typeof entry !== "string") {
      throw new TypeError(
        `Every element in Oracle VARRAY column "${column.name}" must be a string or null.`,
      );
    }
    return entry;
  });
}
function canonicalOracleVarrayTextValue(
  value: unknown,
  column: ColumnTypeMeta,
): string {
  return JSON.stringify(parseOracleVarrayTextValues(value, column));
}
function canonicalOracleJsonObject(value: unknown): string | null {
  try {
    const parsed =
      typeof value === "string"
        ? JSON.parse(value)
        : JSON.parse(JSON.stringify(value));
    if (
      parsed === null ||
      typeof parsed !== "object" ||
      Array.isArray(parsed)
    ) {
      return null;
    }
    const sortKeys = (entry: unknown): unknown => {
      if (Array.isArray(entry)) return entry.map(sortKeys);
      if (entry === null || typeof entry !== "object") return entry;
      return Object.fromEntries(
        Object.keys(entry)
          .sort()
          .map((key) => [
            key,
            sortKeys((entry as Record<string, unknown>)[key]),
          ]),
      );
    };
    return JSON.stringify(sortKeys(parsed));
  } catch {
    return null;
  }
}
function oracleObjectJsonValue(value: unknown, column: ColumnTypeMeta): string {
  if (value === NULL_SENTINEL || value === null) {
    return JSON.stringify(null);
  }
  const normalized = parseOracleObjectRecord(
    value,
    column.objectAttributes ?? [],
    column.name,
  );
  return JSON.stringify(normalized);
}
function isOracleIntervalType(nativeType: string): boolean {
  return oracleTypeName(nativeType).startsWith("INTERVAL ");
}
function isTimezoneAwareOracleTemporal(nativeType: string): boolean {
  const typeName = oracleTypeName(nativeType);
  return (
    typeName === "TIMESTAMP WITH TIME ZONE" ||
    typeName === "TIMESTAMP WITH LOCAL TIME ZONE"
  );
}
function parseOracleTemporalInput(
  value: string,
  options: {
    assumeUtcWhenMissingTimezone?: boolean;
  } = {},
): Date | null {
  const trimmed = value.trim();
  const match =
    /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2}(?:\.\d+)?)(?: ?(Z|[+-]\d{2}(?::\d{2})?))?$/i.exec(
      trimmed,
    );
  if (!match) return null;
  const [, date, time, rawTz] = match;
  if (!isValidDateOnly(date) || !isValidTimeValue(time)) {
    return null;
  }
  const tz = !rawTz
    ? options.assumeUtcWhenMissingTimezone === false
      ? ""
      : "Z"
    : rawTz === "Z"
      ? "Z"
      : rawTz.includes(":")
        ? rawTz
        : `${rawTz}:00`;
  const parsed = new Date(`${date}T${time}${tz}`);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}
function normalizeOracleTemporalText(value: string): {
  text: string;
  hasExplicitTimezone: boolean;
  timezone: string | null;
} | null {
  const trimmed = value.trim();
  const match =
    /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(\.\d+)?(?: ?(Z|[+-]\d{2}(?::\d{2})?))?$/i.exec(
      trimmed,
    );
  if (!match) return null;
  const [, date, time, rawFrac, rawTimezone] = match;
  if (!rawFrac || rawFrac.length <= 1) {
    return {
      text: `${date} ${time}`,
      hasExplicitTimezone: !!rawTimezone,
      timezone: rawTimezone ?? null,
    };
  }
  const digits = rawFrac.slice(1).replace(/0+$/, "");
  const frac = digits.length > 0 ? `.${digits}` : "";
  return {
    text: `${date} ${time}${frac}`,
    hasExplicitTimezone: !!rawTimezone,
    timezone: rawTimezone ?? null,
  };
}
function stripOracleTemporalTimezone(value: string): string {
  return value.replace(/[ ]?(Z|[+-]\d{2}(?::\d{2})?)$/i, "").trim();
}
function trimOracleTemporalFraction(value: string): string {
  return value.replace(/(\.\d*?[1-9])0+$/, "$1").replace(/\.0+$/, "");
}
function formatOracleWallClockDate(value: unknown): string | null {
  if (!(value instanceof Date) || Number.isNaN(value.getTime())) return null;
  const ms = value.getMilliseconds();
  const frac =
    ms > 0 ? `.${String(ms).padStart(3, "0").replace(/0+$/, "")}` : "";
  return (
    `${value.getFullYear()}-${pad2(value.getMonth() + 1)}-${pad2(value.getDate())} ` +
    `${pad2(value.getHours())}:${pad2(value.getMinutes())}:${pad2(value.getSeconds())}${frac}`
  );
}
function isValidDateOnly(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const parsed = new Date(`${value}T00:00:00Z`);
  if (Number.isNaN(parsed.getTime())) return false;
  const [year, month, day] = value.split("-").map(Number);
  return (
    parsed.getUTCFullYear() === year &&
    parsed.getUTCMonth() + 1 === month &&
    parsed.getUTCDate() === day
  );
}
function isValidTimeValue(value: string): boolean {
  const match = /^(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?$/.exec(value);
  if (!match) return false;
  const [, rawHours, rawMinutes, rawSeconds] = match;
  const hours = Number(rawHours);
  const minutes = Number(rawMinutes);
  const seconds = Number(rawSeconds);
  return hours < 24 && minutes < 60 && seconds < 60;
}
function normalizeOracleTemporalValue(
  value: unknown,
  options: {
    preserveExplicitTimezoneText?: boolean;
    includeExplicitTimezone?: boolean;
  } = {},
): string | null {
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) return null;
    return trimOracleTemporalFraction(
      stripOracleTemporalTimezone(formatDatetimeForDisplay(value) ?? ""),
    );
  }
  if (typeof value === "string") {
    const normalizedText = normalizeOracleTemporalText(value);
    if (
      normalizedText &&
      (!normalizedText.hasExplicitTimezone ||
        options.preserveExplicitTimezoneText)
    ) {
      const timezone =
        options.includeExplicitTimezone && normalizedText.timezone
          ? ` ${normalizedText.timezone}`
          : "";
      return `${normalizedText.text}${timezone}`;
    }
    if (normalizedText?.hasExplicitTimezone) {
      const [date, time] = normalizedText.text.split(" ");
      const [seconds, fraction] = time.split(".");
      const instant = parseOracleTemporalInput(
        `${date} ${seconds} ${normalizedText.timezone}`,
      );
      if (instant) {
        return `${instant.toISOString().slice(0, 19).replace("T", " ")}${fraction ? `.${fraction}` : ""}`;
      }
    }
    const parsed = parseOracleTemporalInput(value);
    if (parsed) {
      const formatted = formatDatetimeForDisplay(parsed);
      return formatted
        ? trimOracleTemporalFraction(stripOracleTemporalTimezone(formatted))
        : null;
    }
    const formatted = formatDatetimeForDisplay(value);
    if (formatted)
      return trimOracleTemporalFraction(stripOracleTemporalTimezone(formatted));
    if (DATETIME_SQL_RE.test(value.trim())) {
      return trimOracleTemporalFraction(
        stripOracleTemporalTimezone(value.trim()),
      );
    }
  }
  return null;
}
interface OracleTemporalQueryProjection {
  sql: string;
  columnNames: Map<string, string>;
}
function oracleSelectCandidateKeyword(sql: string): "SELECT" | "WITH" | null {
  const keyword =
    /^(?:\uFEFF?\s+|--[^\r\n]*(?:\r\n|\r|\n|$)|\/\*[\s\S]*?\*\/)*(SELECT|WITH)\b/i
      .exec(sql)?.[1]
      ?.toUpperCase();
  return keyword === "SELECT" || keyword === "WITH" ? keyword : null;
}
function oracleTemporalProjectionExpression(
  column: Pick<oracledb.Metadata<unknown>, "name" | "dbType">,
  source: string,
): string | null {
  const value = `${source}."${column.name.replace(/"/g, '""')}"`;
  if (column.dbType === oracledb.DB_TYPE_OBJECT) {
    return `CASE WHEN ${value} IS NULL THEN NULL ELSE JSON_OBJECT(${value} RETURNING CLOB) END`;
  }
  if (column.dbType === oracledb.DB_TYPE_DATE) {
    return `TO_CHAR(${value}, 'YYYY-MM-DD HH24:MI:SS')`;
  }
  if (column.dbType === oracledb.DB_TYPE_TIMESTAMP) {
    return `TO_CHAR(${value}, 'YYYY-MM-DD HH24:MI:SS.FF9')`;
  }
  if (column.dbType === oracledb.DB_TYPE_TIMESTAMP_TZ) {
    return `TO_CHAR(${value}, 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM')`;
  }
  if (column.dbType === oracledb.DB_TYPE_TIMESTAMP_LTZ) {
    return `CASE WHEN ${value} IS NULL THEN NULL ELSE TO_CHAR(${value} AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.FF9') || 'Z' END`;
  }
  return null;
}
function oracleTemporalFilterExpr(column: ColumnTypeMeta): string {
  const typeName = oracleTypeName(column.nativeType);
  const col = `"${column.name.replace(/"/g, '""')}"`;
  if (typeName === "DATE") {
    return `TO_CHAR(${col}, 'YYYY-MM-DD HH24:MI:SS')`;
  }
  if (isTimezoneAwareOracleTemporal(column.nativeType)) {
    return `RTRIM(RTRIM(TO_CHAR(SYS_EXTRACT_UTC(CAST(${col} AS TIMESTAMP(9) WITH TIME ZONE)), 'YYYY-MM-DD HH24:MI:SS.FF9'), '0'), '.')`;
  }
  return `RTRIM(RTRIM(TO_CHAR(${col}, 'YYYY-MM-DD HH24:MI:SS.FF9'), '0'), '.')`;
}

function oracleTemporalBindText(
  value: unknown,
  column: ColumnTypeMeta,
): string | null {
  const timezoneAware = isTimezoneAwareOracleTemporal(column.nativeType);
  let text =
    typeof value === "string"
      ? value.trim()
      : value instanceof Date && !Number.isNaN(value.getTime())
        ? `${oracleTimestampPreviewText(value, timezoneAware)}${timezoneAware ? " +00:00" : ""}`
        : "";
  if (isValidDateOnly(text)) text += " 00:00:00";
  const normalized = normalizeOracleTemporalText(text);
  if (!normalized || !parseOracleTemporalInput(text)) return null;
  if (!timezoneAware) return normalized.text;
  let timezone = normalized.timezone ?? "+00:00";
  if (timezone.toUpperCase() === "Z") timezone = "+00:00";
  else if (!timezone.includes(":")) timezone += ":00";
  return `${normalized.text} ${timezone}`;
}

function canonicalOracleTemporalEdit(
  value: unknown,
  column: ColumnTypeMeta,
): string | null {
  if (value === null || value === NULL_SENTINEL || value === "")
    return "__rapidb_oracle_temporal_null__";
  const text = oracleTemporalBindText(value, column);
  if (text === null) return null;
  const normalized = normalizeOracleTemporalText(text);
  if (!normalized) return null;
  if (!isTimezoneAwareOracleTemporal(column.nativeType)) return normalized.text;
  const [date, time] = normalized.text.split(" ");
  const [seconds, fraction = ""] = time.split(".");
  const instant = new Date(`${date}T${seconds}${normalized.timezone}`);
  if (Number.isNaN(instant.getTime())) return null;
  return `${instant.toISOString().slice(0, 19)}${fraction ? `.${fraction}` : ""}Z`;
}
function formatOracleQueryValue(
  value: unknown,
  meta: Pick<oracledb.Metadata<unknown>, "dbType">,
): unknown {
  if (meta.dbType === oracledb.DB_TYPE_BINARY_FLOAT) {
    return normalizeOracleFloatValue(value) ?? value;
  }
  if (meta.dbType === oracledb.DB_TYPE_BINARY_DOUBLE) {
    return normalizeOracleFloatValue(value) ?? value;
  }
  if (meta.dbType === oracledb.DB_TYPE_INTERVAL_YM) {
    return normalizeOracleIntervalYMValue(value) ?? value;
  }
  if (meta.dbType === oracledb.DB_TYPE_INTERVAL_DS) {
    return normalizeOracleIntervalDSValue(value) ?? value;
  }
  if (!(value instanceof Date) || Number.isNaN(value.getTime())) {
    return value;
  }
  if (meta.dbType === oracledb.DB_TYPE_DATE) {
    return formatOracleWallClockDate(value) ?? value;
  }
  if (meta.dbType === oracledb.DB_TYPE_TIMESTAMP) {
    return formatOracleWallClockDate(value) ?? value;
  }
  if (
    meta.dbType === oracledb.DB_TYPE_TIMESTAMP_TZ ||
    meta.dbType === oracledb.DB_TYPE_TIMESTAMP_LTZ
  ) {
    const formatted = normalizeOracleTemporalValue(value);
    return typeof formatted === "string"
      ? `${formatted}Z`
      : (formatted ?? value);
  }
  return value;
}
function isPLSQLBlock(stmt: string): boolean {
  const s = stmt
    .replace(/'(?:[^']|'')*'/g, "''")
    .replace(/\/\*[\s\S]*?\*\//g, " ")
    .replace(/--[^\n]*/g, " ")
    .trimStart()
    .toUpperCase();
  return (
    /^CREATE\s+(OR\s+REPLACE\s+)?(EDITIONABLE\s+|NONEDITIONABLE\s+)?(FUNCTION|PROCEDURE|PACKAGE|TRIGGER|TYPE)\b/.test(
      s,
    ) ||
    /^DECLARE\b/.test(s) ||
    /^BEGIN\b/.test(s)
  );
}
export function splitOracleStatements(src: string): string[] {
  const stmts: string[] = [];
  let cur = "";
  let i = 0;
  const len = src.length;
  while (i < len) {
    if (src[i] === "/" && src[i + 1] === "*") {
      const end = src.indexOf("*/", i + 2);
      if (end === -1) {
        cur += src.slice(i);
        i = len;
      } else {
        cur += src.slice(i, end + 2);
        i = end + 2;
      }
      continue;
    }
    if (src[i] === "-" && src[i + 1] === "-") {
      const end = src.indexOf("\n", i + 2);
      if (end === -1) {
        cur += src.slice(i);
        i = len;
      } else {
        cur += src.slice(i, end + 1);
        i = end + 1;
      }
      continue;
    }
    const alternativeEnd = oracleAlternativeQuoteEnd(src, i);
    if (alternativeEnd !== undefined) {
      cur += src.slice(i, alternativeEnd);
      i = alternativeEnd;
      continue;
    }
    if (src[i] === "'" || src[i] === '"') {
      const quote = src[i];
      let j = i + 1;
      while (j < len) {
        if (src[j] === quote && src[j + 1] === quote) {
          j += 2;
        } else if (src[j] === quote) {
          j++;
          break;
        } else {
          j++;
        }
      }
      cur += src.slice(i, j);
      i = j;
      continue;
    }
    if (src[i] === ";") {
      if (isPLSQLBlock(cur)) {
        cur += src[i];
      } else {
        const stmt = cur.trim();
        if (stmt) stmts.push(stmt);
        cur = "";
      }
      i++;
      continue;
    }
    if (src[i] === "/") {
      const lineStart = src.lastIndexOf("\n", i - 1) + 1;
      const beforeSlash = src.slice(lineStart, i);
      const afterSlash = src.slice(i + 1).match(/^[ \t]*([\r\n]|$)/);
      if (beforeSlash.trim() === "" && afterSlash) {
        const stmt = cur.trim();
        if (stmt) stmts.push(stmt);
        cur = "";
        i++;
        while (i < len && src[i] !== "\n") i++;
        if (i < len) i++;
        continue;
      }
    }
    cur += src[i];
    i++;
  }
  const last = cur.trim();
  if (last) stmts.push(last);
  return stmts.filter((s) => s.length > 0);
}
const ORACLE_FILTER_DENYLIST = new Set([
  "clob",
  "nclob",
  "bfile",
  "long raw",
  "long",
  "anydata",
  "anytype",
]);
interface OracleQueryOperation {
  requestToken?: number;
  connection?: oracledb.Connection;
  cancelled: boolean;
}
export class OracleDriver extends BaseDBDriver {
  override getCapabilities() {
    return { ...super.getCapabilities(), boundedQueryResults: true };
  }
  protected override getQueryEditorSqlDialect() {
    return "plsql" as const;
  }

  private pool: oracledb.Pool | null = null;
  private connectionEpoch = 0;
  private connectionAttempt: Promise<void> | null = null;
  private publishedConnectionAttempt: Promise<void> | null = null;
  private readonly pendingPools = new Set<oracledb.Pool>();
  private readonly closingPools = new WeakMap<oracledb.Pool, Promise<void>>();
  private readonly config: ConnectionConfig;
  private readonly activeQueryOperations = new Set<OracleQueryOperation>();
  constructor(
    config: ConnectionConfig,
    timeoutSettingsProvider?: DriverTimeoutSettingsProvider,
  ) {
    super(timeoutSettingsProvider);
    this.config = config;
  }
  connect(): Promise<void> {
    if (this.connectionAttempt) return this.connectionAttempt;
    const epoch = ++this.connectionEpoch;
    this.publishedConnectionAttempt = null;
    const cleanup = this.closeOwnedPools();
    const attempt: Promise<void> = cleanup
      .then(() => this.openConnection(epoch, attempt))
      .finally(() => {
        if (this.connectionAttempt === attempt) this.connectionAttempt = null;
      });
    this.connectionAttempt = attempt;
    return attempt;
  }

  private assertConnectionEpoch(epoch: number): void {
    if (epoch !== this.connectionEpoch) {
      throw new DOMException(
        "Oracle connection attempt was cancelled.",
        "AbortError",
      );
    }
  }

  private async openConnection(
    epoch: number,
    attempt: Promise<void>,
  ): Promise<void> {
    this.assertConnectionEpoch(epoch);
    const serviceName = this.config.serviceName || this.config.database;
    const forwardedTransport = getSshTcpForwardTransport(this.config);
    const host =
      forwardedTransport?.localHost ?? this.config.host ?? "localhost";
    const port = forwardedTransport?.localPort ?? this.config.port ?? 1521;
    const connectString = serviceName
      ? `${host}:${port}/${serviceName}`
      : `${host}:${port}`;
    const pool = (await oracledb.createPool({
      user: this.config.username ?? "",
      password: this.config.password ?? "",
      connectString,
      connectTimeout: this.getTimeoutSettings().connectionTimeoutSeconds,
      transportConnectTimeout:
        this.getTimeoutSettings().connectionTimeoutSeconds,
      poolMin: 1,
      poolMax: 5,
      poolIncrement: 1,
      poolTimeout: 30,
      poolPingInterval: 60,
    })) as unknown as oracledb.Pool;
    this.pendingPools.add(pool);
    try {
      this.assertConnectionEpoch(epoch);
      const conn = await pool.getConnection();
      let probeFailed = false;
      let probeError: unknown;
      try {
        this.assertConnectionEpoch(epoch);
        conn.callTimeout = this.getDbOperationTimeoutMs();
        await conn.ping();
      } catch (error) {
        probeFailed = true;
        probeError = error;
      }
      try {
        await conn.close();
      } catch (closeError) {
        if (!probeFailed) throw closeError;
      }
      if (probeFailed) throw probeError;
      this.assertConnectionEpoch(epoch);
    } catch (error) {
      await this.closePool(pool);
      throw error;
    } finally {
      this.pendingPools.delete(pool);
    }
    this.pool = pool;
    this.publishedConnectionAttempt = attempt;
  }

  private closePool(pool: oracledb.Pool): Promise<void> {
    let closing = this.closingPools.get(pool);
    if (!closing) {
      closing = Promise.resolve()
        .then(() => pool.close(0))
        .catch(() => undefined);
      this.closingPools.set(pool, closing);
    }
    return closing;
  }

  private async closeOwnedPools(): Promise<void> {
    const pools = new Set(this.pendingPools);
    if (this.pool) pools.add(this.pool);
    this.pool = null;
    this.pendingPools.clear();
    await Promise.all([...pools].map((pool) => this.closePool(pool)));
  }

  async cancelConnectionAttempt(attempt: Promise<unknown>): Promise<void> {
    if (
      attempt === this.connectionAttempt ||
      attempt === this.publishedConnectionAttempt
    ) {
      await this.disconnect();
    }
  }

  async disconnect(): Promise<void> {
    ++this.connectionEpoch;
    this.connectionAttempt = null;
    this.publishedConnectionAttempt = null;
    await this.closeOwnedPools();
  }
  async cancelCurrentOperation(
    context?: OperationCancellationContext,
  ): Promise<void> {
    if (
      context?.operationName !== "query" ||
      context.requestToken === undefined
    ) {
      return;
    }
    await Promise.all(
      [...this.activeQueryOperations].map(async (operation) => {
        if (operation.requestToken !== context.requestToken) {
          return;
        }
        operation.cancelled = true;
        await operation.connection?.break().catch(() => undefined);
      }),
    );
  }
  isConnected(): boolean {
    if (!this.pool) {
      return false;
    }
    return this.pool.connectionsInUse > 0 || this.pool.connectionsOpen > 0;
  }

  getEntityManifest(): DriverEntityManifest {
    return ORACLE_ENTITY_MANIFEST;
  }

  private async getConnection(): Promise<oracledb.Connection> {
    const metadata = getMetadataContext();
    if (!this.pool) {
      throw new Error("[RapiDB] Oracle connection pool is not initialized.");
    }
    let connection = await this.pool.getConnection();
    if (metadata) {
      if (metadata.signal.aborted) {
        await connection.close().catch(() => undefined);
        metadata.signal.throwIfAborted();
      }
      connection = new Proxy(connection, {
        get(target, property) {
          if (property === "execute")
            return (...args: unknown[]) =>
              withMetadataResource(
                () => target.break(),
                () => Reflect.apply(target.execute, target, args),
              );
          const value = Reflect.get(target, property, target);
          return typeof value === "function" ? value.bind(target) : value;
        },
      });
    }
    connection.callTimeout = this.getDbOperationTimeoutMs();
    try {
      await connection.execute(
        "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6'",
      );
      await connection.execute(
        "ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM'",
      );
    } catch {}
    return connection;
  }

  private async readStructuredConstraintKinds(
    conn: oracledb.Connection,
    schema: string,
    table: string,
  ): Promise<Map<string, OracleStructuredConstraintKind>> {
    const result = new Map<string, OracleStructuredConstraintKind>();
    try {
      const res = await conn.execute<{
        COLUMN_NAME: string;
        SEARCH_CONDITION_VC: string | null;
      }>(
        `SELECT cols.column_name, cons.search_condition_vc
         FROM all_constraints cons
         JOIN all_cons_columns cols
           ON cons.constraint_name = cols.constraint_name
          AND cons.owner = cols.owner
         WHERE cons.owner = :1
           AND cons.table_name = :2
           AND cons.constraint_type = 'C'
           AND cons.search_condition_vc IS NOT NULL`,
        [schema.toUpperCase(), table.toUpperCase()],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );

      for (const row of res.rows ?? []) {
        const kind = parseOracleStructuredConstraintKind(
          row.SEARCH_CONDITION_VC,
        );
        if (!kind) {
          continue;
        }
        const existing = result.get(row.COLUMN_NAME);
        if (existing === "json") {
          continue;
        }
        result.set(row.COLUMN_NAME, kind);
      }
    } catch {}
    return result;
  }
  async listDatabases(): Promise<DatabaseInfo[]> {
    const conn = await this.getConnection();
    try {
      const res = await conn.execute<{
        ORA_DATABASE_NAME: string;
      }>(
        `SELECT ora_database_name FROM dual`,
        {},
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );
      const name =
        res.rows?.[0]?.ORA_DATABASE_NAME ??
        this.config.serviceName ??
        this.config.database ??
        "ORACLE";
      return [{ name, schemas: [] }];
    } catch {
      return [
        {
          name: this.config.serviceName ?? this.config.database ?? "ORACLE",
          schemas: [],
        },
      ];
    } finally {
      await conn.close();
    }
  }
  async listSchemas(_database: string): Promise<SchemaInfo[]> {
    const conn = await this.getConnection();
    try {
      const res = await conn.execute<{
        OWNER: string;
      }>(
        `SELECT username AS owner
        FROM all_users
        WHERE username = sys_context('USERENV','SESSION_USER')
           OR oracle_maintained = 'N'
        ORDER BY CASE
                   WHEN username = sys_context('USERENV','SESSION_USER') THEN 0
                   ELSE 1
                 END,
                 username`,
        {},
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );
      return (res.rows ?? []).map((r) => ({ name: r.OWNER }));
    } catch {
      const res2 = await conn.execute<{
        USERNAME: string;
      }>(
        `SELECT sys_context('USERENV','SESSION_USER') AS username FROM dual`,
        {},
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );
      const owner = res2.rows?.[0]?.USERNAME ?? this.config.username ?? "";
      return owner ? [{ name: owner }] : [];
    } finally {
      await conn.close();
    }
  }
  async listObjects(_database: string, schema: string): Promise<TableInfo[]> {
    const conn = await this.getConnection();
    try {
      const objects: TableInfo[] = [];
      const tableRes = await conn.execute<{
        OBJECT_NAME: string;
        OBJECT_TYPE: string;
      }>(
        `SELECT object_name, object_type
         FROM all_objects
         WHERE owner = :1
           AND object_type IN ('TABLE','VIEW','MATERIALIZED VIEW','FUNCTION','PROCEDURE','PACKAGE','SEQUENCE','TYPE')
           AND status = 'VALID'
         ORDER BY object_type, object_name`,
        [schema.toUpperCase()],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );
      for (const r of tableRes.rows ?? []) {
        const rawType = r.OBJECT_TYPE.trim();
        let type: TableInfo["type"];
        let routineIdentity: string | undefined;
        if (rawType === "TABLE") {
          type = "table";
        } else if (rawType === "VIEW") {
          type = "view";
        } else if (rawType === "MATERIALIZED VIEW") {
          type = "materializedView";
        } else if (rawType === "FUNCTION") {
          type = "function";
        } else if (rawType === "PROCEDURE") {
          type = "procedure";
          routineIdentity = "oracle:procedure";
        } else if (rawType === "PACKAGE") {
          type = "procedure";
          routineIdentity = "oracle:package";
        } else if (rawType === "SEQUENCE") {
          type = "sequence";
        } else if (rawType === "TYPE") {
          type = "type";
        } else {
          continue;
        }
        objects.push({
          schema,
          name: r.OBJECT_NAME,
          type,
          ...(routineIdentity ? { routineIdentity } : {}),
        });
      }
      return objects;
    } finally {
      await conn.close();
    }
  }
  async describeTable(
    _database: string,
    schema: string,
    table: string,
  ): Promise<ColumnMeta[]> {
    const conn = await this.getConnection();
    try {
      const colRows = await this._readTableColumnsWithVirtualFallback(
        conn,
        schema,
        table,
      );
      const collectionByColumn = new Map<
        string,
        {
          kind: "varray" | "nestedTable";
          elementType: string;
          owner: string;
          name: string;
        }
      >();
      try {
        const collectionRes = await conn.execute<{
          COLUMN_NAME: string;
          COLL_TYPE: string;
          ELEM_TYPE_NAME: string;
          TYPE_OWNER: string;
          TYPE_NAME: string;
        }>(
          `SELECT tab.column_name,
                  coll.coll_type,
                  coll.elem_type_name,
                  tab.data_type_owner AS type_owner,
                  tab.data_type AS type_name
           FROM all_tab_columns tab
           JOIN all_coll_types coll
             ON coll.owner = tab.data_type_owner
            AND coll.type_name = tab.data_type
           WHERE tab.owner = :1 AND tab.table_name = :2`,
          [schema.toUpperCase(), table.toUpperCase()],
          { outFormat: oracledb.OUT_FORMAT_OBJECT },
        );
        for (const row of collectionRes.rows ?? []) {
          const kind = row.COLL_TYPE.trim().toUpperCase();
          if (kind !== "VARYING ARRAY" && kind !== "TABLE") continue;
          collectionByColumn.set(row.COLUMN_NAME, {
            kind: kind === "VARYING ARRAY" ? "varray" : "nestedTable",
            elementType: row.ELEM_TYPE_NAME.trim().toUpperCase(),
            owner: row.TYPE_OWNER,
            name: row.TYPE_NAME,
          });
        }
      } catch {}
      const objectByColumn = new Map<
        string,
        {
          owner: string;
          name: string;
          attributes: Array<{ name: string; nativeType: string }>;
        }
      >();
      try {
        const objectRes = await conn.execute<{
          COLUMN_NAME: string;
          TYPE_OWNER: string;
          TYPE_NAME: string;
          ATTR_NAME: string;
          ATTR_TYPE_NAME: string;
          ATTR_NO: number;
        }>(
          `SELECT tab.column_name,
                  tab.data_type_owner AS type_owner,
                  tab.data_type AS type_name,
                  attr.attr_name,
                  attr.attr_type_name,
                  attr.attr_no
           FROM all_tab_columns tab
           JOIN all_type_attrs attr
             ON attr.owner = tab.data_type_owner
            AND attr.type_name = tab.data_type
           WHERE tab.owner = :1 AND tab.table_name = :2
           ORDER BY tab.column_id, attr.attr_no`,
          [schema.toUpperCase(), table.toUpperCase()],
          { outFormat: oracledb.OUT_FORMAT_OBJECT },
        );
        for (const row of objectRes.rows ?? []) {
          let objectType = objectByColumn.get(row.COLUMN_NAME);
          if (!objectType) {
            objectType = {
              owner: row.TYPE_OWNER,
              name: row.TYPE_NAME,
              attributes: [],
            };
            objectByColumn.set(row.COLUMN_NAME, objectType);
          }
          objectType.attributes.push({
            name: row.ATTR_NAME,
            nativeType: row.ATTR_TYPE_NAME.trim().toUpperCase(),
          });
        }
      } catch {}
      const pkRes = await conn.execute<{
        COLUMN_NAME: string;
        POSITION: number;
      }>(
        `SELECT cols.column_name, cols.position
         FROM all_constraints cons
         JOIN all_cons_columns cols
           ON cons.constraint_name = cols.constraint_name
           AND cons.owner = cols.owner
         WHERE cons.constraint_type = 'P'
           AND cons.owner = :1
           AND cons.table_name = :2
         ORDER BY cols.position`,
        [schema.toUpperCase(), table.toUpperCase()],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );
      const pkOrdinalByColumn = new Map(
        (pkRes.rows ?? []).map((r) => [r.COLUMN_NAME, r.POSITION]),
      );
      const fkRes = await conn.execute<{
        COLUMN_NAME: string;
      }>(
        `SELECT cols.column_name
         FROM all_constraints cons
         JOIN all_cons_columns cols
           ON cons.constraint_name = cols.constraint_name
           AND cons.owner = cols.owner
         WHERE cons.constraint_type = 'R'
           AND cons.owner = :1
           AND cons.table_name = :2`,
        [schema.toUpperCase(), table.toUpperCase()],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );
      const fkCols = new Set((fkRes.rows ?? []).map((r) => r.COLUMN_NAME));
      const structuredConstraintKinds =
        await this.readStructuredConstraintKinds(conn, schema, table);
      const identityMap = new Map<
        string,
        "ALWAYS" | "BY DEFAULT" | "BY DEFAULT ON NULL"
      >();
      try {
        const idRes = await conn.execute<{
          COLUMN_NAME: string;
          GENERATION_TYPE: string;
        }>(
          `SELECT column_name, generation_type
           FROM all_tab_identity_cols
           WHERE owner = :1 AND table_name = :2`,
          [schema.toUpperCase(), table.toUpperCase()],
          { outFormat: oracledb.OUT_FORMAT_OBJECT },
        );
        for (const r of idRes.rows ?? []) {
          identityMap.set(
            r.COLUMN_NAME,
            (r.GENERATION_TYPE ?? "ALWAYS") as
              | "ALWAYS"
              | "BY DEFAULT"
              | "BY DEFAULT ON NULL",
          );
        }
      } catch {}
      return colRows.map((r) => {
        const genType = identityMap.get(r.COLUMN_NAME);
        const isComputed = r.VIRTUAL_COLUMN === "YES";
        const rawDefault = r.DATA_DEFAULT?.trim() ?? undefined;
        const computedExpression = isComputed ? rawDefault : undefined;
        const primaryKeyOrdinal = pkOrdinalByColumn.get(r.COLUMN_NAME);
        const defaultValue =
          !isComputed && genType === undefined ? rawDefault : undefined;
        const baseType = oracleFullType(
          r.DATA_TYPE,
          r.DATA_PRECISION,
          r.DATA_SCALE,
          r.DATA_LENGTH,
          r.CHAR_LENGTH,
          r.CHAR_USED,
        );
        const structuredKind = structuredConstraintKinds.get(r.COLUMN_NAME);
        const displayType =
          structuredKind === "json"
            ? `${baseType} IS JSON`
            : structuredKind === "xml"
              ? `${baseType} IS XML`
              : baseType;
        const collection = collectionByColumn.get(r.COLUMN_NAME);
        const objectType = objectByColumn.get(r.COLUMN_NAME);
        return {
          name: r.COLUMN_NAME,
          type: displayType,
          ...(collection
            ? {
                collectionKind: collection.kind,
                collectionElementType: collection.elementType,
                collectionTypeOwner: collection.owner,
                collectionTypeName: collection.name,
              }
            : {}),
          ...(objectType
            ? {
                objectTypeOwner: objectType.owner,
                objectTypeName: objectType.name,
                objectAttributes: objectType.attributes,
              }
            : {}),
          nullable: r.NULLABLE === "Y",
          defaultValue,
          identityGeneration:
            genType === undefined
              ? undefined
              : genType === "ALWAYS"
                ? "always"
                : "by_default",
          isComputed,
          computedExpression,
          generatedKind: isComputed ? "virtual" : undefined,
          isPersisted: isComputed ? false : undefined,
          isPrimaryKey: primaryKeyOrdinal !== undefined,
          primaryKeyOrdinal,
          isForeignKey: fkCols.has(r.COLUMN_NAME),
        };
      });
    } finally {
      await conn.close();
    }
  }
  async getIndexes(
    _database: string,
    schema: string,
    table: string,
  ): Promise<IndexMeta[]> {
    const conn = await this.getConnection();
    try {
      const res = await conn.execute<{
        INDEX_NAME: string;
        COLUMN_NAME: string;
        UNIQUENESS: string;
        INDEX_TYPE: string;
      }>(
        `SELECT i.index_name, c.column_name, i.uniqueness,
                CASE WHEN i.index_name = (
                  SELECT constraint_name FROM all_constraints
                  WHERE owner = i.owner AND table_name = i.table_name
                    AND constraint_type = 'P' AND ROWNUM = 1
                ) THEN 'PRIMARY' ELSE 'NORMAL' END AS index_type
         FROM all_indexes i
         JOIN all_ind_columns c
           ON c.index_name = i.index_name AND c.index_owner = i.owner
         WHERE i.owner = :1 AND i.table_name = :2
         ORDER BY i.index_name, c.column_position`,
        [schema.toUpperCase(), table.toUpperCase()],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );
      const map = new Map<string, IndexMeta>();
      for (const r of res.rows ?? []) {
        if (!map.has(r.INDEX_NAME)) {
          map.set(r.INDEX_NAME, {
            name: r.INDEX_NAME,
            columns: [],
            unique: r.UNIQUENESS === "UNIQUE",
            primary: r.INDEX_TYPE === "PRIMARY",
          });
        }
        const index = map.get(r.INDEX_NAME);
        if (index) {
          index.columns.push(r.COLUMN_NAME);
        }
      }
      return [...map.values()];
    } finally {
      await conn.close();
    }
  }
  async getForeignKeys(
    _database: string,
    schema: string,
    table: string,
  ): Promise<ForeignKeyMeta[]> {
    const conn = await this.getConnection();
    try {
      const res = await conn.execute<{
        CONSTRAINT_NAME: string;
        COLUMN_NAME: string;
        R_OWNER: string;
        R_TABLE_NAME: string;
        R_COLUMN_NAME: string;
      }>(
        `SELECT fk.constraint_name,
                fkcol.column_name,
                pk.owner       AS r_owner,
                pk.table_name  AS r_table_name,
                pkcol.column_name AS r_column_name
         FROM all_constraints fk
         JOIN all_cons_columns fkcol
           ON fkcol.constraint_name = fk.constraint_name AND fkcol.owner = fk.owner
         JOIN all_constraints pk
           ON pk.constraint_name = fk.r_constraint_name AND pk.owner = fk.r_owner
         JOIN all_cons_columns pkcol
           ON pkcol.constraint_name = pk.constraint_name AND pkcol.owner = pk.owner
              AND pkcol.position = fkcol.position
         WHERE fk.constraint_type = 'R'
           AND fk.owner = :1 AND fk.table_name = :2
         ORDER BY fk.constraint_name, fkcol.position`,
        [schema.toUpperCase(), table.toUpperCase()],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );
      return (res.rows ?? []).map((r) => ({
        constraintName: r.CONSTRAINT_NAME,
        column: r.COLUMN_NAME,
        referencedSchema: r.R_OWNER,
        referencedTable: r.R_TABLE_NAME,
        referencedColumn: r.R_COLUMN_NAME,
      }));
    } finally {
      await conn.close();
    }
  }
  async getConstraints(
    _database: string,
    schema: string,
    table: string,
  ): Promise<import("./types").TableConstraintMeta[]> {
    const conn = await this.getConnection();
    try {
      const keyRes = await conn.execute<{
        CONSTRAINT_NAME: string;
        CONSTRAINT_TYPE: string;
        COLUMN_NAME: string;
      }>(
        `SELECT cons.constraint_name, cons.constraint_type, cols.column_name
         FROM all_constraints cons
         JOIN all_cons_columns cols
           ON cols.constraint_name = cons.constraint_name
          AND cols.owner = cons.owner
         WHERE cons.owner = :1
           AND cons.table_name = :2
           AND cons.constraint_type IN ('P', 'U')
         ORDER BY cons.constraint_name, cols.position`,
        [schema.toUpperCase(), table.toUpperCase()],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );
      const constraints = new Map<
        string,
        import("./types").TableConstraintMeta
      >();
      for (const row of keyRes.rows ?? []) {
        const existing = constraints.get(row.CONSTRAINT_NAME);
        if (existing) {
          existing.columns.push(row.COLUMN_NAME);
          continue;
        }

        constraints.set(row.CONSTRAINT_NAME, {
          name: row.CONSTRAINT_NAME,
          kind: row.CONSTRAINT_TYPE === "P" ? "primary_key" : "unique",
          columns: [row.COLUMN_NAME],
          source: "catalog",
        });
      }

      const foreignKeys = await this.getForeignKeys("", schema, table);
      for (const foreignKey of foreignKeys) {
        const existing = constraints.get(foreignKey.constraintName);
        if (existing) {
          existing.columns.push(foreignKey.column);
          existing.referencedColumns = [
            ...(existing.referencedColumns ?? []),
            foreignKey.referencedColumn,
          ];
          continue;
        }

        constraints.set(foreignKey.constraintName, {
          name: foreignKey.constraintName,
          kind: "foreign_key",
          columns: [foreignKey.column],
          referencedSchema: foreignKey.referencedSchema,
          referencedTable: foreignKey.referencedTable,
          referencedColumns: [foreignKey.referencedColumn],
          source: "catalog",
        });
      }

      const res = await conn.execute<{
        CONSTRAINT_NAME: string;
        SEARCH_CONDITION_VC: string | null;
      }>(
        `SELECT constraint_name, search_condition_vc
         FROM all_constraints
         WHERE owner = :1
           AND table_name = :2
           AND constraint_type = 'C'
         ORDER BY constraint_name`,
        [schema.toUpperCase(), table.toUpperCase()],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );
      const checkConstraints = (res.rows ?? []).map((row) => ({
        name: row.CONSTRAINT_NAME,
        kind: "check" as const,
        columns: [],
        checkExpression: row.SEARCH_CONDITION_VC ?? undefined,
        source: "catalog" as const,
      }));
      return [...constraints.values(), ...checkConstraints];
    } finally {
      await conn.close();
    }
  }
  async getTriggers(
    _database: string,
    schema: string,
    table: string,
  ): Promise<import("./types").TriggerMeta[] | null> {
    const conn = await this.getConnection();
    try {
      const res = await conn.execute<{
        TRIGGER_NAME: string;
        TRIGGER_TYPE: string;
        TRIGGERING_EVENT: string;
        STATUS: string;
        TRIGGER_BODY: string | null;
      }>(
        `SELECT trigger_name, trigger_type, triggering_event, status, trigger_body
         FROM all_triggers
         WHERE table_owner = :1
           AND table_name = :2
         ORDER BY trigger_name`,
        [schema.toUpperCase(), table.toUpperCase()],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );
      return (res.rows ?? []).map((row) => {
        const triggerType = row.TRIGGER_TYPE.toUpperCase();
        const triggeringEvent = row.TRIGGERING_EVENT.toUpperCase();
        const events: import("./types").TriggerMeta["events"] = [];
        if (triggeringEvent.includes("INSERT")) {
          events.push("insert");
        }
        if (triggeringEvent.includes("UPDATE")) {
          events.push("update");
        }
        if (triggeringEvent.includes("DELETE")) {
          events.push("delete");
        }
        if (events.length === 0) {
          events.push("unknown");
        }

        return {
          name: row.TRIGGER_NAME,
          timing: triggerType.includes("BEFORE")
            ? "before"
            : triggerType.includes("INSTEAD OF")
              ? "instead_of"
              : triggerType.includes("AFTER")
                ? "after"
                : "unknown",
          events,
          orientation: triggerType.includes("EACH ROW") ? "row" : "statement",
          enabled: row.STATUS === "ENABLED",
          definition: row.TRIGGER_BODY ?? undefined,
        };
      });
    } finally {
      await conn.close();
    }
  }
  override async getTriggerDDL(
    _database: string,
    schema: string,
    table: string,
    triggerName: string,
  ): Promise<string> {
    const conn = await this.getConnection();
    try {
      const res = await conn.execute<{
        TRIGGER_TYPE: string;
        TRIGGERING_EVENT: string;
        TRIGGER_BODY: string | null;
      }>(
        `SELECT trigger_type, triggering_event, trigger_body
         FROM all_triggers
         WHERE table_owner = :1
           AND table_name = :2
           AND trigger_name = :3`,
        [schema.toUpperCase(), table.toUpperCase(), triggerName.toUpperCase()],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );
      const row = res.rows?.[0];
      if (!row) {
        throw new Error(`Trigger "${triggerName}" not found`);
      }

      const triggerType = row.TRIGGER_TYPE.toUpperCase();
      const timingClause = triggerType.replace(/\s+EACH ROW/i, "");
      const forEachRowClause = triggerType.includes("EACH ROW")
        ? "\nFOR EACH ROW"
        : "";
      const body = row.TRIGGER_BODY?.trim() || "BEGIN\n  NULL;\nEND;";
      return [
        `CREATE OR REPLACE TRIGGER ${this.quoteIdentifier(schema.toUpperCase())}.${this.quoteIdentifier(triggerName.toUpperCase())}`,
        `${timingClause} ${row.TRIGGERING_EVENT.toUpperCase()}`,
        `ON ${this.qualifiedTableName("", schema.toUpperCase(), table.toUpperCase())}${forEachRowClause}`,
        body,
      ].join("\n");
    } finally {
      await conn.close();
    }
  }
  async getCreateTableDDL(
    _database: string,
    schema: string,
    table: string,
  ): Promise<string> {
    const conn = await this.getConnection();
    try {
      const objectType = await this._resolveDdlObjectType(conn, schema, table);
      if (objectType) {
        const metadataObjectType =
          objectType === "MATERIALIZED VIEW" ? "MATERIALIZED_VIEW" : objectType;
        const ddl = await this._getMetadataDDL(
          conn,
          metadataObjectType,
          schema,
          table,
        );
        if (ddl) {
          return ddl;
        }
      }
      if (objectType === "VIEW") {
        return await this._fallbackViewDDL(conn, schema, table);
      }
      if (objectType === "TABLE") {
        return await this._fallbackDDL(conn, schema, table);
      }
      try {
        const viewDdl = await this._fallbackViewDDL(conn, schema, table);
        if (!isUnavailableOracleDdl(viewDdl)) {
          return viewDdl;
        }
      } catch {}
      try {
        const tableDdl = await this._fallbackDDL(conn, schema, table);
        if (!isUnavailableOracleDdl(tableDdl)) {
          return tableDdl;
        }
      } catch {}
      return `-- DDL not available for "${schema}"."${table}"`;
    } catch {
      return `-- DDL not available for "${schema}"."${table}"`;
    } finally {
      await conn.close();
    }
  }
  async getObjectDefinition(
    _database: string,
    schema: string,
    name: string,
    kind: DdlOnlyDbObjectKind,
  ): Promise<string | null> {
    const conn = await this.getConnection();
    try {
      if (kind === "sequence") {
        const metadataDdl = await this._getMetadataDDL(
          conn,
          "SEQUENCE",
          schema,
          name,
        );
        if (metadataDdl) {
          return metadataDdl;
        }

        return await this._fallbackSequenceDDL(conn, schema, name);
      }

      return await this._getMetadataDDL(conn, "TYPE", schema, name);
    } finally {
      await conn.close();
    }
  }
  private async _resolveDdlObjectType(
    conn: oracledb.Connection,
    schema: string,
    objectName: string,
  ): Promise<"TABLE" | "VIEW" | "MATERIALIZED VIEW" | null> {
    const owner = schema.toUpperCase();
    const name = objectName.toUpperCase();
    const obj = oracledb.OUT_FORMAT_OBJECT;
    try {
      const objRes = await conn.execute<{
        OBJECT_TYPE: string;
      }>(
        `SELECT object_type FROM all_objects
         WHERE owner = :1 AND object_name = :2
           AND object_type IN ('TABLE','VIEW','MATERIALIZED VIEW') AND ROWNUM = 1`,
        [owner, name],
        { outFormat: obj },
      );
      const objectType = objRes.rows?.[0]?.OBJECT_TYPE;
      if (
        objectType === "TABLE" ||
        objectType === "VIEW" ||
        objectType === "MATERIALIZED VIEW"
      ) {
        return objectType;
      }
    } catch {}
    try {
      const viewRes = await conn.execute<{
        VIEW_NAME: string;
      }>(
        `SELECT view_name FROM all_views
         WHERE owner = :1 AND view_name = :2 AND ROWNUM = 1`,
        [owner, name],
        { outFormat: obj },
      );
      if (viewRes.rows?.[0]?.VIEW_NAME) {
        return "VIEW";
      }
    } catch {}
    try {
      const tableRes = await conn.execute<{
        TABLE_NAME: string;
      }>(
        `SELECT table_name FROM all_tables
         WHERE owner = :1 AND table_name = :2 AND ROWNUM = 1`,
        [owner, name],
        { outFormat: obj },
      );
      if (tableRes.rows?.[0]?.TABLE_NAME) {
        return "TABLE";
      }
    } catch {}
    return null;
  }
  private async _fallbackViewDDL(
    conn: oracledb.Connection,
    schema: string,
    view: string,
  ): Promise<string> {
    const res = await conn.execute<{
      TEXT: unknown;
    }>(
      `SELECT text FROM all_views WHERE owner = :1 AND view_name = :2`,
      [schema.toUpperCase(), view.toUpperCase()],
      { outFormat: oracledb.OUT_FORMAT_OBJECT },
    );
    const row = res.rows?.[0];
    if (!row?.TEXT) {
      return `-- View source not available for "${schema}"."${view}"`;
    }
    const text = await this._consumeLob(row.TEXT);
    const cleanText = toOracleDdlText(text);
    return cleanText
      ? `CREATE OR REPLACE VIEW "${schema}"."${view}" AS\n${cleanText}`
      : `-- View source not available for "${schema}"."${view}"`;
  }
  private async _getMetadataDDL(
    conn: oracledb.Connection,
    objectType:
      | "TABLE"
      | "VIEW"
      | "MATERIALIZED_VIEW"
      | "SEQUENCE"
      | "TYPE"
      | "PACKAGE",
    schema: string,
    objectName: string,
  ): Promise<string | null> {
    try {
      await conn.execute(ORACLE_DDL_TRANSFORM_BLOCK);
    } catch {}
    try {
      const res = await conn.execute<{
        DDL: unknown;
      }>(
        `SELECT DBMS_METADATA.GET_DDL('${objectType}', :2, :1) AS ddl FROM dual`,
        [schema.toUpperCase(), objectName.toUpperCase()],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );
      const ddl = await this._consumeLob(res.rows?.[0]?.DDL);
      return toOracleDdlText(ddl);
    } catch {
      return null;
    }
  }
  private async _fallbackSequenceDDL(
    conn: oracledb.Connection,
    schema: string,
    sequenceName: string,
  ): Promise<string> {
    try {
      const res = await conn.execute<{
        MIN_VALUE: string | number | null;
        MAX_VALUE: string | number | null;
        INCREMENT_BY: string | number;
        CYCLE_FLAG: string | null;
        ORDER_FLAG: string | null;
        CACHE_SIZE: string | number | null;
        LAST_NUMBER: string | number | null;
      }>(
        `SELECT min_value,
                max_value,
                increment_by,
                cycle_flag,
                order_flag,
                cache_size,
                last_number
         FROM all_sequences
         WHERE sequence_owner = :1
           AND sequence_name = :2`,
        [schema.toUpperCase(), sequenceName.toUpperCase()],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );

      const row = res.rows?.[0];
      if (!row) {
        return `-- Sequence DDL unavailable for "${schema}"."${sequenceName}": sequence metadata not found or not accessible.`;
      }

      const minValue =
        row.MIN_VALUE !== null && row.MIN_VALUE !== undefined
          ? `MINVALUE ${String(row.MIN_VALUE)}`
          : "NOMINVALUE";
      const maxValue =
        row.MAX_VALUE !== null && row.MAX_VALUE !== undefined
          ? `MAXVALUE ${String(row.MAX_VALUE)}`
          : "NOMAXVALUE";
      const cacheValue =
        row.CACHE_SIZE !== null && row.CACHE_SIZE !== undefined
          ? Number(row.CACHE_SIZE) > 0
            ? `CACHE ${String(row.CACHE_SIZE)}`
            : "NOCACHE"
          : "NOCACHE";
      const startWith =
        row.LAST_NUMBER !== null && row.LAST_NUMBER !== undefined
          ? `START WITH ${String(row.LAST_NUMBER)}`
          : undefined;

      const parts = [
        `CREATE SEQUENCE ${this.quoteIdentifier(schema.toUpperCase())}.${this.quoteIdentifier(sequenceName.toUpperCase())}`,
        `INCREMENT BY ${String(row.INCREMENT_BY)}`,
        minValue,
        maxValue,
        startWith,
        cacheValue,
        row.CYCLE_FLAG === "Y" ? "CYCLE" : "NOCYCLE",
        row.ORDER_FLAG === "Y" ? "ORDER" : "NOORDER",
      ].filter((part): part is string => Boolean(part));

      return `${parts.join(" ")};`;
    } catch (error: unknown) {
      return `-- Sequence DDL unavailable for "${schema}"."${sequenceName}": ${oracleErrorMessage(error)}`;
    }
  }
  private async _consumeLob(lob: unknown): Promise<string | Buffer | null> {
    if (!lob) return null;
    if (!isOracleLobLike(lob)) {
      return typeof lob === "string" || Buffer.isBuffer(lob) ? lob : null;
    }
    try {
      if (typeof lob.getData === "function") {
        const data = await lob.getData();
        return data;
      }
      return null;
    } finally {
      if (typeof lob.destroy === "function") {
        lob.destroy();
      } else if (typeof lob.close === "function") {
        await lob.close().catch(() => {});
      }
    }
  }
  private async _fallbackDDL(
    conn: oracledb.Connection,
    schema: string,
    table: string,
  ): Promise<string> {
    const OBJ = oracledb.OUT_FORMAT_OBJECT;
    const colRows = await this._readTableColumnsWithVirtualFallback(
      conn,
      schema,
      table,
    );
    const identityMap = new Map<
      string,
      "ALWAYS" | "BY DEFAULT" | "BY DEFAULT ON NULL"
    >();
    try {
      const idRes = await conn.execute<{
        COLUMN_NAME: string;
        GENERATION_TYPE: string;
      }>(
        `SELECT column_name, generation_type
         FROM all_tab_identity_cols
         WHERE owner = :1 AND table_name = :2`,
        [schema.toUpperCase(), table.toUpperCase()],
        { outFormat: OBJ },
      );
      for (const r of idRes.rows ?? []) {
        identityMap.set(
          r.COLUMN_NAME,
          (r.GENERATION_TYPE ?? "ALWAYS") as
            | "ALWAYS"
            | "BY DEFAULT"
            | "BY DEFAULT ON NULL",
        );
      }
    } catch {}
    const pkCols: string[] = [];
    let pkConstraintName = "";
    try {
      const pkRes = await conn.execute<{
        COLUMN_NAME: string;
        CONSTRAINT_NAME: string;
      }>(
        `SELECT cols.column_name, cons.constraint_name
         FROM all_constraints cons
         JOIN all_cons_columns cols
           ON cons.constraint_name = cols.constraint_name
           AND cons.owner = cols.owner
         WHERE cons.constraint_type = 'P'
           AND cons.owner = :1 AND cons.table_name = :2
         ORDER BY cols.position`,
        [schema.toUpperCase(), table.toUpperCase()],
        { outFormat: OBJ },
      );
      for (const r of pkRes.rows ?? []) {
        pkCols.push(r.COLUMN_NAME);
        pkConstraintName = pkConstraintName || r.CONSTRAINT_NAME;
      }
    } catch {}
    const colLines = colRows.map((r) => {
      const typ = oracleFullType(
        r.DATA_TYPE,
        r.DATA_PRECISION,
        r.DATA_SCALE,
        r.DATA_LENGTH,
        r.CHAR_LENGTH,
        r.CHAR_USED,
      );
      const nullable = r.NULLABLE === "Y" ? "" : " NOT NULL";
      if (r.VIRTUAL_COLUMN === "YES") {
        const expression = r.DATA_DEFAULT?.trim() ?? "";
        return `  "${r.COLUMN_NAME}" ${typ} GENERATED ALWAYS AS (${expression}) VIRTUAL${nullable}`;
      }
      const genType = identityMap.get(r.COLUMN_NAME);
      if (genType !== undefined) {
        const genClause =
          genType === "ALWAYS"
            ? "GENERATED ALWAYS AS IDENTITY"
            : genType === "BY DEFAULT ON NULL"
              ? "GENERATED BY DEFAULT ON NULL AS IDENTITY"
              : "GENERATED BY DEFAULT AS IDENTITY";
        return `  "${r.COLUMN_NAME}" ${typ} ${genClause}${nullable}`;
      }
      let defClause = "";
      if (r.DATA_DEFAULT) {
        const raw = r.DATA_DEFAULT.trim();
        if (!raw.toLowerCase().includes(".nextval")) {
          defClause = ` DEFAULT ${raw}`;
        }
      }
      return `  "${r.COLUMN_NAME}" ${typ}${nullable}${defClause}`;
    });
    if (pkCols.length > 0) {
      const pkColList = pkCols.map((c) => `"${c}"`).join(", ");
      const constraintClause = pkConstraintName
        ? `CONSTRAINT "${pkConstraintName}" PRIMARY KEY (${pkColList})`
        : `PRIMARY KEY (${pkColList})`;
      colLines.push(`  ${constraintClause}`);
    }
    return `CREATE TABLE "${schema}"."${table}" (\n${colLines.join(",\n")}\n);`;
  }
  private async _readTableColumnsWithVirtualFallback(
    conn: oracledb.Connection,
    schema: string,
    table: string,
  ): Promise<
    Array<{
      COLUMN_NAME: string;
      DATA_TYPE: string;
      DATA_PRECISION: number | null;
      DATA_SCALE: number | null;
      DATA_LENGTH: number;
      CHAR_LENGTH: number | null;
      CHAR_USED: string | null;
      NULLABLE: string;
      DATA_DEFAULT: string | null;
      VIRTUAL_COLUMN: string;
      COLUMN_ID: number;
    }>
  > {
    const owner = schema.toUpperCase();
    const tableName = table.toUpperCase();
    try {
      const res = await conn.execute<{
        COLUMN_NAME: string;
        DATA_TYPE: string;
        DATA_PRECISION: number | null;
        DATA_SCALE: number | null;
        DATA_LENGTH: number;
        CHAR_LENGTH: number | null;
        CHAR_USED: string | null;
        NULLABLE: string;
        DATA_DEFAULT: string | null;
        VIRTUAL_COLUMN: string;
        COLUMN_ID: number;
      }>(
        `SELECT column_name, data_type, data_precision, data_scale,
                data_length, char_length, char_used, nullable, data_default, virtual_column, column_id
         FROM all_tab_cols
         WHERE owner = :1 AND table_name = :2 AND hidden_column = 'NO'
         ORDER BY column_id`,
        [owner, tableName],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );
      return (res.rows ?? []).map((row) => ({
        ...row,
        VIRTUAL_COLUMN:
          row.DATA_TYPE?.toUpperCase() === "XMLTYPE" &&
          !row.DATA_DEFAULT?.trim()
            ? "NO"
            : row.VIRTUAL_COLUMN,
      }));
    } catch (error) {
      if (!isOracleInvalidIdentifierError(error)) {
        throw error;
      }
      const res = await conn.execute<{
        COLUMN_NAME: string;
        DATA_TYPE: string;
        DATA_PRECISION: number | null;
        DATA_SCALE: number | null;
        DATA_LENGTH: number;
        CHAR_LENGTH: number | null;
        CHAR_USED: string | null;
        NULLABLE: string;
        DATA_DEFAULT: string | null;
        COLUMN_ID: number;
      }>(
        `SELECT column_name, data_type, data_precision, data_scale,
                data_length, char_length, char_used, nullable, data_default, column_id
         FROM all_tab_columns
         WHERE owner = :1 AND table_name = :2
         ORDER BY column_id`,
        [owner, tableName],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );
      return (res.rows ?? []).map((row) => ({
        ...row,
        VIRTUAL_COLUMN: "NO",
      }));
    }
  }
  async getRoutineDefinition(
    _database: string,
    schema: string,
    name: string,
    kind: "function" | "procedure",
    routineIdentity?: string,
  ): Promise<string> {
    const conn = await this.getConnection();
    try {
      const parsedIdentity = parseOracleRoutineIdentity(routineIdentity);
      if (kind === "procedure" && parsedIdentity?.kind === "package") {
        const metadataDdl = await this._getMetadataDDL(
          conn,
          "PACKAGE",
          schema,
          name,
        );
        if (metadataDdl) {
          return metadataDdl;
        }

        const packageSource = await conn.execute<{
          TEXT: string;
        }>(
          `SELECT text FROM all_source
           WHERE owner = :1 AND name = :2
             AND type = 'PACKAGE'
           ORDER BY line`,
          [schema.toUpperCase(), name.toUpperCase()],
          { outFormat: oracledb.OUT_FORMAT_OBJECT },
        );
        const packageLines = (packageSource.rows ?? []).map((r) => r.TEXT);
        if (packageLines.length > 0) {
          return `CREATE OR REPLACE ${packageLines.join("")}`;
        }

        return `-- Source not available for package ${schema}.${name}`;
      }

      const sourceType =
        kind === "function"
          ? "FUNCTION"
          : parsedIdentity?.kind === "procedure"
            ? "PROCEDURE"
            : "PROCEDURE";
      const res = await conn.execute<{
        TEXT: string;
      }>(
        `SELECT text FROM all_source
         WHERE owner = :1 AND name = :2
           AND type = :3
         ORDER BY line`,
        [schema.toUpperCase(), name.toUpperCase(), sourceType],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );
      const lines = (res.rows ?? []).map((r) => r.TEXT);
      if (lines.length === 0) {
        return `-- Source not available for ${kind} ${schema}.${name}`;
      }
      return `CREATE OR REPLACE ${lines.join("")}`;
    } finally {
      await conn.close();
    }
  }
  async query(
    sql: string,
    params?: unknown[],
    operationContext?: {
      requestToken?: number;
      readOnly?: boolean;
      hardCap?: number;
    },
  ): Promise<QueryResult> {
    if (operationContext?.readOnly) {
      throw new Error(ORACLE_READ_ONLY_ENFORCEMENT_MESSAGE);
    }
    const limit = queryCollectionLimit(operationContext?.hardCap);
    const start = Date.now();
    const operation: OracleQueryOperation = {
      requestToken: operationContext?.requestToken,
      cancelled: false,
    };
    this.activeQueryOperations.add(operation);
    let conn: oracledb.Connection | undefined;
    let failed = false;
    try {
      conn = await this.getConnection();
      operation.connection = conn;
      const statements =
        params && params.length > 0 ? [sql] : splitOracleStatements(sql.trim());
      let selectResult: QueryResult | null = null;
      let totalAffected = 0;
      for (const stmt of statements) {
        if (operation.cancelled) {
          throw new Error("Oracle query cancelled before execution completed.");
        }
        const result = await this._execOne(
          conn,
          stmt,
          params,
          start,
          limit,
          operation,
        );
        totalAffected += result.affectedRows ?? 0;
        if (result.columns.length > 0) {
          selectResult = result;
        }
      }
      if (operation.cancelled) {
        throw new Error("Oracle query cancelled before commit.");
      }
      await conn.commit();
      if (selectResult) {
        return { ...selectResult, executionTimeMs: Date.now() - start };
      }
      return {
        columns: [],
        rows: [],
        rowCount: totalAffected,
        executionTimeMs: Date.now() - start,
        affectedRows: totalAffected,
      };
    } catch (error) {
      failed = true;
      await conn?.rollback().catch(() => undefined);
      throw error;
    } finally {
      this.activeQueryOperations.delete(operation);
      await conn?.close().catch((error: unknown) => {
        if (!failed) throw error;
      });
    }
  }
  private _fetchTypeHandler(
    metaData: oracledb.Metadata<unknown>,
  ): oracledb.FetchTypeResponse | undefined {
    if (metaData.dbType === oracledb.DB_TYPE_NUMBER) {
      return {
        type: oracledb.STRING,
        converter: (value) => normalizeOracleNumberValue(value, metaData),
      };
    }
    if (metaData.dbType === oracledb.DB_TYPE_INTERVAL_YM) {
      return {
        converter: (value) => normalizeOracleIntervalYMValue(value) ?? value,
      };
    }
    if (metaData.dbType === oracledb.DB_TYPE_INTERVAL_DS) {
      return {
        converter: (value) => normalizeOracleIntervalDSValue(value) ?? value,
      };
    }
    return undefined;
  }
  private async _buildTemporalQueryProjection(
    conn: oracledb.Connection,
    sql: string,
  ): Promise<OracleTemporalQueryProjection | undefined> {
    const candidateKeyword = oracleSelectCandidateKeyword(sql);
    if (!candidateKeyword) return undefined;

    try {
      let statementInfo: {
        statementType?: number;
        metaData?: Array<Pick<oracledb.Metadata<unknown>, "name" | "dbType">>;
      };
      try {
        statementInfo = await conn.getStatementInfo(sql);
      } catch (error) {
        if (
          !(error instanceof TypeError) ||
          !error.message.includes("_getDbObjectClass")
        )
          throw error;
        const result = await conn.execute<{ columns: oracledb.Lob }>(
          `
DECLARE
  cursor_id INTEGER;
  column_count INTEGER;
  descriptors DBMS_SQL.DESC_TAB3;
  entries JSON_ARRAY_T := JSON_ARRAY_T();
  entry JSON_OBJECT_T;
BEGIN
  cursor_id := DBMS_SQL.OPEN_CURSOR;
  DBMS_SQL.PARSE(cursor_id, :sql_text, DBMS_SQL.NATIVE);
  DBMS_SQL.DESCRIBE_COLUMNS3(cursor_id, column_count, descriptors);
  FOR i IN 1..column_count LOOP
    entry := JSON_OBJECT_T();
    entry.put('name', descriptors(i).col_name);
    entry.put('type', descriptors(i).col_type);
    entries.append(entry);
  END LOOP;
  :columns := entries.to_clob();
  DBMS_SQL.CLOSE_CURSOR(cursor_id);
EXCEPTION WHEN OTHERS THEN
  IF DBMS_SQL.IS_OPEN(cursor_id) THEN DBMS_SQL.CLOSE_CURSOR(cursor_id); END IF;
  RAISE;
END;`,
          {
            sql_text: { dir: oracledb.BIND_IN, type: oracledb.CLOB, val: sql },
            columns: { dir: oracledb.BIND_OUT, type: oracledb.CLOB },
          },
        );
        const text = await this._consumeLob(result.outBinds?.columns);
        if (typeof text !== "string") return undefined;
        const described = JSON.parse(text) as Array<{
          name: string;
          type: number;
        }>;
        const types = new Map<number, oracledb.DbType>([
          [12, oracledb.DB_TYPE_DATE],
          [180, oracledb.DB_TYPE_TIMESTAMP],
          [181, oracledb.DB_TYPE_TIMESTAMP_TZ],
          [231, oracledb.DB_TYPE_TIMESTAMP_LTZ],
          [109, oracledb.DB_TYPE_OBJECT],
        ]);
        statementInfo = {
          statementType: oracledb.STMT_TYPE_SELECT,
          metaData: described.map((column) => ({
            name: column.name,
            dbType: types.get(column.type),
          })) as oracledb.Metadata<unknown>[],
        };
      }
      const columns = statementInfo.metaData ?? [];
      if (
        (statementInfo.statementType !== oracledb.STMT_TYPE_SELECT &&
          !(candidateKeyword === "WITH" && !statementInfo.statementType)) ||
        columns.length === 0 ||
        !columns.some((column) =>
          oracleTemporalProjectionExpression(
            column,
            '"__RAPIDB_TEMPORAL_OUTER"',
          ),
        )
      ) {
        return undefined;
      }

      if (
        new Set(columns.map((column) => column.name)).size !== columns.length
      ) {
        return undefined;
      }

      const outputNames = new Map<string, string>();
      const innerAlias = '"__RAPIDB_TEMPORAL_INNER"';
      const outerAlias = '"__RAPIDB_TEMPORAL_OUTER"';
      const columnLabels = new Set(columns.map((column) => column.name));
      let rowOrderName = "__RAPIDB_TEMPORAL_ORDER";
      let rowOrderSuffix = 0;
      while (columnLabels.has(rowOrderName)) {
        rowOrderName = `__RAPIDB_TEMPORAL_ORDER_${rowOrderSuffix++}`;
      }
      const rowOrderAlias = `"${rowOrderName}"`;
      const selectList = columns.map((column, index) => {
        const outputAlias = `"__RAPIDB_Q_COL_${index}"`;
        const expression =
          oracleTemporalProjectionExpression(column, outerAlias) ??
          `${outerAlias}."${column.name.replace(/"/g, '""')}"`;
        outputNames.set(outputAlias.slice(1, -1), column.name);
        return `${expression} AS ${outputAlias}`;
      });
      const projectedSql =
        `SELECT ${selectList.join(", ")} ` +
        `FROM (SELECT ${innerAlias}.*, ROWNUM AS ${rowOrderAlias} ` +
        `FROM (${sql}\n) ${innerAlias}) ${outerAlias} ` +
        `ORDER BY ${outerAlias}.${rowOrderAlias}`;

      const projectedInfo = await conn.getStatementInfo(projectedSql);
      const projectedColumns = projectedInfo.metaData ?? [];
      if (
        projectedInfo.statementType !== oracledb.STMT_TYPE_SELECT ||
        projectedColumns.length !== columns.length ||
        projectedColumns.some(
          (column, index) => column.name !== `__RAPIDB_Q_COL_${index}`,
        )
      ) {
        return undefined;
      }
      return { sql: projectedSql, columnNames: outputNames };
    } catch {
      return undefined;
    }
  }
  private async _execOne(
    conn: oracledb.Connection,
    sql: string,
    params: unknown[] | undefined,
    start: number,
    limit: number | undefined,
    operation: OracleQueryOperation,
  ): Promise<QueryResult> {
    let finalSql = sql;
    let binds: unknown[] = [];
    if (params && params.length > 0) {
      const replaced = replacePositionalParams(sql, params);
      finalSql = replaced.sql;
      binds = replaced.binds;
    }
    const temporalProjection = await this._buildTemporalQueryProjection(
      conn,
      finalSql,
    );
    const columnNames = new WeakMap<object, string>();
    const options: oracledb.ExecuteOptions = {
      outFormat: oracledb.OUT_FORMAT_ARRAY,
      dbObjectAsPojo: true,
      fetchArraySize: 100,
      prefetchRows: 0,
      resultSet: true,
      autoCommit: false,
      fetchTypeHandler: (metadata) => {
        columnNames.set(
          metadata,
          temporalProjection?.columnNames.get(metadata.name) ?? metadata.name,
        );
        return this._fetchTypeHandler(metadata);
      },
    };
    const res = await conn.execute(
      temporalProjection?.sql ?? finalSql,
      binds,
      options,
    );
    const cursors = new Set<oracledb.ResultSet<unknown[]>>();
    const register = (value: unknown): void => {
      if (
        value &&
        typeof value === "object" &&
        "getRows" in value &&
        "close" in value
      ) {
        cursors.add(value as oracledb.ResultSet<unknown[]>);
      } else if (Array.isArray(value)) {
        value.forEach(register);
      }
    };
    register(res.resultSet);
    register(res.implicitResults);
    Object.values(res.outBinds ?? {}).forEach(register);
    let selected: QueryResult | undefined;
    let closeError: unknown;
    try {
      for (const cursor of cursors) {
        const metadata = cursor.metaData;
        const collected = new BoundedQueryRows<Record<string, unknown>>(
          limit ?? Infinity,
        );
        selected = undefined;
        while (true) {
          if (operation.cancelled)
            throw new Error("Oracle query cancelled during fetch.");
          const batch = await cursor.getRows(100);
          if (operation.cancelled)
            throw new Error("Oracle query cancelled during fetch.");
          if (batch.length === 0) break;
          for (const row of batch) {
            if (collected.rows.length < collected.limit) {
              collected.add(
                Object.fromEntries(
                  row.map((value, index) => [
                    `__col_${index}`,
                    formatOracleQueryValue(value, metadata[index]),
                  ]),
                ),
              );
            } else {
              collected.rowCount++;
            }
          }
        }
        selected = {
          columns: metadata.map(
            (column) => columnNames.get(column) ?? column.name,
          ),
          rows: collected.rows,
          rowCount: collected.rowCount,
          ...(collected.truncated ? { truncated: true } : {}),
          executionTimeMs: Date.now() - start,
        };
      }
    } finally {
      for (const cursor of cursors) {
        try {
          await cursor.close();
        } catch (error) {
          closeError ??= error;
        }
      }
    }
    if (closeError) throw closeError;
    if (selected)
      return {
        ...selected,
        ...(res.rowsAffected !== undefined
          ? { affectedRows: res.rowsAffected }
          : {}),
      };
    const executionTimeMs = Date.now() - start;
    const affectedRows = res.rowsAffected ?? 0;
    return {
      columns: [],
      rows: [],
      rowCount: affectedRows,
      executionTimeMs,
      affectedRows,
    };
  }
  async runTransaction(
    operations: import("./types").TransactionOperation[],
    context?: import("./types").TransactionContext,
    scope?: import("./types").TransactionOptions,
  ): Promise<void> {
    throwIfTransactionCancelled(context);
    const conn = await this.getConnection();
    const cancel = () => {
      void conn.break().catch(() => undefined);
    };
    context?.signal.addEventListener("abort", cancel, { once: true });
    try {
      const identities = new TransactionIdentityStore();
      for (const [index, op] of operations.entries()) {
        throwIfTransactionCancelled(context);
        let finalSql = op.sql;
        let binds: Record<string, unknown> | unknown[] = {};
        if (op.params && op.params.length > 0) {
          const replaced = replacePositionalParams(op.sql, op.params);
          finalSql = replaced.sql;
          binds = replaced.binds;
        }
        const outTypes = op.captureIdentity?.oracleOutTypes;
        if (outTypes) {
          binds = [
            ...(Array.isArray(binds) ? binds : []),
            ...outTypes.map((type) => ({
              dir: oracledb.BIND_OUT,
              type: type === "buffer" ? oracledb.BUFFER : oracledb.STRING,
              maxSize: 32767,
            })),
          ];
        }
        const res = await conn.execute(
          finalSql,
          binds as oracledb.BindParameters,
          {
            outFormat: oracledb.OUT_FORMAT_OBJECT,
            autoCommit: false,
          },
        );
        assertTransactionAffectedRows(op, res.rowsAffected ?? 0);
        if (outTypes) {
          const outputs = res.outBinds as unknown[][] | undefined;
          const returnedKeys =
            outputs?.length === outTypes.length &&
            outputs.every(
              (values) => Array.isArray(values) && values.length === 1,
            )
              ? [
                  Object.fromEntries(
                    outputs.map((values, keyIndex) => [
                      `__col_${keyIndex}`,
                      values[0],
                    ]),
                  ),
                ]
              : [];
          await identities.capture(index, op, returnedKeys, async () => {
            throw new Error(
              "Oracle identity must come from RETURNING OUT binds.",
            );
          });
        }
      }
      await verifyTransaction(
        this,
        identities.resolve(scope?.verifications),
        async (verification) => {
          const result = await this._execOne(
            conn,
            verification.sql,
            verification.params,
            Date.now(),
            2,
            {
              get cancelled() {
                return (
                  context?.signal.aborted === true ||
                  (context?.deadline !== undefined &&
                    Date.now() >= context.deadline)
                );
              },
            },
          );
          return result.rows;
        },
        context,
      );
      throwIfTransactionCancelled(context);
      await conn.commit();
    } catch (e) {
      try {
        await conn.rollback();
      } catch {}
      throw e;
    } finally {
      context?.signal.removeEventListener("abort", cancel);
      await conn.close();
    }
  }
  mapTypeCategory(nativeType: string): TypeCategory {
    const normalizedType = nativeType.toUpperCase().trim();
    const ct = oracleTypeName(normalizedType);
    if (
      [
        "NUMBER",
        "INTEGER",
        "SMALLINT",
        "PLS_INTEGER",
        "BINARY_INTEGER",
      ].includes(ct)
    ) {
      if (ct === "NUMBER" && !normalizedType.includes("(")) {
        return "decimal";
      }
      const scale = parseOracleNumberScale(normalizedType);
      if (ct === "NUMBER" && scale !== null && scale > 0) {
        return "decimal";
      }
      return "integer";
    }
    if (ct === "FLOAT" || ct === "BINARY_FLOAT" || ct === "BINARY_DOUBLE")
      return "float";
    if (ct === "DATE") return "datetime";
    if (ct.startsWith("TIMESTAMP")) return "datetime";
    if (ct.startsWith("INTERVAL")) return "interval";
    if (["BLOB", "RAW", "LONG RAW"].includes(ct)) return "binary";
    if (["CLOB", "NCLOB", "LONG"].includes(ct)) return "text";
    if (ct === "XMLTYPE") return "text";
    if (ct === "SDO_GEOMETRY") return "spatial";
    if (ct === "ROWID" || ct === "UROWID") return "text";
    if (ct.includes("CHAR") || ct.includes("VARCHAR")) return "text";
    return "other";
  }
  protected getValueSemantics(
    _nativeType: string,
    _category: TypeCategory,
  ): ValueSemantics {
    return "plain";
  }
  protected override enrichColumn(col: ColumnMeta): ColumnTypeMeta {
    const enriched = super.enrichColumn(col);
    const typeName = oracleTypeName(enriched.nativeType);
    const baseOperators: FilterOperator[] = enriched.filterOperators.filter(
      (operator) => operator !== "is_null" && operator !== "is_not_null",
    );
    const withNullableOperators = (
      operators: FilterOperator[],
    ): FilterOperator[] => {
      if (!enriched.nullable) {
        return operators;
      }
      const nextOperators = operators.slice();
      nextOperators.push("is_null", "is_not_null");
      return nextOperators;
    };

    if (col.collectionKind === "varray") {
      const elementType = col.collectionElementType ?? "";
      const supportsTextContains = [
        "CHAR",
        "NCHAR",
        "VARCHAR2",
        "NVARCHAR2",
      ].includes(oracleTypeName(elementType));
      return {
        ...enriched,
        filterable: supportsTextContains,
        filterOperators: withNullableOperators(
          supportsTextContains ? ["like"] : [],
        ),
      };
    }

    if (isOracleIntervalType(enriched.nativeType)) {
      const operators: FilterOperator[] = ["eq", "neq", "like"];
      return {
        ...enriched,
        filterable: true,
        filterOperators: withNullableOperators(operators),
      };
    }

    if (typeName === "CLOB" || typeName === "NCLOB") {
      const operators: FilterOperator[] = [...baseOperators];
      if (!operators.includes("eq")) {
        operators.unshift("eq");
      }
      if (!operators.includes("neq")) {
        operators.splice(1, 0, "neq");
      }
      return {
        ...enriched,
        filterable: true,
        filterOperators: withNullableOperators(operators),
      };
    }

    return enriched;
  }
  isDatetimeWithTime(nativeType: string): boolean {
    const ct = nativeType.toUpperCase().split("(")[0].trim();
    return ct === "DATE" || ct.startsWith("TIMESTAMP");
  }
  protected override isFilterable(
    nativeType: string,
    category: TypeCategory,
  ): boolean {
    if (category === "spatial") {
      return true;
    }
    return (
      super.isFilterable(nativeType, category) &&
      !ORACLE_FILTER_DENYLIST.has(oracleTypeName(nativeType).toLowerCase())
    );
  }
  override quoteIdentifier(name: string): string {
    return `"${name.replace(/"/g, '""')}"`;
  }
  buildTableDataOrderExpression(
    columnName: string,
    tableReference: string,
  ): string {
    return `${tableReference}.${this.quoteIdentifier(columnName)}`;
  }
  buildTableDataProjection(columns: readonly ColumnTypeMeta[]): string {
    return columns
      .map(
        (column) =>
          `${this.buildVerificationValueExpr(column)} AS ${this.quoteIdentifier(column.name)}`,
      )
      .join(", ");
  }
  buildVerificationValueExpr(column: ColumnTypeMeta): string {
    const identifier = this.quoteIdentifier(column.name);
    if (oracleTypeName(column.nativeType) === "XMLTYPE") {
      return `XMLSERIALIZE(CONTENT ${identifier} AS CLOB NO INDENT)`;
    }
    if (oracleTypeName(column.nativeType) === "JSON") {
      return `JSON_SERIALIZE(${identifier} RETURNING CLOB)`;
    }
    if (column.category === "other" && column.objectAttributes?.length) {
      return `CASE WHEN ${identifier} IS NULL THEN NULL ELSE JSON_OBJECT(${identifier} RETURNING CLOB) END`;
    }
    const typeName = oracleTypeName(column.nativeType);
    const format =
      typeName === "DATE"
        ? "YYYY-MM-DD HH24:MI:SS"
        : typeName.startsWith("TIMESTAMP") &&
            typeName !== "TIMESTAMP WITH LOCAL TIME ZONE"
          ? isTimezoneAwareOracleTemporal(column.nativeType)
            ? "YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM"
            : "YYYY-MM-DD HH24:MI:SS.FF9"
          : null;
    if (typeName === "TIMESTAMP WITH LOCAL TIME ZONE") {
      return `CASE WHEN ${identifier} IS NULL THEN NULL ELSE TO_CHAR(${identifier} AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.FF9') || 'Z' END`;
    }
    return format ? `TO_CHAR(${identifier}, '${format}')` : identifier;
  }
  override qualifiedTableName(
    _database: string,
    schema: string,
    table: string,
  ): string {
    return `${this.quoteIdentifier(schema)}.${this.quoteIdentifier(table)}`;
  }
  override buildPagination(
    offset: number,
    limit: number,
    paramIndex: number,
  ): PaginationResult {
    return {
      sql: `OFFSET :${paramIndex} ROWS FETCH NEXT :${paramIndex + 1} ROWS ONLY`,
      params: [offset, limit],
    };
  }
  override buildInsertDefaultValuesSql(
    qualifiedTableName: string,
    columns?: readonly ColumnTypeMeta[],
  ): string {
    if (!columns || columns.length === 0) {
      throw new Error(
        "Oracle insert failed: the table has no insertable columns to provide default values for.",
      );
    }
    const columnNames = columns.map((column) =>
      this.quoteIdentifier(column.name),
    );
    const defaults = columns.map(() => "DEFAULT");
    return `INSERT INTO ${qualifiedTableName} (${columnNames.join(", ")}) VALUES (${defaults.join(", ")})`;
  }
  override buildInsertValueExpr(
    column: ColumnTypeMeta,
    paramIndex: number,
  ): string {
    if (oracleTypeName(column.nativeType) === "DATE")
      return `TO_DATE(:${paramIndex}, 'YYYY-MM-DD HH24:MI:SS')`;
    if (this.isDatetimeWithTime(column.nativeType)) {
      return isTimezoneAwareOracleTemporal(column.nativeType)
        ? `TO_TIMESTAMP_TZ(:${paramIndex}, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')`
        : `TO_TIMESTAMP(:${paramIndex}, 'YYYY-MM-DD HH24:MI:SS.FF')`;
    }
    return `:${paramIndex}`;
  }
  buildMutationValueExpr(
    column: ColumnTypeMeta,
    value: unknown,
    _operation: "insert" | "update",
    _originalValue?: unknown,
    paramIndex?: number,
  ): MutationSqlExpression | null {
    if (oracleTypeName(column.nativeType) === "XMLTYPE") {
      if (value === null || value === NULL_SENTINEL)
        return { sql: "NULL", params: [] };
      if (paramIndex === undefined)
        throw new Error("Oracle XML mutation requires a bind parameter index.");
      return {
        sql: `XMLTYPE(:${paramIndex})`,
        params: [value],
        previewColumns: [{ ...column, nativeType: "CLOB" }],
      };
    }
    if (column.collectionKind === "varray") {
      if (value === null || value === NULL_SENTINEL) {
        return { sql: "NULL", params: [] };
      }
      if (paramIndex === undefined) {
        throw new Error(
          "Oracle VARRAY mutation requires a bind parameter index.",
        );
      }
      if (!column.collectionTypeOwner || !column.collectionTypeName) {
        throw new TypeError(
          `Oracle VARRAY type metadata for column "${column.name}" is unavailable. Refresh the table schema and retry.`,
        );
      }
      const values = parseOracleVarrayTextValues(value, column);
      const typeName = `${this.quoteIdentifier(column.collectionTypeOwner)}.${this.quoteIdentifier(column.collectionTypeName)}`;
      return {
        sql: `${typeName}(${values
          .map(
            (_entry, index) =>
              `JSON_VALUE(:${paramIndex + index}, '$' RETURNING VARCHAR2(4000))`,
          )
          .join(", ")})`,
        params: values.map((entry) => JSON.stringify(entry)),
      };
    }
    if (
      !column.objectTypeOwner ||
      !column.objectTypeName ||
      !column.objectAttributes
    ) {
      return null;
    }
    if (paramIndex === undefined) {
      throw new Error(
        "Oracle object mutation requires a bind parameter index.",
      );
    }
    if (value === null || value === NULL_SENTINEL)
      return { sql: "NULL", params: [] };
    const normalized = parseOracleObjectRecord(
      value,
      column.objectAttributes,
      column.name,
    );
    const typeName = `${this.quoteIdentifier(column.objectTypeOwner)}.${this.quoteIdentifier(column.objectTypeName)}`;
    const argumentsSql = column.objectAttributes
      .map((attribute, index) =>
        oracleObjectJsonAttributeExpr(paramIndex + index, attribute),
      )
      .join(", ");
    return {
      sql: `${typeName}(${argumentsSql})`,
      params: column.objectAttributes.map(() => JSON.stringify(normalized)),
    };
  }
  override buildCursorComparison(
    column: ColumnTypeMeta,
    operator: "eq" | "gt" | "lt",
    rawValue: unknown,
    paramIndex: number,
  ): FilterConditionResult {
    if (!this.isDatetimeWithTime(column.nativeType)) {
      return super.buildCursorComparison(
        column,
        operator,
        rawValue,
        paramIndex,
      );
    }
    const timezoneAware = isTimezoneAwareOracleTemporal(column.nativeType);
    const dateOnly = oracleTypeName(column.nativeType) === "DATE";
    const text =
      dateOnly && typeof rawValue === "string" && isValidDateOnly(rawValue)
        ? `${rawValue} 00:00:00`
        : normalizeOracleTemporalValue(rawValue, {
            preserveExplicitTimezoneText: !timezoneAware,
          });
    if (text === null) {
      throw new Error(`Invalid temporal export cursor for ${column.name}.`);
    }
    const name = this.quoteIdentifier(column.name);
    const expression = timezoneAware
      ? `SYS_EXTRACT_UTC(CAST(${name} AS TIMESTAMP(9) WITH TIME ZONE))`
      : name;
    const bind = dateOnly
      ? `TO_DATE(:${paramIndex}, 'YYYY-MM-DD HH24:MI:SS')`
      : `TO_TIMESTAMP(:${paramIndex}, 'YYYY-MM-DD HH24:MI:SS.FF')`;
    return {
      sql: `${expression} ${this.sqlOperator(operator)} ${bind}`,
      params: [dateOnly ? text.replace(/\.\d+$/, "") : text],
    };
  }
  override buildSetExpr(column: ColumnTypeMeta, paramIndex: number): string {
    return `${this.quoteIdentifier(column.name)} = ${this.buildInsertValueExpr(column, paramIndex)}`;
  }
  override buildOriginalValueComparison(
    column: ColumnTypeMeta,
    paramIndex: number,
  ): string {
    const name = this.quoteIdentifier(column.name);
    if (column.objectAttributes?.length) {
      return column.objectAttributes
        .map(
          (attribute) =>
            `DECODE(${name}.${this.quoteIdentifier(attribute.name)}, ${oracleObjectJsonAttributeExpr(paramIndex, attribute)}, 1, 0) = 1`,
        )
        .join(" AND ");
    }
    const type = oracleTypeName(column.nativeType);
    if (type === "JSON") {
      return `JSON_EQUAL(${name}, :${paramIndex} ERROR ON ERROR)`;
    }
    if (this.isDatetimeWithTime(column.nativeType)) {
      return `${name} = ${this.buildInsertValueExpr(column, paramIndex)}`;
    }
    if (type === "CLOB" || type === "NCLOB" || type === "BLOB") {
      return `DBMS_LOB.COMPARE(${name}, TO_${type}(:${paramIndex})) = 0`;
    }
    if (type === "XMLTYPE") {
      return `DBMS_LOB.COMPARE(XMLSERIALIZE(CONTENT ${name} AS CLOB NO INDENT), XMLSERIALIZE(CONTENT XMLTYPE(:${paramIndex}) AS CLOB NO INDENT)) = 0`;
    }
    if (type === "SDO_GEOMETRY") {
      return `DBMS_LOB.COMPARE(SDO_UTIL.TO_WKTGEOMETRY(${name}), TO_CLOB(:${paramIndex})) = 0`;
    }
    if (type === "LONG" || type === "LONG RAW") {
      throw new Error(
        `Optimistic conflict detection is not supported for Oracle ${type} columns. Convert ${column.name} to a LOB before editing.`,
      );
    }
    return super.buildOriginalValueComparison(column, paramIndex);
  }
  buildOriginalValueComparisonWithParams(
    column: ColumnTypeMeta,
    value: unknown,
    paramIndex: number,
    tableAlias?: string,
  ): MutationSqlExpression | null {
    if (oracleTypeName(column.nativeType) === "JSON") {
      const jsonColumn = tableAlias
        ? `${this.quoteIdentifier(tableAlias)}.${this.quoteIdentifier(column.name)}`
        : this.quoteIdentifier(column.name);
      return {
        sql: `JSON_EQUAL(${jsonColumn}, :${paramIndex} ERROR ON ERROR)`,
        params: [value],
      };
    }
    if (oracleTypeName(column.nativeType) === "XMLTYPE") {
      return {
        sql: this.buildOriginalValueComparison(column, paramIndex),
        params: [value],
        previewColumns: [{ ...column, nativeType: "CLOB" }],
      };
    }
    if (column.collectionKind === "varray") {
      const values = parseOracleVarrayTextValues(value, column);
      const objectColumn = tableAlias
        ? `${this.quoteIdentifier(tableAlias)}.${this.quoteIdentifier(column.name)}`
        : this.quoteIdentifier(column.name);
      const expectedJson = `JSON_TABLE(:${paramIndex}, '$[*]' COLUMNS (ORDINALITY FOR ORDINALITY, ITEM_VALUE VARCHAR2(4000) PATH '$' NULL ON ERROR)) RAPIDB_EXPECTED`;
      return {
        sql: `${objectColumn} IS NOT NULL AND DBMS_LOB.COMPARE(NVL((SELECT JSON_ARRAYAGG(RAPIDB_ELEMENT.COLUMN_VALUE ORDER BY ROWNUM NULL ON NULL RETURNING CLOB) FROM TABLE(${objectColumn}) RAPIDB_ELEMENT), TO_CLOB('[]')), NVL((SELECT JSON_ARRAYAGG(RAPIDB_EXPECTED.ITEM_VALUE ORDER BY RAPIDB_EXPECTED.ORDINALITY NULL ON NULL RETURNING CLOB) FROM ${expectedJson}), TO_CLOB('[]'))) = 0`,
        params: [JSON.stringify(values)],
      };
    }
    if (!column.objectAttributes?.length) return null;
    const objectColumn = tableAlias
      ? `${this.quoteIdentifier(tableAlias)}.${this.quoteIdentifier(column.name)}`
      : this.quoteIdentifier(column.name);
    const expressions = column.objectAttributes.map(
      (attribute, index) =>
        `DECODE(${objectColumn}.${this.quoteIdentifier(attribute.name)}, ${oracleObjectJsonAttributeExpr(paramIndex + index, attribute)}, 1, 0) = 1`,
    );
    return {
      sql: `${objectColumn} IS NOT NULL AND ${expressions.join(" AND ")}`,
      params: column.objectAttributes.map(() => value),
    };
  }
  getUpdateTableAlias(): string {
    return "RAPIDB_UPDATE_ROW";
  }
  materializePreviewInsertSql(
    sql: string,
    params: readonly unknown[] | undefined,
    columns: readonly ColumnTypeMeta[],
  ): string {
    if (!params || params.length === 0 || columns.length === 0) {
      return this.materializePreviewSql(sql, params);
    }
    return this.materializePreviewColumnSql(sql, params, columns);
  }
  materializePreviewColumnSql(
    sql: string,
    params: readonly unknown[] | undefined,
    columns: readonly (ColumnTypeMeta | undefined)[],
  ): string {
    if (!params || params.length === 0) {
      return sql;
    }
    const offsets = indexedPlaceholderOffsets(sql, ":", { dialect: "oracle" });
    return replaceIndexedPlaceholders(sql, offsets, ({ index, text }) => {
      if (index < 0 || index >= params.length) {
        return text;
      }
      return this.formatOracleColumnAwarePreviewLiteral(
        params[index],
        columns[index],
      );
    });
  }
  private formatOracleColumnAwarePreviewLiteral(
    value: unknown,
    column: ColumnTypeMeta | undefined,
  ): string {
    if (column !== undefined) {
      if (value instanceof Date && !Number.isNaN(value.getTime())) {
        const typeName = oracleTypeName(column.nativeType);
        if (typeName === "DATE") {
          const temporalText = oracleTimestampPreviewText(value, true).slice(
            0,
            19,
          );
          return `TO_DATE('${escapeSqlPreviewStringLiteral(temporalText)}', 'YYYY-MM-DD HH24:MI:SS')`;
        }
        if (isTimezoneAwareOracleTemporal(column.nativeType)) {
          const temporalText = oracleTimestampPreviewText(value, true);
          return `TO_TIMESTAMP_TZ('${escapeSqlPreviewStringLiteral(temporalText)} +00:00', 'YYYY-MM-DD HH24:MI:SS.FF3 TZH:TZM')`;
        }
        if (typeName.startsWith("TIMESTAMP")) {
          const temporalText = oracleTimestampPreviewText(value, false);
          return `TO_TIMESTAMP('${escapeSqlPreviewStringLiteral(temporalText)}', 'YYYY-MM-DD HH24:MI:SS.FF3')`;
        }
      }
      if (typeof value === "string") {
        const typeName = oracleTypeName(column.nativeType);
        if (typeName === "XMLTYPE") {
          return formatOracleXmlPreviewExpression(value);
        }
        if (typeName === "CLOB") {
          return formatOracleLobPreviewExpression(value, {
            converter: "TO_CLOB",
          });
        }
        if (typeName === "NCLOB") {
          return formatOracleLobPreviewExpression(value, {
            converter: "TO_NCLOB",
            useNationalCharacterSet: true,
          });
        }
        if (typeName === "NCHAR" || typeName === "NVARCHAR2") {
          return formatSqlPreviewStringLiteral(value, "N");
        }
      }
    }
    if (Buffer.isBuffer(value)) {
      return `HEXTORAW('${formatHexSqlPreviewLiteral(value)}')`;
    }
    if (value instanceof ArrayBuffer) {
      return `HEXTORAW('${formatHexSqlPreviewLiteral(Buffer.from(new Uint8Array(value)))}')`;
    }
    if (ArrayBuffer.isView(value)) {
      return `HEXTORAW('${formatHexSqlPreviewLiteral(Buffer.from(value.buffer, value.byteOffset, value.byteLength))}')`;
    }
    return this.formatPreviewSqlLiteral(value);
  }
  protected override formatPreviewSqlLiteral(value: unknown): string {
    if (value instanceof Date && !Number.isNaN(value.getTime())) {
      return oracleTimestampPreviewLiteral(value);
    }
    if (Buffer.isBuffer(value)) {
      return `HEXTORAW('${formatHexSqlPreviewLiteral(value)}')`;
    }
    if (value instanceof ArrayBuffer) {
      return `HEXTORAW('${formatHexSqlPreviewLiteral(Buffer.from(new Uint8Array(value)))}')`;
    }
    if (ArrayBuffer.isView(value)) {
      return `HEXTORAW('${formatHexSqlPreviewLiteral(Buffer.from(value.buffer, value.byteOffset, value.byteLength))}')`;
    }
    return super.formatPreviewSqlLiteral(value);
  }
  override coerceInputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (value === null || value === undefined || value === "") return value;
    if (value === NULL_SENTINEL) return null;
    if (value instanceof Date && this.isDatetimeWithTime(column.nativeType)) {
      const text = oracleTemporalBindText(value, column);
      if (text === null)
        throw new TypeError(
          `Column "${column.name}" expects a valid Oracle temporal value.`,
        );
      return text;
    }
    if (typeof value !== "string") return value;
    if (column.category === "float") {
      const trimmed = value.trim();
      if (trimmed !== "") {
        const parsed = Number(trimmed);
        if (Number.isFinite(parsed)) return parsed;
      }
    }
    if (column.category === "binary")
      return super.coerceInputValue(value, column);
    if (this.isDatetimeWithTime(column.nativeType)) {
      const text = oracleTemporalBindText(value, column);
      if (text === null)
        throw new TypeError(
          `Column "${column.name}" expects a valid Oracle temporal value.`,
        );
      return text;
    }
    return value;
  }
  coerceOriginalValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (column.objectTypeOwner && column.objectAttributes && value !== null) {
      return oracleObjectJsonValue(value, column);
    }
    return this.coerceInputValue(value, column);
  }
  override normalizeFilterValue(
    column: ColumnTypeMeta,
    operator: FilterOperator,
    value: string | [string, string] | undefined,
  ): string | [string, string] | undefined {
    const normalized = super.normalizeFilterValue(column, operator, value);
    if (oracleTypeName(column.nativeType) !== "XMLTYPE") {
      return normalized;
    }
    if (typeof normalized === "string") {
      return normalizeOracleXmlValue(normalized) ?? normalized;
    }
    return normalized;
  }
  override formatOutputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (value === null || value === undefined) return value;
    if (Buffer.isBuffer(value)) return super.formatOutputValue(value, column);
    if (typeof value === "bigint") return value.toString();
    if (oracleTypeName(column.nativeType) === "DATE") {
      if (typeof value === "string") {
        const match = /^(\d{4}-\d{2}-\d{2})(?:[ T](\d{2}:\d{2}:\d{2}))?$/.exec(
          value.trim(),
        );
        if (match?.[1] && isValidDateOnly(match[1])) {
          return match[2] ? `${match[1]} ${match[2]}` : match[1];
        }
      }
      const formatted = formatOracleWallClockDate(value);
      if (formatted !== null) return formatted;
    }
    if (column.category === "float") {
      const normalizedFloat = normalizeOracleFloatValue(value);
      if (normalizedFloat !== null) return normalizedFloat;
    }
    if (isOracleIntervalType(column.nativeType)) {
      const intervalText = normalizeOracleIntervalValue(value);
      if (intervalText !== null) return intervalText;
    }
    if (oracleTypeName(column.nativeType) === "XMLTYPE") {
      const xmlText = normalizeOracleXmlValue(value);
      if (xmlText !== null) return xmlText;
    }
    if (this.isDatetimeWithTime(column.nativeType)) {
      if (
        value instanceof Date &&
        isTimezoneAwareOracleTemporal(column.nativeType)
      ) {
        return oracleTemporalBindText(value, column) ?? value;
      }
      if (
        value instanceof Date &&
        !isTimezoneAwareOracleTemporal(column.nativeType)
      ) {
        const formatted = formatOracleWallClockDate(value);
        if (formatted !== null) return formatted;
      }
      const timezoneAware = isTimezoneAwareOracleTemporal(column.nativeType);
      const formatted = normalizeOracleTemporalValue(value, {
        preserveExplicitTimezoneText: timezoneAware,
        includeExplicitTimezone: timezoneAware,
      });
      if (formatted !== null) return formatted;
    }
    return super.formatOutputValue(value, column);
  }
  override checkPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null {
    if (column.collectionKind === "varray") {
      if (expectedValue === null || expectedValue === NULL_SENTINEL) {
        if (options === undefined) return { ok: true, shouldVerify: true };
        return options.persistedValue === null
          ? { ok: true, shouldVerify: true }
          : {
              ok: false,
              shouldVerify: true,
              message: `Oracle VARRAY column "${column.name}" did not match the requested NULL value after it was read back.`,
            };
      }
      const canonicalize = (value: unknown): string | null => {
        try {
          return canonicalOracleVarrayTextValue(value, column);
        } catch {
          return null;
        }
      };
      const expected = canonicalize(expectedValue);
      if (expected === null) {
        return {
          ok: false,
          shouldVerify: false,
          message: `Column "${column.name}" expects a valid Oracle VARRAY value.`,
        };
      }
      if (options === undefined) return { ok: true, shouldVerify: true };
      if (canonicalize(options.persistedValue) === expected) {
        return { ok: true, shouldVerify: true };
      }
      return {
        ok: false,
        shouldVerify: true,
        message: `Oracle VARRAY column "${column.name}" did not match the requested value after it was read back.`,
      };
    }
    if (oracleTypeName(column.nativeType) === "JSON") {
      const isNull = expectedValue === null || expectedValue === NULL_SENTINEL;
      if (isNull) {
        return options === undefined || options.persistedValue === null
          ? { ok: true, shouldVerify: true }
          : {
              ok: false,
              shouldVerify: true,
              message: `Oracle JSON column "${column.name}" did not preserve SQL NULL.`,
            };
      }
      const canonicalize = (value: unknown): string | null => {
        try {
          const jsonText =
            typeof value === "string"
              ? value
              : value !== null && typeof value === "object"
                ? JSON.stringify(value)
                : null;
          return jsonText === undefined || jsonText === null
            ? null
            : canonicalizeJsonNumericValues(jsonText, {
                requireUniqueKeys: true,
              });
        } catch {
          return null;
        }
      };
      const expected = canonicalize(expectedValue);
      if (expected === null) {
        return {
          ok: false,
          shouldVerify: false,
          message: `Column "${column.name}" expects a valid Oracle JSON value.`,
        };
      }
      if (options === undefined) return { ok: true, shouldVerify: true };
      if (canonicalize(options.persistedValue) === expected) {
        return { ok: true, shouldVerify: true };
      }
      return {
        ok: false,
        shouldVerify: true,
        message: `Oracle JSON column "${column.name}" did not match the requested value after it was read back.`,
      };
    }
    const isNamedObject =
      Boolean(column.objectTypeOwner || column.objectTypeName) ||
      (column.category === "other" &&
        canonicalOracleJsonObject(expectedValue) !== null);
    if (isNamedObject) {
      if (expectedValue === null || expectedValue === NULL_SENTINEL) {
        return options === undefined || options.persistedValue === null
          ? { ok: true, shouldVerify: true }
          : {
              ok: false,
              shouldVerify: true,
              message: `Oracle object column "${column.name}" did not preserve SQL NULL.`,
            };
      }
      const canonicalize = (value: unknown): string | null => {
        try {
          return column.objectAttributes?.length
            ? canonicalOracleObjectValue(value, column)
            : canonicalOracleJsonObject(value);
        } catch {
          return null;
        }
      };
      const expected = canonicalize(expectedValue);
      if (expected === null) {
        return {
          ok: false,
          shouldVerify: false,
          message: `Column "${column.name}" expects a valid Oracle object value.`,
        };
      }
      if (options === undefined) return { ok: true, shouldVerify: true };
      const actual = canonicalize(options.persistedValue);
      if (actual === expected) return { ok: true, shouldVerify: true };
      return {
        ok: false,
        shouldVerify: true,
        message: `Oracle object value did not match the requested value after it was read back.`,
      };
    }
    const baseType = column.nativeType.toUpperCase().split("(")[0].trim();
    if (column.category === "integer") {
      const constraint =
        baseType === "NUMBER"
          ? (() => {
              const parsed = this.parseExactNumericConstraint(
                column.nativeType,
              );
              return {
                precision: parsed.precision,
                scale: parsed.scale ?? 0,
              };
            })()
          : { precision: null, scale: 0 };
      return this.checkExactNumericPersistedEdit(
        column,
        expectedValue,
        constraint,
        options,
      );
    }
    if (column.category === "decimal") {
      if (baseType !== "NUMBER") {
        return null;
      }
      return this.checkExactNumericPersistedEdit(
        column,
        expectedValue,
        this.parseExactNumericConstraint(column.nativeType),
        options,
      );
    }
    if (column.category === "float") {
      const significantDigits = oracleFloatPrecision(column.nativeType);
      if (significantDigits !== null) {
        return this.checkApproximateNumericPersistedEdit(
          column,
          expectedValue,
          significantDigits,
          options,
        );
      }
      return this.checkTextPersistedEdit(column, expectedValue, options);
    }
    if (column.category === "binary") {
      return this.checkBinaryPersistedEdit(column, expectedValue, options);
    }
    if (
      column.category === "date" ||
      column.category === "time" ||
      column.category === "datetime"
    ) {
      const type = oracleTypeName(column.nativeType);
      const precision =
        type === "DATE"
          ? 0
          : Number(
              /TIMESTAMP(?:\((\d+)\))?/i.exec(column.nativeType)?.[1] ?? 6,
            );
      const text =
        expectedValue === null ||
        expectedValue === NULL_SENTINEL ||
        expectedValue === ""
          ? null
          : oracleTemporalBindText(expectedValue, column);
      const fraction = text
        ? (normalizeOracleTemporalText(text)?.text.split(".")[1] ?? "")
        : "";
      if (fraction.length > precision) {
        return {
          ok: false,
          shouldVerify: false,
          message: `Column "${column.name}" supports at most ${precision} fractional second digits.`,
        };
      }
      return this.checkNormalizedPersistedEdit(
        column,
        expectedValue,
        options,
        (value) => {
          const canonical = canonicalOracleTemporalEdit(value, column);
          return canonical === null ? null : { canonical };
        },
        `Column "${column.name}" expects a valid Oracle temporal value.`,
      );
    }
    if (column.category === "text") {
      if (oracleTypeName(column.nativeType) === "XMLTYPE") {
        return this.checkNormalizedPersistedEdit(
          column,
          expectedValue,
          options,
          (value) => {
            if (value === NULL_SENTINEL || value === null) {
              return { canonical: "\u0000__RAPIDB_ORACLE_XML_NULL__\u0000" };
            }
            if (typeof value !== "string") {
              return null;
            }
            return {
              canonical: normalizeOracleXmlValue(value) ?? value.trim(),
            };
          },
          `Column "${column.name}" expects XML text.`,
        );
      }
      if (["CHAR", "NCHAR"].includes(baseType)) {
        return this.checkFixedWidthCharPersistedEdit(
          column,
          expectedValue,
          options,
        );
      }
      return this.checkTextPersistedEdit(column, expectedValue, options);
    }
    return null;
  }
  override buildFilterCondition(
    column: ColumnTypeMeta,
    operator: FilterOperator,
    value: string | [string, string] | undefined,
    paramIndex: number,
  ): FilterConditionResult | null {
    const preamble = this.createFilterConditionPreamble(
      column,
      operator,
      value,
    );
    if (!preamble) return null;
    if (preamble.kind === "resolved") return preamble.condition;
    const col = preamble.columnSql;
    const val = preamble.value;
    if (column.collectionKind === "varray") {
      if (
        !["CHAR", "NCHAR", "VARCHAR2", "NVARCHAR2"].includes(
          oracleTypeName(column.collectionElementType ?? ""),
        ) ||
        (operator !== "like" && operator !== "ilike")
      ) {
        return null;
      }
      const searchValue = typeof val === "string" ? val : val[0];
      return {
        sql: `EXISTS (SELECT 1 FROM TABLE(${col}) rapidb_element WHERE UPPER(rapidb_element.COLUMN_VALUE) LIKE UPPER(:${paramIndex}) ESCAPE '!')`,
        params: [literalContainsPattern(searchValue)],
      };
    }
    if (
      oracleTypeName(column.nativeType) === "JSON" &&
      (operator === "like" || operator === "ilike")
    ) {
      const searchValue = typeof val === "string" ? val : val[0];
      if (!searchValue.trim()) {
        return null;
      }
      return {
        sql: `DBMS_LOB.INSTR(UPPER(JSON_SERIALIZE(${col} RETURNING CLOB)), UPPER(:${paramIndex})) > 0`,
        params: [searchValue.trim()],
      };
    }
    if (
      column.category === "binary" &&
      typeof val === "string" &&
      (operator === "eq" || operator === "neq")
    ) {
      const bind = `:${paramIndex}`;
      const typeName = oracleTypeName(column.nativeType);
      if (typeName === "BLOB") {
        return {
          sql:
            operator === "neq"
              ? `NVL(DBMS_LOB.COMPARE(${col}, ${bind}), 1) != 0`
              : `NVL(DBMS_LOB.COMPARE(${col}, ${bind}), 1) = 0`,
          params: [this.coerceInputValue(val, column)],
        };
      }
      const sqlOp = operator === "neq" ? "!=" : "=";
      return {
        sql: `${col} ${sqlOp} ${bind}`,
        params: [this.coerceInputValue(val, column)],
      };
    }
    if (column.category === "spatial" && typeof val === "string") {
      if (operator !== "eq" && operator !== "neq") {
        return null;
      }
      const searchValue = val.trim();
      if (!searchValue) {
        return null;
      }
      const spatialExpr = `SDO_UTIL.TO_WKTGEOMETRY(${col})`;
      const compareExpr = `NVL(DBMS_LOB.COMPARE(${spatialExpr}, TO_CLOB(:${paramIndex})), 1)`;
      return {
        sql: `${compareExpr} ${operator === "neq" ? "!=" : "="} 0`,
        params: [searchValue],
      };
    }
    if (oracleTypeName(column.nativeType) === "XMLTYPE") {
      if (
        operator !== "like" &&
        operator !== "ilike" &&
        operator !== "eq" &&
        operator !== "neq"
      ) {
        return null;
      }
      const xmlValue =
        typeof val === "string"
          ? (normalizeOracleXmlValue(val) ?? val)
          : val[0];
      const xmlExpr = oracleXmlFilterExpr(column);
      const sqlOp = operator === "neq" ? "NOT LIKE" : "LIKE";
      if (operator === "like" || operator === "ilike") {
        return {
          sql: `UPPER(${xmlExpr}) LIKE UPPER(:${paramIndex}) ESCAPE '!'`,
          params: [literalContainsPattern(xmlValue)],
        };
      }
      return {
        sql: `UPPER(${xmlExpr}) ${sqlOp} UPPER(:${paramIndex})`,
        params: [`%${xmlValue}%`],
      };
    }
    if (column.category === "array") {
      if (operator !== "like" && operator !== "ilike") {
        return null;
      }
      const arrayValue = typeof val === "string" ? val : val[0];
      return {
        sql: `UPPER(${col}) LIKE UPPER(:${paramIndex}) ESCAPE '!'`,
        params: [literalContainsPattern(arrayValue)],
      };
    }
    if (
      isOracleIntervalType(column.nativeType) &&
      typeof val === "string" &&
      (operator === "eq" || operator === "neq")
    ) {
      const sqlOp = operator === "neq" ? "<>" : "=";
      return {
        sql: `TRIM(CAST(${col} AS VARCHAR2(128))) ${sqlOp} :${paramIndex}`,
        params: [val.trim()],
      };
    }
    if (typeof val === "string" && (operator === "eq" || operator === "neq")) {
      const typeName = oracleTypeName(column.nativeType);
      if (typeName === "CLOB" || typeName === "NCLOB") {
        const converter = typeName === "NCLOB" ? "TO_NCLOB" : "TO_CLOB";
        return {
          sql:
            operator === "neq"
              ? `NVL(DBMS_LOB.COMPARE(${col}, ${converter}(:${paramIndex})), 1) != 0`
              : `NVL(DBMS_LOB.COMPARE(${col}, ${converter}(:${paramIndex})), 1) = 0`,
          params: [val],
        };
      }
    }
    if (
      this.isNumericCategory(column.category) &&
      typeof val === "string" &&
      !Number.isNaN(Number(val)) &&
      val !== ""
    ) {
      if (
        column.category === "float" &&
        (operator === "eq" || operator === "neq")
      ) {
        const numericValue = Number(val);
        const tolerance = oracleFloatFilterTolerance(column.nativeType);
        const valueBind = `:${paramIndex}`;
        const epsilonBind = `:${paramIndex + 1}`;
        const valueRepeatBind = `:${paramIndex + 2}`;
        const epsilonRepeatBind = `:${paramIndex + 3}`;
        const deltaExpr = `ABS(TO_BINARY_DOUBLE(${col}) - TO_BINARY_DOUBLE(${valueBind}))`;
        const toleranceExpr = `GREATEST(${epsilonBind}, ABS(TO_BINARY_DOUBLE(${valueRepeatBind})) * ${epsilonRepeatBind})`;
        return {
          sql:
            operator === "neq"
              ? `${deltaExpr} >= ${toleranceExpr}`
              : `${deltaExpr} < ${toleranceExpr}`,
          params: [numericValue, tolerance, numericValue, tolerance],
        };
      }
      const sqlOp = this.sqlOperator(operator);
      return {
        sql: `${col} ${sqlOp} :${paramIndex}`,
        params: [column.category === "float" ? Number(val) : val],
      };
    }
    if (this.isDatetimeWithTime(column.nativeType)) {
      const v = typeof val === "string" ? val : val[0];
      const temporalExpr = oracleTemporalFilterExpr(column);
      const normalizeComparable = (rawValue: string) => {
        const normalized =
          normalizeOracleTemporalValue(rawValue, {
            preserveExplicitTimezoneText: !isTimezoneAwareOracleTemporal(
              column.nativeType,
            ),
          }) ?? rawValue.trim();
        return oracleTypeName(column.nativeType) === "DATE"
          ? normalized.replace(/\.\d+$/, "")
          : normalized;
      };
      if (operator === "between" && Array.isArray(val)) {
        return {
          sql: `${temporalExpr} BETWEEN :${paramIndex} AND :${paramIndex + 1}`,
          params: [normalizeComparable(val[0]), normalizeComparable(val[1])],
        };
      }
      if (operator === "in") {
        const parts = v
          .split(",")
          .map((part) => normalizeComparable(part.trim()));
        return {
          sql: `${temporalExpr} IN (${parts.map((_, index) => `:${paramIndex + index}`).join(", ")})`,
          params: parts,
        };
      }
      const normalized =
        normalizeOracleTemporalValue(v, {
          preserveExplicitTimezoneText: !isTimezoneAwareOracleTemporal(
            column.nativeType,
          ),
        }) ?? v.trim();
      const comparable =
        oracleTypeName(column.nativeType) === "DATE"
          ? normalized.replace(/\.\d+$/, "")
          : normalized;
      if (["eq", "neq", "gt", "gte", "lt", "lte"].includes(operator)) {
        const sqlOp = operator === "neq" ? "<>" : this.sqlOperator(operator);
        return {
          sql: `${temporalExpr} ${sqlOp} :${paramIndex}`,
          params: [comparable],
        };
      }
      return {
        sql: `${temporalExpr} LIKE :${paramIndex}${operator === "like" || operator === "ilike" ? " ESCAPE '!'" : ""}`,
        params: [
          operator === "like" || operator === "ilike"
            ? literalContainsPattern(comparable)
            : `%${comparable}%`,
        ],
      };
    }
    if (operator === "between" && Array.isArray(val)) {
      return {
        sql: `${col} BETWEEN :${paramIndex} AND :${paramIndex + 1}`,
        params: [val[0], val[1]],
      };
    }
    if (operator === "in" && typeof val === "string") {
      const parts = val.split(",").map((s) => s.trim());
      return {
        sql: `${col} IN (${parts.map((_, i) => `:${paramIndex + i}`).join(", ")})`,
        params: parts,
      };
    }
    const v = typeof val === "string" ? val : val[0];
    if (operator === "like" || operator === "ilike") {
      return {
        sql: `UPPER(${col}) LIKE UPPER(:${paramIndex}) ESCAPE '!'`,
        params: [literalContainsPattern(v)],
      };
    }
    return {
      sql: `UPPER(${col}) LIKE UPPER(:${paramIndex})`,
      params: [`%${v}%`],
    };
  }
}
