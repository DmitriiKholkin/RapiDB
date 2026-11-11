import type { Connection as NativeMysqlConnection, Query } from "mysql2";
import type {
  ConnectionOptions,
  FieldPacket,
  Pool,
  PoolConnection,
  PoolOptions,
  QueryOptions,
  ResultSetHeader,
  RowDataPacket,
} from "mysql2/promise";
import * as mysql from "mysql2/promise";
import type { DdlOnlyDbObjectKind } from "../../shared/dbObjectKinds";
import type { OperationCancellationContext } from "../../shared/safetyContracts";
import type { QueryResultSet } from "../../shared/webviewContracts";
import type { ConnectionConfig } from "../connectionManager";
import { getSshTcpForwardTransport } from "../driverRuntimeConfig";
import { resolveConnectionTlsSettings } from "../services/connectionTls";
import { SqlTlsSocket } from "../services/sqlTlsSocket";
import { buildWhere } from "../table/filterSql";
import { normalizeUnknownError } from "../utils/errorHandling";
import { logger } from "../utils/logger";
import { splitMySQLScript } from "../utils/mysqlScriptSplit";
import { isMysqlDashCommentStart } from "../utils/sqlStatementScan";
import { BaseDBDriver, formatDatetimeForDisplay } from "./BaseDBDriver";
import { queryCollectionLimit } from "./boundedQueryRows";
import { closeDriverResource } from "./driverCleanup";
import { exactNumericFilterLiteral } from "./exactNumericFilter";
import { literalContainsPattern } from "./literalContains";
import {
  getMetadataContext,
  runWithMetadataCancellation,
  withMetadataResource,
} from "./metadataCancellation";
import { MutationNotExecutedError } from "./mutationExecutionState";
import {
  createMysqlStreamingQuery,
  discardMysqlResultHistory,
} from "./mysqlBoundedQuery";
import { formatMysqlQuery, formatMysqlStringLiteral } from "./mysqlQueryFormat";
import {
  canonicalizeExactNumeric,
  numberToDecimalString,
} from "./numericUtils";
import {
  DriverTimeoutError,
  type DriverTimeoutSettingsProvider,
  throwIfTransactionCancelled,
} from "./timeout";
import {
  TransactionIdentityStore,
  TransactionVerificationError,
  verifyTransaction,
} from "./transactionVerification";
import type {
  ColumnMeta,
  ColumnTypeMeta,
  DatabaseInfo,
  DriverEntityManifest,
  DriverTablePageRequest,
  DriverTablePageResult,
  FilterConditionResult,
  FilterOperator,
  PersistedEditCheckOptions,
  PersistedEditCheckResult,
  QueryColumnMeta,
  QueryExecutionOptions,
  QueryResult,
  SchemaInfo,
  TableInfo,
  TypeCategory,
  ValueSemantics,
} from "./types";
import {
  assertTransactionAffectedRows,
  DATETIME_SQL_RE,
  ISO_DATETIME_RE,
  NULL_SENTINEL,
} from "./types";

const MYSQL_ENTITY_MANIFEST: DriverEntityManifest = {
  dbObjectKinds: ["table", "view", "function", "procedure"],
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
      triggers: "not_applicable",
    },
  },
};

const MYSQL_MAX_RETAINED_RESULT_SETS = 100;
const MYSQL_MAX_RETAINED_RESULT_COLUMNS = 4096;
const MYSQL_JSON_FIELD_TYPE = 245;

function mysqlQueryColumnMeta(field: FieldPacket): QueryColumnMeta {
  const nativeType = field.type ?? field.columnType;
  const typeName = field.typeName?.trim().toLowerCase();
  const extendedTypeName = field.extendedTypeName?.trim().toLowerCase();
  const extendedFormat = field.extendedFormat?.trim().toLowerCase();
  const numericTypeNames = [typeName, extendedTypeName, extendedFormat];

  if (
    nativeType === 0 ||
    nativeType === 246 ||
    numericTypeNames.some((name) =>
      name ? ["decimal", "newdecimal", "numeric"].includes(name) : false,
    )
  ) {
    return { category: "decimal" };
  }
  if (
    [1, 2, 3, 8, 9, 13, 16].includes(nativeType ?? -1) ||
    numericTypeNames.some((name) =>
      name
        ? [
            "tiny",
            "tinyint",
            "short",
            "smallint",
            "long",
            "int",
            "integer",
            "mediumint",
            "longlong",
            "bigint",
            "int24",
            "year",
            "bit",
          ].includes(name)
        : false,
    )
  ) {
    return { category: "integer" };
  }
  if (
    [4, 5].includes(nativeType ?? -1) ||
    numericTypeNames.some((name) =>
      name ? ["float", "double", "real"].includes(name) : false,
    )
  ) {
    return { category: "float" };
  }
  if (
    [7, 12].includes(nativeType ?? -1) ||
    numericTypeNames.some((name) =>
      name ? ["datetime", "timestamp"].includes(name) : false,
    )
  ) {
    return { category: "datetime" };
  }

  return {
    category:
      nativeType === MYSQL_JSON_FIELD_TYPE ||
      typeName === "json" ||
      extendedTypeName === "json" ||
      extendedFormat === "json"
        ? "json"
        : null,
  };
}

interface MysqlCapturedResultSet {
  fields: FieldPacket[];
  rows: unknown[][];
  rowCount: number;
  truncated: boolean;
  protocolResultIndex: number;
  partial?: boolean;
  completed: boolean;
}

function isQueryResult(value: unknown): value is QueryResult {
  if (typeof value !== "object" || value === null) return false;
  const result = value as Partial<QueryResult>;
  return (
    Array.isArray(result.columns) &&
    Array.isArray(result.rows) &&
    typeof result.rowCount === "number" &&
    typeof result.executionTimeMs === "number"
  );
}

function isMysqlServerQueryError(error: unknown): error is Error {
  if (!(error instanceof Error)) return false;
  const mysqlError = error as Error & {
    code?: unknown;
    errno?: unknown;
    sqlState?: unknown;
    fatal?: unknown;
  };
  return (
    mysqlError.fatal !== true &&
    (typeof mysqlError.sqlState === "string" ||
      (typeof mysqlError.code === "string" &&
        mysqlError.code.startsWith("ER_") &&
        typeof mysqlError.errno === "number"))
  );
}

function mayMysqlServerHaveRolledBackTransaction(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const native = error as {
    code?: unknown;
    errno?: unknown;
    sqlState?: unknown;
  };
  return (
    native.code === "ER_LOCK_DEADLOCK" ||
    native.errno === 1213 ||
    native.code === "ER_LOCK_WAIT_TIMEOUT" ||
    native.errno === 1205 ||
    native.sqlState === "40001"
  );
}

function hasJsonValidColumnCheck(
  checkClauses: readonly string[],
  columnName: string,
): boolean {
  const escapeRegex = (value: string) =>
    value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const quotedColumn = `\`${escapeRegex(columnName.replace(/`/g, "``"))}\``;
  const columnReference = /^[a-z_][a-z\d_$]*$/i.test(columnName)
    ? `(?:${quotedColumn}|${escapeRegex(columnName)})`
    : quotedColumn;
  const canonicalCheck = new RegExp(
    `^json_valid\\s*\\(\\s*${columnReference}\\s*\\)$`,
    "i",
  );
  return checkClauses.some((clause) => {
    let expression = clause.trim();
    while (expression.startsWith("(") && expression.endsWith(")")) {
      expression = expression.slice(1, -1).trim();
    }
    return canonicalCheck.test(expression);
  });
}

function isUnavailableCheckMetadataError(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  const mysqlError = error as Error & { code?: unknown; errno?: unknown };
  return (
    mysqlError.code === "ER_UNKNOWN_TABLE" ||
    mysqlError.code === "ER_NO_SUCH_TABLE" ||
    mysqlError.code === "ER_BAD_FIELD_ERROR" ||
    mysqlError.errno === 1054 ||
    mysqlError.errno === 1109 ||
    mysqlError.errno === 1146
  );
}

type MysqlSslOptions = Exclude<PoolOptions["ssl"], string | undefined>;

export { splitMySQLScript };

const MYSQL_EXECUTABLE_COMMENT_START = /\/\*M?!\d*/y;
const MYSQL_WORD = /[\w$\u0080-\uffff]+/y;
const MYSQL_SET_STATEMENT_PREDECESSORS = new Set([
  "",
  ";",
  "begin",
  "atomic",
  "then",
  "else",
  "do",
  "loop",
  "repeat",
  "for",
]);
const MYSQL_VARIABLE_SCOPES = new Set([
  "global",
  "session",
  "local",
  "persist",
  "persist_only",
]);

// Lowercased words with backtick and double quotes removed; string
// literals become "'", user variables "@" and system variables "@@".
// /*! */ contents are code; versioned and /*M! */ contents only when
// versionedCommentsRun, since the server may ignore them.
function mysqlCodeTokens(
  sql: string,
  backslashEscapes: boolean,
  versionedCommentsRun: boolean,
): string[] {
  const tokens: string[] = [];
  let i = 0;
  const skipQuoted = (q: string) => {
    let text = "";
    i++;
    while (i < sql.length) {
      const c = sql[i];
      if (backslashEscapes && c === "\\" && q !== "`") {
        text += sql.slice(i, i + 2);
        i += 2;
      } else if (c === q && sql[i + 1] === q) {
        text += q;
        i += 2;
      } else if (c === q) {
        i++;
        break;
      } else {
        text += c;
        i++;
      }
    }
    return text;
  };
  while (i < sql.length) {
    const c = sql[i];
    MYSQL_WORD.lastIndex = i;
    if (/\s/.test(c)) {
      i++;
    } else if (isMysqlDashCommentStart(sql, i) || c === "#") {
      while (i < sql.length && sql[i] !== "\n") i++;
    } else if (sql.startsWith("/*", i)) {
      MYSQL_EXECUTABLE_COMMENT_START.lastIndex = i;
      const start = MYSQL_EXECUTABLE_COMMENT_START.exec(sql)?.[0];
      if (start === "/*!" || (start && versionedCommentsRun)) {
        i += start.length;
      } else {
        const end = sql.indexOf("*/", i + 2);
        i = end < 0 ? sql.length : end + 2;
      }
    } else if (sql.startsWith("*/", i)) {
      i += 2;
    } else if (sql.startsWith("@@", i)) {
      tokens.push("@@");
      i += 2;
    } else if (c === "@") {
      i++;
      if (sql[i] === "'" || sql[i] === '"' || sql[i] === "`") {
        skipQuoted(sql[i]);
      } else {
        while (i < sql.length && /[\w$.\u0080-\uffff]/.test(sql[i])) i++;
      }
      tokens.push("@");
    } else if (c === "'") {
      skipQuoted(c);
      tokens.push("'");
    } else if (c === '"' || c === "`") {
      tokens.push(skipQuoted(c).toLowerCase());
    } else if (MYSQL_WORD.test(sql)) {
      tokens.push(sql.slice(i, MYSQL_WORD.lastIndex).toLowerCase());
      i = MYSQL_WORD.lastIndex;
    } else if (sql.startsWith(":=", i)) {
      tokens.push(":=");
      i += 2;
    } else {
      tokens.push(c);
      i++;
    }
  }
  return tokens;
}

function setsSqlMode(tokens: string[]): boolean {
  if (tokens[0] === "create" || tokens[0] === "alter") return false;
  const isAssignment = (token: string | undefined) =>
    token === "=" || token === ":=";
  for (let i = 0; i < tokens.length; i++) {
    if (
      tokens[i] !== "set" ||
      !MYSQL_SET_STATEMENT_PREDECESSORS.has(tokens[i - 1] ?? "")
    ) {
      continue;
    }
    i++;
    const setStatement =
      tokens[i] === "statement" && !isAssignment(tokens[i + 1]);
    if (setStatement) i++;
    while (i < tokens.length) {
      let target = i;
      if (MYSQL_VARIABLE_SCOPES.has(tokens[target])) target++;
      if (tokens[target] === "@@") {
        target++;
        if (
          MYSQL_VARIABLE_SCOPES.has(tokens[target]) &&
          tokens[target + 1] === "."
        ) {
          target += 2;
        }
      }
      if (tokens[target] === "sql_mode" && isAssignment(tokens[target + 1])) {
        return true;
      }
      let depth = 0;
      for (; i < tokens.length; i++) {
        const token = tokens[i];
        if (token === "(") depth++;
        else if (token === ")") depth--;
        else if (
          depth === 0 &&
          (token === "," || token === ";" || (setStatement && token === "for"))
        ) {
          break;
        }
      }
      if (tokens[i] !== ",") break;
      i++;
    }
  }
  return false;
}

// Conservatively true for any SET statement (including SET STATEMENT ... FOR
// and SETs inside anonymous compound blocks) whose assignment targets include
// sql_mode in any scope, whether or not versioned comments run; routine and
// event definitions only store their SETs.
function mayChangeSessionSqlMode(
  statement: string,
  backslashEscapes: boolean,
): boolean {
  return [true, false].some((versionedCommentsRun) =>
    setsSqlMode(
      mysqlCodeTokens(statement, backslashEscapes, versionedCommentsRun),
    ),
  );
}

// Backslashes only change statement boundaries under the session's
// NO_BACKSLASH_ESCAPES mode, so the mode is resolved only when they do.
function splitMySQLQueryScript(
  sql: string,
): string[] | ((backslashEscapes: boolean) => string[]) {
  if (!sql.includes("\\")) return splitMySQLScript(sql);
  const [escaped, literal] = [true, false].map((backslashEscapes) => {
    try {
      return splitMySQLScript(sql, { backslashEscapes });
    } catch (error) {
      return error instanceof Error ? error : new Error(String(error));
    }
  });
  if (escaped instanceof Error && literal instanceof Error) throw escaped;
  if (
    Array.isArray(escaped) &&
    Array.isArray(literal) &&
    escaped.length === literal.length &&
    escaped.every((statement, index) => statement === literal[index])
  ) {
    return escaped;
  }
  const changesSqlMode = (
    statements: string[] | Error,
    backslashEscapes: boolean,
  ) =>
    Array.isArray(statements) &&
    statements.some((statement) =>
      mayChangeSessionSqlMode(statement, backslashEscapes),
    );
  if (changesSqlMode(escaped, true) || changesSqlMode(literal, false)) {
    throw new Error(
      "Script changes sql_mode and contains backslashes whose meaning depends on NO_BACKSLASH_ESCAPES, so its statement boundaries are ambiguous; script was not submitted. Change sql_mode in a separate run.",
    );
  }
  return (backslashEscapes) => {
    const statements = backslashEscapes ? escaped : literal;
    if (statements instanceof Error) throw statements;
    return statements;
  };
}

function emptyMysqlQueryResult(hardCap: number | undefined): QueryResult {
  return {
    columns: [],
    rows: [],
    rowCount: 0,
    executionTimeMs: 0,
    ...(hardCap !== undefined ? { truncated: false } : {}),
  };
}

const MYSQL_SPATIAL_TYPES = new Set([
  "point",
  "linestring",
  "polygon",
  "multipoint",
  "multilinestring",
  "multipolygon",
  "geomcollection",
  "geometrycollection",
  "geometry",
]);
const MYSQL_WKT_VALUE_RE =
  /^(?:srid=\d+;)?\s*(?:point|linestring|polygon|multipoint|multilinestring|multipolygon|geometrycollection)\s*\(/i;
function mysqlTypeName(nativeType: string): string {
  const match = /^[a-z]+/i.exec(nativeType.trim());
  return match?.[0]?.toLowerCase() ?? nativeType.toLowerCase().trim();
}
function mysqlGeneratedKind(
  extra: string | null | undefined,
): ColumnMeta["generatedKind"] {
  if (!extra) {
    return undefined;
  }
  if (/\bstored generated\b/i.test(extra)) {
    return "stored";
  }
  if (/\bvirtual generated\b/i.test(extra)) {
    return "virtual";
  }
  return undefined;
}
function parseMysqlExtraMetadata(extra: string | null | undefined): {
  onUpdateExpression: string | undefined;
  generatedKind: ColumnMeta["generatedKind"];
  identityGeneration: ColumnMeta["identityGeneration"];
} {
  const normalizedExtra = extra?.trim() ?? "";
  const onUpdateExpression = /\bon update\s+(.+)$/i
    .exec(normalizedExtra)?.[1]
    ?.trim();
  return {
    onUpdateExpression,
    generatedKind: mysqlGeneratedKind(normalizedExtra),
    identityGeneration: /\bauto_increment\b/i.test(normalizedExtra)
      ? "auto_increment"
      : undefined,
  };
}
function isValidMysqlDateParts(
  year: number,
  month: number,
  day: number,
): boolean {
  if (month < 1 || month > 12 || day < 1 || day > 31) {
    return false;
  }
  const parsed = new Date(0);
  parsed.setUTCFullYear(year, month - 1, day);
  return (
    parsed.getUTCFullYear() === year &&
    parsed.getUTCMonth() + 1 === month &&
    parsed.getUTCDate() === day
  );
}
function parseMysqlDateParts(value: string): {
  year: number;
  month: number;
  day: number;
} | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value.trim());
  if (!match) {
    return null;
  }
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (!isValidMysqlDateParts(year, month, day)) {
    return null;
  }
  return { year, month, day };
}
type ParsedMysqlDatetimeInput = {
  year: number;
  month: number;
  day: number;
  hours: number;
  minutes: number;
  seconds: number;
  fractionMicros: number;
  fractionDigits: number;
  timezoneOffsetMinutes: number | null;
};
function parseMysqlTimezoneOffset(token: string): number {
  if (token === "Z" || token === "z") {
    return 0;
  }
  const sign = token.startsWith("-") ? -1 : 1;
  const hours = Number(token.slice(1, 3));
  const minutes = Number(token.slice(4, 6));
  return sign * (hours * 60 + minutes);
}
function parseMysqlDatetimeParts(
  value: string,
): ParsedMysqlDatetimeInput | null {
  const normalized = value.trim().replace(/ ([+-]\d{2}:\d{2})$/, "$1");
  const match =
    /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})?$/i.exec(
      normalized,
    );
  if (!match) {
    return null;
  }
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hours = Number(match[4]);
  const minutes = Number(match[5]);
  const seconds = Number(match[6]);
  const rawFraction = match[7] ?? "";
  const timezoneToken = match[8] ?? null;
  if (!isValidMysqlDateParts(year, month, day)) {
    return null;
  }
  if (hours > 23 || minutes > 59 || seconds > 59) {
    return null;
  }
  return {
    year,
    month,
    day,
    hours,
    minutes,
    seconds,
    fractionMicros: rawFraction
      ? Number(rawFraction.slice(0, 6).padEnd(6, "0"))
      : 0,
    fractionDigits: Math.min(rawFraction.length, 6),
    timezoneOffsetMinutes: timezoneToken
      ? parseMysqlTimezoneOffset(timezoneToken)
      : null,
  };
}
function formatMysqlDateParts(parts: {
  year: number;
  month: number;
  day: number;
}): string {
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${String(parts.year).padStart(4, "0")}-${pad(parts.month)}-${pad(parts.day)}`;
}
function formatMysqlFraction(
  fractionMicros: number,
  fractionDigits: number,
): string {
  if (fractionDigits === 0) {
    return "";
  }
  const fraction = String(fractionMicros)
    .padStart(6, "0")
    .slice(0, fractionDigits)
    .replace(/0+$/, "");
  return fraction.length > 0 ? `.${fraction}` : "";
}
function formatMysqlDatetimeParts(parts: ParsedMysqlDatetimeInput): string {
  const pad = (n: number) => String(n).padStart(2, "0");
  return (
    `${formatMysqlDateParts(parts)} ` +
    `${pad(parts.hours)}:${pad(parts.minutes)}:${pad(parts.seconds)}` +
    formatMysqlFraction(parts.fractionMicros, parts.fractionDigits)
  );
}
function formatMysqlDatetimeUtc(
  parts: ParsedMysqlDatetimeInput,
): string | null {
  if (parts.timezoneOffsetMinutes === null) {
    return formatMysqlDatetimeParts(parts);
  }
  const local = new Date(0);
  local.setUTCFullYear(parts.year, parts.month - 1, parts.day);
  local.setUTCHours(
    parts.hours,
    parts.minutes,
    parts.seconds,
    Math.floor(parts.fractionMicros / 1000),
  );
  const totalMicros =
    BigInt(local.getTime()) * 1000n +
    BigInt(parts.fractionMicros % 1000) -
    BigInt(parts.timezoneOffsetMinutes) * 60n * 1000000n;
  let epochMillis = totalMicros / 1000n;
  let extraMicros = totalMicros % 1000n;
  if (extraMicros < 0n) {
    epochMillis -= 1n;
    extraMicros += 1000n;
  }
  const value = new Date(Number(epochMillis));
  if (Number.isNaN(value.getTime()) || value.getUTCFullYear() < 0) {
    return null;
  }
  const utcFractionMicros =
    value.getUTCMilliseconds() * 1000 + Number(extraMicros);
  const pad = (n: number) => String(n).padStart(2, "0");
  return (
    `${String(value.getUTCFullYear()).padStart(4, "0")}-${pad(value.getUTCMonth() + 1)}-${pad(value.getUTCDate())} ` +
    `${pad(value.getUTCHours())}:${pad(value.getUTCMinutes())}:${pad(value.getUTCSeconds())}` +
    formatMysqlFraction(utcFractionMicros, parts.fractionDigits)
  );
}
function looksLikeMysqlDateInput(value: string): boolean {
  return /^\d{4}-\d{2}-\d{2}(?:[ T].*)?$/.test(value.trim());
}
function looksLikeMysqlDatetimeInput(value: string): boolean {
  return /^\d{4}-\d{2}-\d{2}[ T]/.test(value.trim());
}
function normalizeMysqlDateInput(value: string): string | null {
  const trimmed = value.trim();
  const dateOnly = parseMysqlDateParts(trimmed);
  if (dateOnly) {
    return formatMysqlDateParts(dateOnly);
  }
  if (!ISO_DATETIME_RE.test(trimmed) && !DATETIME_SQL_RE.test(trimmed)) {
    return null;
  }
  const parsed = parseMysqlDatetimeParts(trimmed);
  if (!parsed) {
    return null;
  }
  if (parsed.timezoneOffsetMinutes === null) {
    return formatMysqlDateParts(parsed);
  }
  return formatMysqlDatetimeUtc(parsed)?.slice(0, 10) ?? null;
}
function normalizeMysqlDatetimeInput(value: string): string | null {
  const trimmed = value.trim();
  if (!ISO_DATETIME_RE.test(trimmed) && !DATETIME_SQL_RE.test(trimmed)) {
    return null;
  }
  const parsed = parseMysqlDatetimeParts(trimmed);
  if (!parsed) {
    return null;
  }
  if (parsed.timezoneOffsetMinutes === null) {
    return formatMysqlDatetimeParts(parsed);
  }
  return formatMysqlDatetimeUtc(parsed);
}
function hasMysqlTimezoneOffset(value: string): boolean {
  return parseMysqlDatetimeParts(value)?.timezoneOffsetMinutes != null;
}
function mysqlUtcTimestampValue(value: unknown): string | null {
  if (typeof value !== "string" || !value.endsWith("+00:00")) return null;
  return normalizeMysqlDatetimeInput(value);
}
function mysqlTimestampEpoch(value: string): string | null {
  const parts = parseMysqlDatetimeParts(value);
  if (!parts || parts.timezoneOffsetMinutes === null) return null;
  const local = new Date(0);
  local.setUTCFullYear(parts.year, parts.month - 1, parts.day);
  local.setUTCHours(parts.hours, parts.minutes, parts.seconds, 0);
  const seconds =
    BigInt(local.getTime() / 1000) - BigInt(parts.timezoneOffsetMinutes) * 60n;
  if (seconds < 1n) return null;
  return `${seconds}${formatMysqlFraction(parts.fractionMicros, parts.fractionDigits)}`;
}
function mysqlTimestampEpochParam(
  prefix: string,
  columnName: string,
  value: string,
): string {
  const epoch = mysqlTimestampEpoch(value);
  if (epoch === null) {
    throw new Error(
      `${prefix} Column ${columnName} expects a TIMESTAMP value after 1970-01-01 00:00:00 UTC.`,
    );
  }
  return epoch;
}
function invalidMysqlTemporalInputError(
  columnName: string,
  typeName: string,
): Error {
  return new Error(
    `[RapiDB] Column ${columnName} expects a valid ${typeName.toUpperCase()} value.`,
  );
}
function decodeMysqlBitBuffer(
  buffer: Buffer,
  declaredBits: number,
): number | bigint {
  if (buffer.length === 0) {
    return 0;
  }
  let result = 0n;
  for (const byte of buffer.values()) {
    result = (result << 8n) | BigInt(byte);
  }
  return declaredBits > 48 ? result : Number(result);
}
function parseMysqlBitWidth(nativeType: string): number | null {
  const match = /bit\s*\(\s*(\d+)\s*\)/i.exec(nativeType);
  if (!match) {
    return null;
  }
  const width = Number.parseInt(match[1], 10);
  return Number.isInteger(width) && width > 0 ? width : null;
}
function toMysqlIntegerFilterParam(val: string): number | string {
  if (/^[+-]?\d+$/.test(val)) {
    const big = BigInt(val);
    const safe = BigInt(Number.MAX_SAFE_INTEGER);
    if (big > safe || big < -safe) {
      return val;
    }
  }
  return Number(val);
}
function mysqlFilterCollation(
  characterSet: string,
  collation: string,
): { characterSet: string; collation: string } | null {
  if (!/^[a-z0-9_]+$/i.test(characterSet) || !/^[a-z0-9_]+$/i.test(collation)) {
    return null;
  }
  return { characterSet, collation };
}
function mysqlBitMaxValue(nativeType: string): bigint | null {
  const width = parseMysqlBitWidth(nativeType);
  return width === null ? null : (1n << BigInt(width)) - 1n;
}
function mysqlBitValidationMessage(
  columnName: string,
  nativeType: string,
): string {
  const width = parseMysqlBitWidth(nativeType);
  if (width === null) {
    return `Column ${columnName} expects a non-negative integer bit value.`;
  }
  return `Column ${columnName} expects an unsigned BIT(${width}) value between 0 and ${mysqlBitMaxValue(nativeType)?.toString() ?? "0"}.`;
}
function parseMysqlBitValue(
  value: unknown,
  nativeType: string,
): {
  canonical: string;
  parameter: number | string;
} | null {
  let bigintValue: bigint;
  if (typeof value === "number") {
    if (!Number.isInteger(value) || value < 0) {
      return null;
    }
    bigintValue = BigInt(value);
  } else if (typeof value === "bigint") {
    if (value < 0n) {
      return null;
    }
    bigintValue = value;
  } else if (typeof value === "string") {
    const normalized = value.trim();
    if (!/^\d+$/.test(normalized)) {
      return null;
    }
    bigintValue = BigInt(normalized);
  } else {
    return null;
  }
  const maxValue = mysqlBitMaxValue(nativeType);
  if (maxValue !== null && bigintValue > maxValue) {
    return null;
  }
  const canonical = bigintValue.toString();
  const safeNumber = BigInt(Number.MAX_SAFE_INTEGER);
  return {
    canonical,
    parameter: bigintValue <= safeNumber ? Number(canonical) : canonical,
  };
}
function isMysqlSpatialPointLike(
  value: unknown,
): value is { x: unknown; y: unknown } {
  return (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    "x" in value &&
    "y" in value
  );
}
function formatMysqlSpatialCoordinate(value: unknown): string | null {
  if (typeof value === "number") {
    return Number.isFinite(value) ? String(value) : null;
  }
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (/^[+-]?(?:\d+(?:\.\d*)?|\d*\.\d+)(?:[eE][+-]?\d+)?$/.test(trimmed)) {
      return trimmed;
    }
  }
  return null;
}
function mysqlPointCoordinatesText(value: unknown): string | null {
  if (!isMysqlSpatialPointLike(value)) {
    return null;
  }
  const x = formatMysqlSpatialCoordinate(value.x);
  const y = formatMysqlSpatialCoordinate(value.y);
  if (x === null || y === null) {
    return null;
  }
  return `${x} ${y}`;
}
function mysqlLineStringCoordinatesText(value: unknown): string | null {
  if (!Array.isArray(value) || value.length === 0) {
    return null;
  }
  const points = value.map((entry) => mysqlPointCoordinatesText(entry));
  if (points.some((entry) => entry === null)) {
    return null;
  }
  return points.join(", ");
}
function mysqlPolygonCoordinatesText(value: unknown): string | null {
  if (!Array.isArray(value) || value.length === 0) {
    return null;
  }
  const rings = value.map((entry) => mysqlLineStringCoordinatesText(entry));
  if (rings.some((entry) => entry === null)) {
    return null;
  }
  return rings.map((ring) => `(${ring})`).join(", ");
}
function inferMysqlSpatialWkt(value: unknown): string | null {
  const point = mysqlPointCoordinatesText(value);
  if (point !== null) {
    return `POINT(${point})`;
  }
  const lineString = mysqlLineStringCoordinatesText(value);
  if (lineString !== null) {
    return `LINESTRING(${lineString})`;
  }
  const polygon = mysqlPolygonCoordinatesText(value);
  if (polygon !== null) {
    return `POLYGON(${polygon})`;
  }
  if (Array.isArray(value) && value.length > 0) {
    const polygons = value.map((entry) => mysqlPolygonCoordinatesText(entry));
    if (polygons.every((entry) => entry !== null)) {
      return `MULTIPOLYGON(${polygons.map((polygonText) => `(${polygonText})`).join(", ")})`;
    }
    const geometries = value.map((entry) => inferMysqlSpatialWkt(entry));
    if (geometries.every((entry) => entry !== null)) {
      return `GEOMETRYCOLLECTION(${geometries.join(", ")})`;
    }
  }
  return null;
}
function buildMysqlSpatialWkt(
  value: unknown,
  nativeType: string,
): string | null {
  const baseType = mysqlTypeName(nativeType);
  switch (baseType) {
    case "point": {
      const coordinates = mysqlPointCoordinatesText(value);
      return coordinates === null ? null : `POINT(${coordinates})`;
    }
    case "linestring": {
      const coordinates = mysqlLineStringCoordinatesText(value);
      return coordinates === null ? null : `LINESTRING(${coordinates})`;
    }
    case "polygon": {
      const coordinates = mysqlPolygonCoordinatesText(value);
      return coordinates === null ? null : `POLYGON(${coordinates})`;
    }
    case "multipoint": {
      const coordinates = mysqlLineStringCoordinatesText(value);
      if (coordinates === null) {
        return null;
      }
      return `MULTIPOINT(${coordinates
        .split(", ")
        .map((point) => `(${point})`)
        .join(", ")})`;
    }
    case "multilinestring": {
      if (!Array.isArray(value) || value.length === 0) {
        return null;
      }
      const lineStrings = value.map((entry) =>
        mysqlLineStringCoordinatesText(entry),
      );
      if (lineStrings.some((entry) => entry === null)) {
        return null;
      }
      return `MULTILINESTRING(${lineStrings.map((lineString) => `(${lineString})`).join(", ")})`;
    }
    case "multipolygon": {
      if (!Array.isArray(value) || value.length === 0) {
        return null;
      }
      const polygons = value.map((entry) => mysqlPolygonCoordinatesText(entry));
      if (polygons.some((entry) => entry === null)) {
        return null;
      }
      return `MULTIPOLYGON(${polygons.map((polygonText) => `(${polygonText})`).join(", ")})`;
    }
    case "geometrycollection": {
      if (!Array.isArray(value) || value.length === 0) {
        return null;
      }
      const geometries = value.map((entry) => inferMysqlSpatialWkt(entry));
      if (geometries.some((entry) => entry === null)) {
        return null;
      }
      return `GEOMETRYCOLLECTION(${geometries.join(", ")})`;
    }
    case "geometry":
      return inferMysqlSpatialWkt(value);
    default:
      return null;
  }
}
function normalizeMysqlSpatialInput(
  value: unknown,
  nativeType: string,
): string | null {
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed === "") {
      return null;
    }
    if (MYSQL_WKT_VALUE_RE.test(trimmed)) {
      return trimmed;
    }
  }
  return buildMysqlSpatialWkt(value, nativeType);
}
function parseMysqlSpatialSridPrefix(value: string): {
  wkt: string;
  srid: number | null;
} {
  const trimmed = value.trim();
  const sridMatch = /^srid=(\d+);(.*)$/i.exec(trimmed);
  if (!sridMatch) {
    return { wkt: trimmed, srid: null };
  }
  const srid = Number(sridMatch[1]);
  if (!Number.isInteger(srid) || srid < 0 || srid > 0xffffffff) {
    throw new TypeError(
      "MySQL spatial SRID must be an unsigned 32-bit integer.",
    );
  }
  return {
    srid,
    wkt: sridMatch[2].trim(),
  };
}

type MysqlGeometryParseResult = {
  geometryType: number;
  body: string;
  wkt: string;
  nextOffset: number;
};

function readMysqlGeometryUInt32(
  view: DataView,
  offset: number,
  littleEndian: boolean,
): number {
  if (offset + 4 > view.byteLength) {
    throw new Error("Invalid WKB: unexpected end of data while reading uint32");
  }
  return view.getUint32(offset, littleEndian);
}

function readMysqlGeometryDouble(
  view: DataView,
  offset: number,
  littleEndian: boolean,
): number {
  if (offset + 8 > view.byteLength) {
    throw new Error(
      "Invalid WKB: unexpected end of data while reading float64",
    );
  }
  return view.getFloat64(offset, littleEndian);
}

function mysqlGeometryNumber(value: number): string {
  if (Number.isInteger(value)) {
    return String(value);
  }
  return value
    .toString()
    .replace(/(\.\d*?[1-9])0+$/, "$1")
    .replace(/\.0+$/, "");
}

function parseMysqlWkbGeometry(
  view: DataView,
  offset: number,
): MysqlGeometryParseResult {
  if (offset + 5 > view.byteLength) {
    throw new Error("Invalid WKB: missing byte order or geometry type");
  }

  const byteOrder = view.getUint8(offset);
  const littleEndian = byteOrder === 1;
  if (!littleEndian && byteOrder !== 0) {
    throw new Error(`Invalid WKB: unsupported byte order ${byteOrder}`);
  }

  let cursor = offset + 1;
  const rawType = readMysqlGeometryUInt32(view, cursor, littleEndian);
  cursor += 4;
  const geometryType = rawType % 1000;

  if (geometryType === 1) {
    const x = readMysqlGeometryDouble(view, cursor, littleEndian);
    cursor += 8;
    const y = readMysqlGeometryDouble(view, cursor, littleEndian);
    cursor += 8;
    const body = `${mysqlGeometryNumber(x)} ${mysqlGeometryNumber(y)}`;
    return {
      geometryType,
      body,
      wkt: `POINT(${body})`,
      nextOffset: cursor,
    };
  }

  if (geometryType === 2) {
    const pointCount = readMysqlGeometryUInt32(view, cursor, littleEndian);
    cursor += 4;
    const points: string[] = [];
    for (let index = 0; index < pointCount; index += 1) {
      const x = readMysqlGeometryDouble(view, cursor, littleEndian);
      cursor += 8;
      const y = readMysqlGeometryDouble(view, cursor, littleEndian);
      cursor += 8;
      points.push(`${mysqlGeometryNumber(x)} ${mysqlGeometryNumber(y)}`);
    }
    const body = points.join(", ");
    return {
      geometryType,
      body,
      wkt: `LINESTRING(${body})`,
      nextOffset: cursor,
    };
  }

  if (geometryType === 3) {
    const ringCount = readMysqlGeometryUInt32(view, cursor, littleEndian);
    cursor += 4;
    const rings: string[] = [];
    for (let ringIndex = 0; ringIndex < ringCount; ringIndex += 1) {
      const pointCount = readMysqlGeometryUInt32(view, cursor, littleEndian);
      cursor += 4;
      const points: string[] = [];
      for (let pointIndex = 0; pointIndex < pointCount; pointIndex += 1) {
        const x = readMysqlGeometryDouble(view, cursor, littleEndian);
        cursor += 8;
        const y = readMysqlGeometryDouble(view, cursor, littleEndian);
        cursor += 8;
        points.push(`${mysqlGeometryNumber(x)} ${mysqlGeometryNumber(y)}`);
      }
      rings.push(`(${points.join(", ")})`);
    }
    const body = rings.join(", ");
    return {
      geometryType,
      body,
      wkt: `POLYGON(${body})`,
      nextOffset: cursor,
    };
  }

  if (geometryType >= 4 && geometryType <= 7) {
    const geometryCount = readMysqlGeometryUInt32(view, cursor, littleEndian);
    cursor += 4;
    const children: MysqlGeometryParseResult[] = [];
    for (let index = 0; index < geometryCount; index += 1) {
      const parsedChild = parseMysqlWkbGeometry(view, cursor);
      cursor = parsedChild.nextOffset;
      children.push(parsedChild);
    }

    let wkt: string;
    if (geometryType === 4) {
      if (children.some((child) => child.geometryType !== 1)) {
        throw new Error(
          "Invalid WKB: MULTIPOINT contains a non-POINT geometry",
        );
      }
      wkt = `MULTIPOINT(${children
        .map((child) => `(${child.body})`)
        .join(", ")})`;
    } else if (geometryType === 5) {
      if (children.some((child) => child.geometryType !== 2)) {
        throw new Error(
          "Invalid WKB: MULTILINESTRING contains a non-LINESTRING geometry",
        );
      }
      wkt = `MULTILINESTRING(${children
        .map((child) => `(${child.body})`)
        .join(", ")})`;
    } else if (geometryType === 6) {
      if (children.some((child) => child.geometryType !== 3)) {
        throw new Error(
          "Invalid WKB: MULTIPOLYGON contains a non-POLYGON geometry",
        );
      }
      wkt = `MULTIPOLYGON(${children
        .map((child) => `(${child.body})`)
        .join(", ")})`;
    } else {
      wkt = `GEOMETRYCOLLECTION(${children.map((child) => child.wkt).join(", ")})`;
    }

    return {
      geometryType,
      body: "",
      wkt,
      nextOffset: cursor,
    };
  }

  throw new Error(`Unsupported WKB geometry type ${geometryType}`);
}

function decodeMysqlGeometryBuffer(value: Buffer): string | null {
  if (value.length < 9) {
    return null;
  }

  try {
    const srid = value.readUInt32LE(0);
    const view = new DataView(value.buffer, value.byteOffset, value.byteLength);
    const parsed = parseMysqlWkbGeometry(view, 4);
    if (parsed.nextOffset !== view.byteLength) {
      return null;
    }
    return srid > 0 ? `SRID=${srid};${parsed.wkt}` : parsed.wkt;
  } catch {
    return null;
  }
}

function mysqlGeoJsonCoordinateText(value: unknown): string | null {
  if (!Array.isArray(value) || value.length < 2) {
    return null;
  }
  const x = formatMysqlSpatialCoordinate(value[0]);
  const y = formatMysqlSpatialCoordinate(value[1]);
  if (x === null || y === null) {
    return null;
  }
  return `${x} ${y}`;
}

function mysqlGeoJsonLineStringText(value: unknown): string | null {
  if (!Array.isArray(value) || value.length === 0) {
    return null;
  }
  const points = value.map((entry) => mysqlGeoJsonCoordinateText(entry));
  if (points.some((entry) => entry === null)) {
    return null;
  }
  return points.join(", ");
}

function mysqlGeoJsonPolygonText(value: unknown): string | null {
  if (!Array.isArray(value) || value.length === 0) {
    return null;
  }
  const rings = value.map((ring) => mysqlGeoJsonLineStringText(ring));
  if (rings.some((entry) => entry === null)) {
    return null;
  }
  return rings.map((ring) => `(${ring})`).join(", ");
}

function buildMysqlSpatialWktFromGeoJson(value: unknown): string | null {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }

  const record = value as Record<string, unknown>;
  const type = typeof record.type === "string" ? record.type.toLowerCase() : "";

  switch (type) {
    case "point": {
      const coords = mysqlGeoJsonCoordinateText(record.coordinates);
      return coords === null ? null : `POINT(${coords})`;
    }
    case "linestring": {
      const line = mysqlGeoJsonLineStringText(record.coordinates);
      return line === null ? null : `LINESTRING(${line})`;
    }
    case "polygon": {
      const polygon = mysqlGeoJsonPolygonText(record.coordinates);
      return polygon === null ? null : `POLYGON(${polygon})`;
    }
    case "multipoint": {
      const points = mysqlGeoJsonLineStringText(record.coordinates);
      if (points === null) {
        return null;
      }
      return `MULTIPOINT(${points
        .split(", ")
        .map((point) => `(${point})`)
        .join(", ")})`;
    }
    case "multilinestring": {
      if (
        !Array.isArray(record.coordinates) ||
        record.coordinates.length === 0
      ) {
        return null;
      }
      const lines = record.coordinates.map((entry) =>
        mysqlGeoJsonLineStringText(entry),
      );
      if (lines.some((entry) => entry === null)) {
        return null;
      }
      return `MULTILINESTRING(${lines.map((line) => `(${line})`).join(", ")})`;
    }
    case "multipolygon": {
      if (
        !Array.isArray(record.coordinates) ||
        record.coordinates.length === 0
      ) {
        return null;
      }
      const polygons = record.coordinates.map((entry) =>
        mysqlGeoJsonPolygonText(entry),
      );
      if (polygons.some((entry) => entry === null)) {
        return null;
      }
      return `MULTIPOLYGON(${polygons.map((polygon) => `(${polygon})`).join(", ")})`;
    }
    case "geometrycollection": {
      if (!Array.isArray(record.geometries) || record.geometries.length === 0) {
        return null;
      }
      const geometries = record.geometries.map((entry) =>
        buildMysqlSpatialWktFromGeoJson(entry),
      );
      if (geometries.some((entry) => entry === null)) {
        return null;
      }
      return `GEOMETRYCOLLECTION(${geometries.join(", ")})`;
    }
    default:
      return null;
  }
}

function normalizeMysqlQuerySpatialValue(value: unknown): string | null {
  if (Buffer.isBuffer(value)) {
    return decodeMysqlGeometryBuffer(value);
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) {
      return null;
    }
    if (MYSQL_WKT_VALUE_RE.test(trimmed)) {
      return trimmed;
    }
    if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
      try {
        const parsed = JSON.parse(trimmed);
        return (
          buildMysqlSpatialWktFromGeoJson(parsed) ??
          inferMysqlSpatialWkt(parsed)
        );
      } catch {
        return null;
      }
    }
    return null;
  }
  if (value !== null && typeof value === "object") {
    return (
      buildMysqlSpatialWktFromGeoJson(value) ?? inferMysqlSpatialWkt(value)
    );
  }
  return null;
}

function canonicalizeMysqlBitPersistedEditValue(
  value: unknown,
  nativeType?: string,
): {
  canonical: string;
} | null {
  if (value === NULL_SENTINEL || value === null) {
    return { canonical: "__rapidb_null__" };
  }
  if (Buffer.isBuffer(value)) {
    const type = nativeType ?? "bit";
    const bitValue = decodeMysqlBitBuffer(
      value,
      parseMysqlBitWidth(type) ?? value.length * 8,
    );
    const parsed = parseMysqlBitValue(bitValue, type);
    if (!parsed) {
      return null;
    }
    return { canonical: parsed.canonical };
  }
  const parsed = parseMysqlBitValue(value, nativeType ?? "bit");
  if (!parsed) {
    return null;
  }
  return { canonical: parsed.canonical };
}
type MysqlObjectRow = RowDataPacket & Record<string, unknown>;
type MysqlArrayRow = unknown[];
type MysqlSelectRows = MysqlArrayRow[];
type MysqlResultPayload = MysqlSelectRows | ResultSetHeader;
type MysqlQueryRows = MysqlResultPayload | MysqlResultPayload[];
type MysqlQueryFields =
  | FieldPacket[]
  | (FieldPacket[] | undefined)[]
  | undefined;
const MYSQL_POOL_CONNECTION_LIMIT = 5;
const MYSQL_KILL_QUERY_TIMEOUT_MS = 5000;
const MYSQL_SESSION_PROBE_TIMEOUT_MS = 2000;
const MYSQL_SESSION_INIT_SQL = "SET SESSION autocommit = 1";
const MYSQL_UTC_TIMESTAMP_OPERAND =
  "CONVERT_TZ(?, '+00:00', @@session.time_zone)";
const MYSQL_TIMESTAMP_EPOCH_OPERAND = "CAST(? AS DECIMAL(20,6))";
const MYSQL_TIMESTAMP_PREFILTER_SECONDS = 86400n;
const MYSQL_FROM_UNIXTIME_SAFE_MAX = 2147483647n;
function mysqlUtcTimestampWriteValues(
  operation: import("./types").TransactionOperation,
): string[] {
  if (!operation.sql.includes(MYSQL_UTC_TIMESTAMP_OPERAND)) return [];
  const params = operation.params ?? [];
  const bound = formatMysqlQuery(operation.sql, params);
  const [before, after] = MYSQL_UTC_TIMESTAMP_OPERAND.split("?");
  return params.filter(
    (value): value is string =>
      typeof value === "string" &&
      bound.includes(`${before}${formatMysqlStringLiteral(value)}${after}`),
  );
}
interface MysqlKillTarget {
  threadId: number;
  identitySql: string;
  identity: string;
}
function mysqlServerIdentitySql(connection: object): string {
  const handshake = Reflect.get(connection, "_handshakePacket");
  const version =
    handshake && typeof handshake === "object"
      ? Reflect.get(handshake, "serverVersion")
      : undefined;
  return typeof version === "string" && !/mariadb/i.test(version)
    ? "SELECT CONNECTION_ID() AS connection_id, @@autocommit AS autocommit, CONCAT_WS(':', @@server_uuid, @@hostname, @@port) AS server_identity"
    : "SELECT CONNECTION_ID() AS connection_id, @@autocommit AS autocommit, CONCAT_WS(':', @@server_id, @@hostname, @@port) AS server_identity";
}
function mysqlHandshakeAutocommit(connection: object): boolean {
  const handshake = Reflect.get(connection, "_handshakePacket");
  const statusFlags =
    handshake && typeof handshake === "object"
      ? Reflect.get(handshake, "statusFlags")
      : undefined;
  return typeof statusFlags === "number" && (statusFlags & 2) !== 0;
}
function readMysqlServerIdentity(
  rows: unknown,
): { connectionId: number; identity: string } | null {
  const row = Array.isArray(rows) ? rows[0] : undefined;
  if (!row || typeof row !== "object") return null;
  const connectionId = Number(Reflect.get(row, "connection_id"));
  const identity = Reflect.get(row, "server_identity");
  return Number.isSafeInteger(connectionId) &&
    connectionId > 0 &&
    typeof identity === "string" &&
    identity !== ""
    ? { connectionId, identity }
    : null;
}
interface MysqlQueryOperation {
  cancelled: boolean;
  reject?: (error: Error) => void;
  requestToken?: number;
  connection?: PoolConnection;
}
function isMysqlSelectRows(rawRows: unknown): rawRows is MysqlSelectRows {
  return (
    Array.isArray(rawRows) &&
    (rawRows.length === 0 || Array.isArray(rawRows[0]))
  );
}

function isMysqlFieldPacket(value: unknown): value is FieldPacket {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    ("name" in value || "columnType" in value)
  );
}

function isMysqlResultFieldGroups(
  fields: MysqlQueryFields,
): fields is (FieldPacket[] | undefined)[] {
  return (
    Array.isArray(fields) &&
    fields.length > 1 &&
    fields.every(
      (fieldGroup) => fieldGroup === undefined || Array.isArray(fieldGroup),
    )
  );
}

function isMysqlResultSetHeader(value: unknown): value is ResultSetHeader {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    "affectedRows" in value
  );
}

function splitMysqlQueryResults(
  rawRows: MysqlQueryRows,
  fields: MysqlQueryFields,
): Array<{ rows: MysqlResultPayload; fields?: FieldPacket[] }> {
  if (isMysqlResultFieldGroups(fields)) {
    return (Array.isArray(rawRows) ? rawRows : [rawRows]).map(
      (resultRows, index) => ({
        rows: resultRows as MysqlResultPayload,
        ...(Array.isArray(fields[index]) ? { fields: fields[index] } : {}),
      }),
    );
  }
  return [
    {
      rows: rawRows as MysqlResultPayload,
      ...(Array.isArray(fields) &&
      (fields.length === 0 || isMysqlFieldPacket(fields[0]))
        ? { fields }
        : {}),
    },
  ];
}
export class MySQLDriver extends BaseDBDriver {
  protected override getQueryEditorSqlDialect() {
    return "mysql" as const;
  }

  private pool: Pool | null = null;
  private readonly poolConnections = new Map<
    Pool,
    Set<NativeMysqlConnection>
  >();
  private readonly config: ConnectionConfig;
  private timeoutRecoveryInFlight: Promise<void> | null = null;
  private readonly activeTransactionConnections = new Set<PoolConnection>();
  private readonly activeQueryConnections = new Set<PoolConnection>();
  private readonly activeQueryOperations = new Set<MysqlQueryOperation>();
  private readonly discardedMetadataConnections = new WeakSet<object>();
  private activeQueryConnectionSlots = 0;
  private connectionEpoch = 0;
  private connectionAbortController?: AbortController;
  private killQueryConnectionOptions?: ConnectionOptions;
  private readonly killTargets = new WeakMap<object, MysqlKillTarget>();
  private readonly sessionReady = new WeakMap<object, Promise<void>>();
  private forceSessionAutocommit = false;
  private readonly tableIndexCache = new Map<
    string,
    import("./types").IndexMeta[]
  >();
  private statisticsExpressionSupported: boolean | undefined;
  constructor(
    config: ConnectionConfig,
    timeoutSettingsProvider?: DriverTimeoutSettingsProvider,
  ) {
    super(timeoutSettingsProvider);
    this.config = config;
  }

  private createQueryOptions(
    sql: string,
    params?: QueryOptions["values"],
  ): QueryOptions {
    return {
      sql,
      values: params,
      timeout: this.getDbOperationTimeoutMs(),
    };
  }

  async connect(): Promise<void> {
    const epoch = ++this.connectionEpoch;
    this.connectionAbortController?.abort();
    const abortController = new AbortController();
    this.connectionAbortController = abortController;
    if (this.pool !== null) {
      const previousPool = this.pool;
      this.pool = null;
      try {
        await this.closePool(previousPool);
      } catch {}
    }
    let pool: Pool | undefined;
    try {
      this.assertConnectionEpoch(epoch);
      const tlsSettings = await resolveConnectionTlsSettings(
        this.config,
        abortController.signal,
      );
      this.assertConnectionEpoch(epoch);
      const forwardedTransport = getSshTcpForwardTransport(this.config);
      const ssl: MysqlSslOptions | undefined = tlsSettings
        ? {
            rejectUnauthorized: tlsSettings.rejectUnauthorized,
            verifyIdentity: !tlsSettings.skipHostnameVerification,
            ca: tlsSettings.ca,
            cert: tlsSettings.cert,
            key: tlsSettings.key,
            passphrase: tlsSettings.passphrase,
          }
        : undefined;

      const endpointHost = forwardedTransport?.localHost ?? this.config.host;
      const endpointPort = forwardedTransport?.localPort ?? this.config.port;
      const tlsIdentity = tlsSettings?.servername?.replace(/^\[|\]$/g, "");
      this.assertConnectionEpoch(epoch);
      const connectionOptions: ConnectionOptions = {
        host: tlsIdentity ?? endpointHost,
        port: endpointPort,
        stream: tlsIdentity
          ? () =>
              new SqlTlsSocket(
                endpointHost || "localhost",
                endpointPort ?? 3306,
                tlsIdentity,
              ).connect(endpointPort ?? 3306)
          : undefined,
        database: this.config.database,
        user: this.config.username,
        password: this.config.password,
        connectTimeout: this.getConnectionTimeoutMs(),
        dateStrings: true,
        jsonStrings: true,
        decimalNumbers: false,
        bigNumberStrings: true,
        supportBigNumbers: true,
        queryFormat: formatMysqlQuery,
        ssl,
      };
      pool = mysql.createPool({
        ...connectionOptions,
        waitForConnections: true,
        connectionLimit: 5,
        idleTimeout: 30000,
      });
      const connections = new Set<NativeMysqlConnection>();
      this.poolConnections.set(pool, connections);
      pool.on?.("connection", (connection) => {
        const native = connection as unknown as NativeMysqlConnection;
        connections.add(native);
        const forget = () => connections.delete(native);
        native.once("end", forget);
        native.once("error", forget);
        const setAutocommit =
          this.forceSessionAutocommit || !mysqlHandshakeAutocommit(native);
        if (setAutocommit) {
          native.query(MYSQL_SESSION_INIT_SQL, (error: Error | null) => {
            if (error) this.failSessionInitialization(native, error);
          });
        }
        const identitySql = mysqlServerIdentitySql(native);
        this.sessionReady.set(
          native,
          new Promise<void>((resolve) => {
            native.query(identitySql, (error: Error | null, rows: unknown) => {
              const row = Array.isArray(rows) ? rows[0] : undefined;
              if (
                !error &&
                !setAutocommit &&
                row &&
                typeof row === "object" &&
                Number(Reflect.get(row, "autocommit")) === 0
              ) {
                this.forceSessionAutocommit = true;
              }
              const identity = error ? null : readMysqlServerIdentity(rows);
              if (identity && identity.connectionId === native.threadId) {
                this.killTargets.set(native, {
                  threadId: identity.connectionId,
                  identitySql,
                  identity: identity.identity,
                });
              }
              resolve();
            });
          }),
        );
      });
      const forcedBeforeProbe = this.forceSessionAutocommit;
      const conn = await pool.getConnection();
      const probe = Reflect.get(conn, "connection");
      const ready =
        probe && typeof probe === "object"
          ? this.sessionReady.get(probe)
          : undefined;
      let probeAnswered = true;
      if (ready) {
        const connectionTimeoutMs = this.getConnectionTimeoutMs();
        let timer: ReturnType<typeof setTimeout> | undefined;
        probeAnswered = await Promise.race([
          ready.then(() => true),
          new Promise<boolean>((resolve) => {
            timer = setTimeout(
              () => resolve(false),
              Math.min(
                MYSQL_SESSION_PROBE_TIMEOUT_MS,
                connectionTimeoutMs > 0
                  ? connectionTimeoutMs
                  : MYSQL_SESSION_PROBE_TIMEOUT_MS,
              ),
            );
          }),
        ]);
        clearTimeout(timer);
      }
      if (
        !probeAnswered ||
        (this.forceSessionAutocommit && !forcedBeforeProbe)
      ) {
        conn.destroy();
      } else {
        conn.release();
      }
      this.assertConnectionEpoch(epoch);
      this.pool = pool;
      this.killQueryConnectionOptions = connectionOptions;
      pool = undefined;
    } catch (error) {
      if (pool) await this.closePool(pool).catch(() => undefined);
      throw error;
    } finally {
      if (this.connectionAbortController === abortController) {
        this.connectionAbortController = undefined;
      }
    }
  }

  private assertConnectionEpoch(epoch: number): void {
    if (epoch !== this.connectionEpoch) {
      throw new DOMException(
        "MySQL connection attempt cancelled",
        "AbortError",
      );
    }
  }
  async disconnect(): Promise<void> {
    this.connectionEpoch += 1;
    this.connectionAbortController?.abort();
    this.connectionAbortController = undefined;
    this.killQueryConnectionOptions = undefined;
    this.tableIndexCache.clear();
    const pool = this.pool;
    this.pool = null;
    if (pool) await this.closePool(pool);
  }

  private failSessionInitialization(
    native: NativeMysqlConnection,
    error: Error,
  ): void {
    const sessionError = new Error(
      `MySQL session initialization (${MYSQL_SESSION_INIT_SQL}) failed: ${normalizeUnknownError(error, this.config).message}`,
    );
    logger.error("MySQL session initialization failed", error);
    const fail = Reflect.get(native, "_handleFatalError");
    try {
      if (typeof fail === "function") fail.call(native, sessionError);
    } finally {
      native.destroy();
    }
  }

  private async closePool(pool: Pool): Promise<void> {
    const connections = this.poolConnections.get(pool);
    this.poolConnections.delete(pool);
    const discard = () => {
      for (const connection of connections ?? []) {
        connections?.delete(connection);
        try {
          try {
            connection.destroy();
          } finally {
            const stream = (
              connection as unknown as {
                stream?: { destroy(error?: Error): void };
              }
            ).stream;
            stream?.destroy(
              new Error("MySQL connection closed during pool shutdown."),
            );
          }
        } catch (error) {
          logger.error("MySQL transport cleanup failed", error);
        }
      }
    };
    try {
      await closeDriverResource(() => pool.end(), discard);
    } finally {
      discard();
      connections?.clear();
    }
  }

  private interruptAndDestroyConnection(connection: PoolConnection): void {
    const native = Reflect.get(connection, "connection");
    const target =
      native && typeof native === "object"
        ? this.killTargets.get(native)
        : undefined;
    const kill =
      target && target.threadId === connection.threadId
        ? this.killServerQuery(target)
        : undefined;
    if (!kill) {
      connection.destroy();
      return;
    }
    void kill.finally(() => connection.destroy());
  }

  private killServerQuery(target: MysqlKillTarget): Promise<void> | undefined {
    const options = this.killQueryConnectionOptions;
    const { threadId } = target;
    if (!options || !Number.isSafeInteger(threadId) || threadId <= 0) {
      return undefined;
    }
    let abandoned = false;
    let killer: Awaited<ReturnType<typeof mysql.createConnection>> | undefined;
    const dispose = () => {
      const connection = killer;
      killer = undefined;
      connection?.destroy();
    };
    const attempt = (async () => {
      const connection = await mysql.createConnection({
        ...options,
        connectTimeout: MYSQL_KILL_QUERY_TIMEOUT_MS,
      });
      killer = connection;
      if (abandoned) return;
      const [rows] = await connection.query({
        sql: target.identitySql,
        timeout: MYSQL_KILL_QUERY_TIMEOUT_MS,
      });
      if (abandoned) return;
      if (readMysqlServerIdentity(rows)?.identity !== target.identity) {
        throw new Error(
          "the cancellation connection reached a different server",
        );
      }
      await connection.query({
        sql: `KILL QUERY ${threadId}`,
        timeout: MYSQL_KILL_QUERY_TIMEOUT_MS,
      });
    })();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const deadline = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(
        () => reject(new Error("MySQL KILL QUERY timed out.")),
        MYSQL_KILL_QUERY_TIMEOUT_MS,
      );
    });
    return Promise.race([attempt, deadline])
      .catch((error: unknown) => {
        logger.warn(
          `MySQL KILL QUERY ${threadId} failed: ${normalizeUnknownError(error, this.config).message}`,
        );
      })
      .finally(() => {
        abandoned = true;
        clearTimeout(timer);
        dispose();
        void attempt.then(dispose, dispose);
      });
  }

  private cancelQueryOperation(operation: MysqlQueryOperation): void {
    operation.cancelled = true;
    if (operation.reject) {
      operation.reject(new Error("SQL query cancelled."));
    } else if (
      operation.connection &&
      this.activeQueryConnections.delete(operation.connection)
    ) {
      this.interruptAndDestroyConnection(operation.connection);
    }
  }

  async cancelCurrentOperation(
    context?: OperationCancellationContext,
  ): Promise<void> {
    if (context?.operationName === "query") {
      if (context.requestToken === undefined) {
        return;
      }
      for (const operation of this.activeQueryOperations) {
        if (operation.requestToken !== context.requestToken) {
          continue;
        }
        this.cancelQueryOperation(operation);
      }
      return;
    }

    if (context?.operationName === "runTransaction" || context === undefined) {
      const hadTransactionConnection =
        this.activeTransactionConnections.size > 0;
      for (const connection of [...this.activeTransactionConnections]) {
        this.activeTransactionConnections.delete(connection);
        this.interruptAndDestroyConnection(connection);
      }
      if (context === undefined && !hadTransactionConnection) {
        await this.recycleConnectionAfterTimeout();
      }
    }
  }

  protected override getQuestionMarkPlaceholderOptions() {
    return {
      hashLineComments: true,
      dashCommentRequiresWhitespace: true,
    } as const;
  }

  async recycleConnectionAfterTimeout(_context?: {
    timeoutKind?: "connection" | "dbOperation";
    operationName?: string;
  }): Promise<void> {
    if (this.timeoutRecoveryInFlight) {
      await this.timeoutRecoveryInFlight;
      return;
    }

    const recover = async () => {
      const wasConnected = this.isConnected();
      await this.disconnect().catch(() => undefined);
      if (wasConnected) {
        await this.connect().catch(() => undefined);
      }
    };

    this.timeoutRecoveryInFlight = recover().finally(() => {
      this.timeoutRecoveryInFlight = null;
    });

    await this.timeoutRecoveryInFlight;
  }

  isConnected(): boolean {
    return this.pool !== null;
  }

  getEntityManifest(): DriverEntityManifest {
    return MYSQL_ENTITY_MANIFEST;
  }
  private requirePool(): Pool {
    if (!this.pool) {
      throw new Error("[RapiDB] MySQL connection is not established.");
    }
    return this.pool;
  }
  private destroyMetadataConnection(
    connection: PoolConnection | NativeMysqlConnection,
  ): void {
    const native = ("connection" in connection
      ? connection.connection
      : connection) as unknown as {
      stream?: { destroy(error?: Error): void };
    };
    const resource = native ?? connection;
    if (this.discardedMetadataConnections.has(resource)) return;
    this.discardedMetadataConnections.add(resource);
    try {
      connection.destroy();
    } finally {
      native?.stream?.destroy(new Error("MySQL metadata operation cancelled."));
    }
  }

  private async acquireMetadataConnection(
    pool: Pool,
    signal: AbortSignal,
  ): Promise<PoolConnection> {
    const core = pool.pool as unknown as {
      _freeConnections: { length: number };
      _allConnections: {
        length: number;
        get?(index: number): NativeMysqlConnection;
      };
      _connectionQueue: { length: number };
      config: { connectionLimit: number };
    };
    if (
      !core?._freeConnections ||
      !core._allConnections ||
      !core._connectionQueue
    )
      throw new Error("MySQL metadata checkout capacity is unavailable.");
    while (true) {
      signal.throwIfAborted();
      if (pool !== this.pool)
        throw new Error("MySQL metadata connection closed.");
      if (
        core._connectionQueue.length === 0 &&
        (core._freeConnections.length > 0 ||
          core.config.connectionLimit === 0 ||
          core._allConnections.length < core.config.connectionLimit)
      ) {
        let onAbort: () => void = () => undefined;
        try {
          return await new Promise<PoolConnection>((resolve, reject) => {
            let cancelled = false;
            let openingConnection: NativeMysqlConnection | undefined;
            onAbort = () => {
              cancelled = true;
              try {
                if (openingConnection)
                  this.destroyMetadataConnection(openingConnection);
              } catch (error) {
                logger.error(
                  "MySQL metadata handshake cancellation failed",
                  error,
                );
              } finally {
                reject(signal.reason);
              }
            };
            signal.addEventListener("abort", onAbort, { once: true });
            const beforeCheckout = core._allConnections.length;
            const checkout = pool.getConnection();
            if (core._allConnections.length > beforeCheckout)
              openingConnection = core._allConnections.get?.(beforeCheckout);
            if (signal.aborted) onAbort();
            void checkout.then((connection) => {
              openingConnection = undefined;
              if (!cancelled && pool === this.pool) resolve(connection);
              else {
                try {
                  this.destroyMetadataConnection(connection);
                } catch (error) {
                  logger.error(
                    "MySQL cancelled metadata checkout cleanup failed",
                    error,
                  );
                }
                reject(new Error("MySQL metadata checkout cancelled."));
              }
            }, reject);
          });
        } finally {
          signal.removeEventListener("abort", onAbort);
        }
      }
      await new Promise<void>((resolve, reject) => {
        const finish = () => {
          clearTimeout(timer);
          signal.removeEventListener("abort", abort);
        };
        const abort = () => {
          finish();
          reject(signal.reason);
        };
        const timer = setTimeout(() => {
          finish();
          resolve();
        }, 25);
        signal.addEventListener("abort", abort, { once: true });
      });
    }
  }

  private async queryObjectRows<TRow extends RowDataPacket = RowDataPacket>(
    sql: string,
    params?: QueryOptions["values"],
  ): Promise<TRow[]> {
    const metadata = getMetadataContext();
    if (metadata) {
      const connection = await this.acquireMetadataConnection(
        this.requirePool(),
        metadata.signal,
      );
      let destroyed = false;
      const destroy = () => {
        if (destroyed) return;
        destroyed = true;
        this.destroyMetadataConnection(connection);
      };
      try {
        metadata.signal.throwIfAborted();
        const [rows] = await withMetadataResource(destroy, () =>
          connection.query<TRow[]>(this.createQueryOptions(sql, params)),
        );
        return rows;
      } finally {
        if (metadata.signal.aborted) destroy();
        else if (!destroyed) connection.release();
      }
    }
    const [rows] = await this.requirePool().query<TRow[]>(
      this.createQueryOptions(sql, params),
    );
    return rows;
  }
  private async queryArrayRows(
    queryable: Pool | PoolConnection,
    options: QueryOptions,
  ): Promise<[MysqlQueryRows, MysqlQueryFields]> {
    for (const operation of this.activeQueryOperations) {
      if (operation.connection === queryable && operation.cancelled) {
        throw new Error("SQL query cancelled.");
      }
    }
    const [rows, fields] = await queryable.query({
      ...options,
      timeout: this.getDbOperationTimeoutMs(),
      rowsAsArray: true,
      typeCast: (field, next) =>
        field.type === "GEOMETRY" ? field.buffer() : next(),
    });
    return [rows as MysqlQueryRows, fields as MysqlQueryFields];
  }
  async listDatabases(): Promise<DatabaseInfo[]> {
    const rows = await this.queryObjectRows<MysqlObjectRow>(
      `SELECT SCHEMA_NAME
       FROM information_schema.SCHEMATA
       WHERE SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
       ORDER BY SCHEMA_NAME`,
    );
    return rows.map((r) => ({
      name: String(Object.values(r)[0] ?? ""),
      schemas: [],
    }));
  }
  async listSchemas(_database: string): Promise<SchemaInfo[]> {
    return [{ name: _database }];
  }
  async listObjects(database: string, _schema: string): Promise<TableInfo[]> {
    const tableRows = await this.queryObjectRows<
      RowDataPacket & {
        name: string;
        type: string;
      }
    >(
      `SELECT TABLE_NAME AS name, TABLE_TYPE AS type
       FROM information_schema.TABLES
       WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME`,
      [database],
    );
    const objects: TableInfo[] = tableRows.map((r) => ({
      schema: database,
      name: r.name as string,
      type: (r.type === "VIEW" ? "view" : "table") as TableInfo["type"],
    }));
    try {
      const fnRows = await this.queryObjectRows<
        RowDataPacket & {
          name: string;
          type: string;
        }
      >(
        `SELECT ROUTINE_NAME AS name, ROUTINE_TYPE AS type
         FROM information_schema.ROUTINES
         WHERE ROUTINE_SCHEMA = ? ORDER BY ROUTINE_NAME`,
        [database],
      );
      for (const r of fnRows) {
        objects.push({
          schema: database,
          name: r.name as string,
          type: (r.type === "FUNCTION"
            ? "function"
            : "procedure") as TableInfo["type"],
        });
      }
    } catch {}
    return objects;
  }
  async describeTable(
    database: string,
    _schema: string,
    table: string,
  ): Promise<ColumnMeta[]> {
    const rows = await this.queryObjectRows<
      RowDataPacket & {
        COLUMN_NAME: string;
        COLUMN_TYPE: string;
        CHARACTER_SET_NAME: string | null;
        COLLATION_NAME: string | null;
        IS_NULLABLE: string;
        COLUMN_DEFAULT: unknown;
        GENERATION_EXPRESSION: string | null;
        EXTRA: string | null;
        IS_PRIMARY_KEY: number | string;
        PRIMARY_KEY_ORDINAL: number | string | null;
        IS_FOREIGN_KEY: number | string;
      }
    >(
      `SELECT c.COLUMN_NAME,
              c.COLUMN_TYPE,
              c.CHARACTER_SET_NAME,
              c.COLLATION_NAME,
              c.IS_NULLABLE,
              c.COLUMN_DEFAULT,
              c.GENERATION_EXPRESSION,
              c.EXTRA,
              CASE WHEN pk.COLUMN_NAME IS NULL THEN 0 ELSE 1 END AS IS_PRIMARY_KEY,
              pk.ORDINAL_POSITION AS PRIMARY_KEY_ORDINAL,
              CASE WHEN fk.COLUMN_NAME IS NULL THEN 0 ELSE 1 END AS IS_FOREIGN_KEY
       FROM information_schema.COLUMNS c
       LEFT JOIN (
         SELECT kcu.COLUMN_NAME, kcu.ORDINAL_POSITION
         FROM information_schema.KEY_COLUMN_USAGE kcu
         JOIN information_schema.TABLE_CONSTRAINTS tc
           ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
          AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
          AND tc.TABLE_NAME = kcu.TABLE_NAME
          AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
         WHERE kcu.TABLE_SCHEMA = ?
           AND kcu.TABLE_NAME = ?
           AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
       ) pk ON pk.COLUMN_NAME = c.COLUMN_NAME
       LEFT JOIN (
         SELECT DISTINCT kcu.COLUMN_NAME
         FROM information_schema.KEY_COLUMN_USAGE kcu
         JOIN information_schema.TABLE_CONSTRAINTS tc
           ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
          AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
          AND tc.TABLE_NAME = kcu.TABLE_NAME
          AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
         WHERE kcu.TABLE_SCHEMA = ?
           AND kcu.TABLE_NAME = ?
           AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
           AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
       ) fk ON fk.COLUMN_NAME = c.COLUMN_NAME
       WHERE c.TABLE_SCHEMA = ?
         AND c.TABLE_NAME = ?
       ORDER BY c.ORDINAL_POSITION`,
      [database, table, database, table, database, table],
    );
    let checkClauses: string[] = [];
    if (rows.some((row) => this.mapTypeCategory(row.COLUMN_TYPE) === "text")) {
      try {
        const checkRows = await this.queryObjectRows<
          RowDataPacket & {
            CHECK_CLAUSE: string | null;
            ENFORCED?: string | null;
          }
        >(
          // NATURAL JOIN also matches TABLE_NAME on MariaDB, whose
          // CHECK_CONSTRAINTS names are only unique per table.
          `SELECT cc.CHECK_CLAUSE, tc.*
           FROM information_schema.TABLE_CONSTRAINTS tc
           NATURAL JOIN information_schema.CHECK_CONSTRAINTS cc
           WHERE tc.TABLE_SCHEMA = ?
             AND tc.TABLE_NAME = ?
             AND tc.CONSTRAINT_TYPE = 'CHECK'`,
          [database, table],
        );
        checkClauses = checkRows
          .filter((row) => row.ENFORCED === undefined || row.ENFORCED === "YES")
          .map((row) => row.CHECK_CLAUSE)
          .filter((clause): clause is string => typeof clause === "string");
      } catch (error) {
        if (!isUnavailableCheckMetadataError(error)) throw error;
      }
    }
    return rows.map((r) => {
      const generationExpression =
        typeof r.GENERATION_EXPRESSION === "string" &&
        r.GENERATION_EXPRESSION.trim().length > 0
          ? r.GENERATION_EXPRESSION.trim()
          : undefined;
      const extraMetadata = parseMysqlExtraMetadata(
        r.EXTRA as string | null | undefined,
      );
      const isComputed =
        generationExpression !== undefined ||
        extraMetadata.generatedKind !== undefined;
      const defaultValue =
        !isComputed && r.COLUMN_DEFAULT != null
          ? String(r.COLUMN_DEFAULT)
          : undefined;
      const isJsonAlias =
        this.mapTypeCategory(r.COLUMN_TYPE) === "text" &&
        hasJsonValidColumnCheck(checkClauses, r.COLUMN_NAME);
      return {
        name: r.COLUMN_NAME as string,
        type: r.COLUMN_TYPE as string,
        ...(isJsonAlias ? { exportCategory: "json" as const } : {}),
        characterSet:
          typeof r.CHARACTER_SET_NAME === "string"
            ? r.CHARACTER_SET_NAME
            : undefined,
        collation:
          typeof r.COLLATION_NAME === "string" ? r.COLLATION_NAME : undefined,
        nullable: r.IS_NULLABLE === "YES",
        defaultValue,
        identityGeneration: extraMetadata.identityGeneration,
        onUpdateExpression: extraMetadata.onUpdateExpression,
        isComputed,
        computedExpression: generationExpression,
        generatedKind: extraMetadata.generatedKind,
        isPersisted:
          extraMetadata.generatedKind === undefined
            ? undefined
            : extraMetadata.generatedKind === "stored",
        isPrimaryKey: Number(r.IS_PRIMARY_KEY) === 1,
        primaryKeyOrdinal:
          r.PRIMARY_KEY_ORDINAL == null
            ? undefined
            : Number(r.PRIMARY_KEY_ORDINAL),
        isForeignKey: Number(r.IS_FOREIGN_KEY) === 1,
      };
    });
  }
  async query(
    sql: string,
    params?: unknown[],
    operationContext?: QueryExecutionOptions,
  ): Promise<QueryResult> {
    const start = Date.now();
    const hardCap = queryCollectionLimit(operationContext?.hardCap);
    if (params && params.length > 0) {
      return this.withTrackedQueryConnection(
        (connection) =>
          this.executeQueryStatements(
            connection,
            [sql],
            params,
            hardCap,
            operationContext,
            start,
          ),
        operationContext,
      );
    }
    const split = splitMySQLQueryScript(sql);
    if (Array.isArray(split) && split.length === 0) {
      return emptyMysqlQueryResult(hardCap);
    }
    return this.withTrackedQueryConnection(async (connection) => {
      let statements: string[];
      if (Array.isArray(split)) {
        statements = split;
      } else {
        const modes = await this.readSessionSqlModes(connection);
        if (!modes) {
          throw new Error(
            "Cannot determine the session SQL mode needed to split a script containing backslashes; script was not submitted.",
          );
        }
        statements = split(!modes.includes("NO_BACKSLASH_ESCAPES"));
        if (statements.length === 0) return emptyMysqlQueryResult(hardCap);
      }
      return this.executeQueryStatements(
        connection,
        statements,
        undefined,
        hardCap,
        operationContext,
        start,
      );
    }, operationContext);
  }

  private executeQueryStatements(
    connection: PoolConnection,
    statements: string[],
    params: unknown[] | undefined,
    hardCap: number | undefined,
    operationContext: QueryExecutionOptions | undefined,
    start: number,
  ): Promise<QueryResult> {
    if (hardCap !== undefined) {
      createMysqlStreamingQuery(
        connection.connection as unknown as NativeMysqlConnection,
        { sql: "", rowsAsArray: true },
      );
      return this.withReadOnlyTransaction(
        connection,
        operationContext,
        async () => {
          await this.assertUnambiguousQueryTimestampWrites(
            connection,
            statements,
            params,
          );
          return this.executeBoundedScript(
            connection,
            statements,
            params,
            hardCap,
            start,
          );
        },
      );
    }
    if (params && params.length > 0) {
      return this.withReadOnlyTransaction(
        connection,
        operationContext,
        async () => {
          await this.assertUnambiguousQueryTimestampWrites(
            connection,
            statements,
            params,
          );
          const [rawRows, fields] = await this.queryArrayRows(connection, {
            sql: statements[0],
            values: params as QueryOptions["values"],
          });
          return this._parseQueryResponse(rawRows, fields, Date.now() - start);
        },
      );
    }
    if (statements.length === 1) {
      return this.withReadOnlyTransaction(
        connection,
        operationContext,
        async () => {
          const [rawRows, fields] = await this.queryArrayRows(connection, {
            sql: statements[0],
          });
          return this._parseQueryResponse(rawRows, fields, Date.now() - start);
        },
      );
    }
    return this.withReadOnlyTransaction(connection, operationContext, () =>
      this._executeScript(connection, statements, start),
    );
  }

  private async readSessionSqlModes(
    conn: PoolConnection,
  ): Promise<string[] | undefined> {
    const [modes] = await conn.query<RowDataPacket[]>(
      this.createQueryOptions("SELECT @@SESSION.sql_mode AS __rapidb_sql_mode"),
    );
    const mode: unknown = modes[0]?.__rapidb_sql_mode;
    return modes.length === 1 && typeof mode === "string"
      ? mode.split(",").map((part) => part.trim().toUpperCase())
      : undefined;
  }

  override getCapabilities() {
    return { ...super.getCapabilities(), boundedQueryResults: true };
  }

  private async executeBoundedScript(
    connection: PoolConnection,
    statements: string[],
    params: unknown[] | undefined,
    limit: number,
    start: number,
  ): Promise<QueryResult> {
    const capturedResultSets: MysqlCapturedResultSet[] = [];
    let retainedRowCount = 0;
    let retainedColumnCount = 0;
    let seenResultSetCount = 0;
    let resultSetsTruncated = false;
    let affectedRows = 0;
    const operation = [...this.activeQueryOperations].find(
      (entry) => entry.connection === connection,
    );

    const createResult = (error?: unknown): QueryResult => {
      const resultSets: QueryResultSet[] = capturedResultSets.map(
        (captured, index) => {
          const parsed = this._parseQueryResult(
            captured.rows,
            captured.fields,
            0,
          );
          return {
            id: `result-${index + 1}`,
            columns: parsed.columns,
            columnMeta: parsed.columnMeta,
            rows: parsed.rows,
            rowCount: captured.rowCount,
            truncated: captured.truncated,
            ...(captured.partial ? { partial: true } : {}),
          };
        },
      );
      const firstResultSet = resultSets[0];
      return {
        columns: firstResultSet?.columns ?? [],
        columnMeta: firstResultSet?.columnMeta ?? [],
        rows: firstResultSet?.rows ?? [],
        rowCount:
          resultSets.length > 0
            ? resultSets.reduce(
                (total, resultSet) => total + resultSet.rowCount,
                0,
              )
            : affectedRows,
        executionTimeMs: Date.now() - start,
        ...(resultSets.length > 0 ? { resultSets } : {}),
        ...(resultSets.some((resultSet) => resultSet.truncated)
          ? { truncated: true }
          : { truncated: false }),
        ...(resultSets.length === 0 && affectedRows > 0
          ? { affectedRows }
          : {}),
        ...(resultSetsTruncated ? { resultSetsTruncated: true } : {}),
        ...(error !== undefined
          ? { error: normalizeUnknownError(error, this.config).message }
          : {}),
      };
    };

    for (const sql of statements) {
      if (operation?.cancelled) throw new Error("SQL query cancelled.");
      let activeResultSet: MysqlCapturedResultSet | undefined;
      try {
        await new Promise<void>((resolve, reject) => {
          const native =
            connection.connection as unknown as NativeMysqlConnection;
          const command = createMysqlStreamingQuery(native, {
            sql,
            values: params as QueryOptions["values"],
            rowsAsArray: true,
          });
          let settled = false;
          const timeoutMs = this.getDbOperationTimeoutMs();
          const timer = setTimeout(
            () =>
              fail(new DriverTimeoutError("dbOperation", "query", timeoutMs)),
            timeoutMs,
          );
          const cleanup = () => {
            clearTimeout(timer);
            command.off("fields", onFields);
            command.off("result", onResult);
            command.off("end", onEnd);
            command.off("error", fail);
            command.on("error", () => undefined);
            if (operation) operation.reject = undefined;
          };
          const fail = (error: Error) => {
            if (settled) return;
            settled = true;
            cleanup();
            if (isMysqlServerQueryError(error)) {
              if (activeResultSet && !activeResultSet.completed) {
                const resultIndex = Reflect.get(command, "_resultIndex");
                if (
                  typeof resultIndex === "number" &&
                  resultIndex > activeResultSet.protocolResultIndex
                ) {
                  activeResultSet.completed = true;
                } else {
                  activeResultSet.partial = true;
                }
              }
            } else {
              this.activeQueryConnections.delete(connection);
              if (Reflect.get(error, "fatal") === true) connection.destroy();
              else this.interruptAndDestroyConnection(connection);
            }
            reject(error);
          };
          if (operation) operation.reject = fail;
          const onFields = (nextFields: FieldPacket[] | undefined) => {
            if (settled) return;
            if (activeResultSet) activeResultSet.completed = true;
            activeResultSet = undefined;
            discardMysqlResultHistory(command, nextFields);
            if (!nextFields?.length) return;
            seenResultSetCount++;
            if (
              seenResultSetCount > MYSQL_MAX_RETAINED_RESULT_SETS ||
              retainedColumnCount + nextFields.length >
                MYSQL_MAX_RETAINED_RESULT_COLUMNS
            ) {
              resultSetsTruncated = true;
              return;
            }
            activeResultSet = {
              fields: nextFields,
              rows: [],
              rowCount: 0,
              truncated: false,
              protocolResultIndex:
                typeof Reflect.get(command, "_resultIndex") === "number"
                  ? Reflect.get(command, "_resultIndex")
                  : 0,
              completed: false,
            };
            capturedResultSets.push(activeResultSet);
            retainedColumnCount += nextFields.length;
          };
          const onResult = (row: unknown[] | ResultSetHeader) => {
            if (settled) return;
            if (Array.isArray(row)) {
              if (!activeResultSet) return;
              activeResultSet.rowCount++;
              if (retainedRowCount < limit) {
                activeResultSet.rows.push(row);
                retainedRowCount++;
              } else {
                activeResultSet.truncated = true;
              }
            } else {
              affectedRows += row.affectedRows ?? 0;
            }
          };
          const onEnd = () => {
            if (settled) return;
            settled = true;
            if (activeResultSet) activeResultSet.completed = true;
            cleanup();
            resolve();
          };
          command.on("fields", onFields);
          command.on("result", onResult);
          command.on("error", fail);
          command.on("end", onEnd);
          try {
            const submit = native.query as unknown as (command: Query) => Query;
            submit.call(native, command);
          } catch (error) {
            fail(error instanceof Error ? error : new Error(String(error)));
          }
        });
      } catch (error) {
        if (
          isMysqlServerQueryError(error) &&
          (capturedResultSets.length > 0 || resultSetsTruncated)
        ) {
          return createResult(error);
        }
        throw error;
      }
    }
    return createResult();
  }

  private async withReadOnlyTransaction<T>(
    connection: PoolConnection,
    operationContext: { readOnly?: boolean } | undefined,
    operation: () => Promise<T>,
  ): Promise<T> {
    if (!operationContext?.readOnly) {
      return operation();
    }
    await connection.query("START TRANSACTION READ ONLY");
    let result: T | undefined;
    let operationError: unknown;
    let operationFailed = false;
    try {
      result = await operation();
    } catch (error) {
      operationFailed = true;
      operationError = error;
    }
    try {
      await connection.query("ROLLBACK");
    } catch (rollbackError) {
      this.activeQueryConnections.delete(connection);
      connection.destroy();
      const rollbackMessage = normalizeUnknownError(
        rollbackError,
        this.config,
      ).message;
      const cleanupDiagnostic = `Read-only transaction rollback failed: ${rollbackMessage}; the connection was discarded.`;
      if (operationFailed) {
        const operationMessage = normalizeUnknownError(
          operationError,
          this.config,
        ).message;
        throw new AggregateError(
          [operationError, rollbackError],
          `${operationMessage}; ${cleanupDiagnostic}`,
        );
      }
      if (isQueryResult(result)) {
        return {
          ...result,
          error: result.error
            ? `${result.error}; ${cleanupDiagnostic}`
            : `Query returned results, but ${cleanupDiagnostic}`,
        } as T;
      }
      throw new AggregateError([rollbackError], cleanupDiagnostic);
    }
    if (operationFailed) throw operationError;
    return result as T;
  }

  private async withTrackedQueryConnection<T>(
    operation: (connection: PoolConnection) => Promise<T>,
    operationContext?: {
      requestToken?: number;
      readOnly?: boolean;
      signal?: AbortSignal;
    },
  ): Promise<T> {
    const queryOperation: MysqlQueryOperation = {
      cancelled: false,
      requestToken: operationContext?.requestToken,
    };
    this.activeQueryOperations.add(queryOperation);
    let connection: PoolConnection | undefined;
    let slotAcquired = false;
    let interrupted = false;
    const signal = operationContext?.signal;
    const onAbort = () => this.cancelQueryOperation(queryOperation);
    signal?.addEventListener("abort", onAbort, { once: true });
    try {
      if (signal?.aborted) onAbort();
      await this.waitForQueryConnectionSlot(queryOperation);
      this.activeQueryConnectionSlots += 1;
      slotAcquired = true;
      connection = await this.requirePool().getConnection();
      queryOperation.connection = connection;
      if (queryOperation.cancelled) {
        connection.destroy();
        throw new Error("SQL query cancelled before execution.");
      }
      this.activeQueryConnections.add(connection);
      const result = await operation(connection);
      if (queryOperation.cancelled) throw new Error("SQL query cancelled.");
      if (!this.activeQueryConnections.has(connection)) return result;
      const activeConnection = connection;
      const cleanupQuery = (sql: string): Promise<ResultSetHeader> => {
        let timer: ReturnType<typeof setTimeout>;
        return new Promise<ResultSetHeader>((resolve, reject) => {
          const timeoutMs = this.getDbOperationTimeoutMs();
          timer = setTimeout(
            () =>
              reject(new DriverTimeoutError("dbOperation", "query", timeoutMs)),
            timeoutMs,
          );
          queryOperation.reject = reject;
          activeConnection
            .query<ResultSetHeader>(this.createQueryOptions(sql))
            .then(([header]) => resolve(header), reject);
        }).finally(() => {
          clearTimeout(timer);
          queryOperation.reject = undefined;
        });
      };
      try {
        const status = await cleanupQuery("DO 0");
        if (
          !status ||
          Array.isArray(status) ||
          !Number.isInteger(status.serverStatus) ||
          status.serverStatus < 0 ||
          status.serverStatus > 0xffff
        ) {
          throw new Error(
            "Cannot verify MySQL transaction state; connection was discarded.",
          );
        }
        if ((status.serverStatus & 1) !== 0) {
          await cleanupQuery("ROLLBACK");
          throw new Error(
            "Unfinished MySQL transaction was rolled back. Execute BEGIN and COMMIT/ROLLBACK in the same script. Earlier commits and nontransactional changes are not undone.",
          );
        }
      } catch (cleanupError) {
        if (
          queryOperation.cancelled ||
          !isQueryResult(result) ||
          !result.error
        ) {
          throw cleanupError;
        }
        return {
          ...result,
          error: `${result.error}; Query cleanup: ${
            normalizeUnknownError(cleanupError, this.config).message
          }`,
        } as T;
      }
      if (queryOperation.cancelled) throw new Error("SQL query cancelled.");
      return result;
    } catch (error) {
      interrupted =
        queryOperation.cancelled ||
        error instanceof DriverTimeoutError ||
        Reflect.get(Object(error), "code") === "PROTOCOL_SEQUENCE_TIMEOUT";
      throw error;
    } finally {
      signal?.removeEventListener("abort", onAbort);
      this.activeQueryOperations.delete(queryOperation);
      if (connection && this.activeQueryConnections.delete(connection)) {
        if (interrupted) this.interruptAndDestroyConnection(connection);
        else connection.destroy();
      }
      if (slotAcquired) {
        this.activeQueryConnectionSlots -= 1;
      }
    }
  }

  private async waitForQueryConnectionSlot(
    operation: MysqlQueryOperation,
  ): Promise<void> {
    while (
      this.activeQueryConnectionSlots >= MYSQL_POOL_CONNECTION_LIMIT ||
      this.activeTransactionConnections.size >= MYSQL_POOL_CONNECTION_LIMIT
    ) {
      if (operation.cancelled) {
        throw new Error("SQL query cancelled before execution.");
      }
      await new Promise<void>((resolve) => setTimeout(resolve, 25));
    }
    if (operation.cancelled) {
      throw new Error("SQL query cancelled before execution.");
    }
  }
  clearTableReadCache(): void {
    this.tableIndexCache.clear();
  }

  private async getCachedTableIndexes(
    request: DriverTablePageRequest,
    signal: AbortSignal,
  ): Promise<import("./types").IndexMeta[]> {
    const key = JSON.stringify([request.database, request.table]);
    const cached = this.tableIndexCache.get(key);
    if (cached) return cached;
    try {
      const indexes = await runWithMetadataCancellation(
        signal,
        request.deadline ?? Number.POSITIVE_INFINITY,
        () => this.getIndexes(request.database, request.schema, request.table),
      );
      this.tableIndexCache.set(key, indexes);
      return indexes;
    } catch (error) {
      signal.throwIfAborted();
      logger.warn(
        `MySQL index lookup for table ordering failed: ${normalizeUnknownError(error, this.config).message}`,
      );
      return [];
    }
  }

  async readTablePage(
    request: DriverTablePageRequest,
  ): Promise<DriverTablePageResult> {
    const controller = new AbortController();
    const onAbort = () => controller.abort(request.signal?.reason);
    request.signal?.addEventListener("abort", onAbort, { once: true });
    if (request.signal?.aborted) onAbort();
    const remainingMs =
      request.deadline === undefined
        ? Number.POSITIVE_INFINITY
        : Math.max(0, request.deadline - Date.now());
    const timer = Number.isFinite(remainingMs)
      ? setTimeout(
          () =>
            controller.abort(
              new DriverTimeoutError(
                "dbOperation",
                "readTablePage",
                remainingMs,
              ),
            ),
          remainingMs,
        )
      : undefined;
    try {
      return await this.readTablePageWithSignal(request, controller.signal);
    } catch (error) {
      if (controller.signal.aborted) throw controller.signal.reason;
      throw error;
    } finally {
      clearTimeout(timer);
      request.signal?.removeEventListener("abort", onAbort);
    }
  }

  private async readTablePageWithSignal(
    request: DriverTablePageRequest,
    signal: AbortSignal,
  ): Promise<DriverTablePageResult> {
    const startTime = performance.now();
    signal.throwIfAborted();
    const columns = await runWithMetadataCancellation(
      signal,
      request.deadline ?? Number.POSITIVE_INFINITY,
      () =>
        this.describeColumns(request.database, request.schema, request.table),
    );
    if (columns.length === 0) {
      const tableName = this.qualifiedTableName(
        request.database,
        request.schema,
        request.table,
      );
      throw new Error(
        `Table ${tableName} no longer exists or has no visible columns. Refresh the database explorer and reopen the table.`,
      );
    }
    const { clause: whereClause, params: whereParams } = buildWhere(
      this,
      request.filters,
      columns,
    );
    const qualifiedTableName = this.qualifiedTableName(
      request.database,
      request.schema,
      request.table,
    );
    const selectList = columns
      .map((column, index) => {
        const quoted = this.quoteIdentifier(column.name);
        const expression =
          column.category === "spatial"
            ? `CASE WHEN ST_SRID(${quoted}) = 0 THEN ST_AsText(${quoted}) ELSE CONCAT('SRID=', ST_SRID(${quoted}), ';', ST_AsText(${quoted})) END`
            : quoted;
        return `${expression} AS ${this.quoteIdentifier(`__rapidb_col_${index}`)}`;
      })
      .join(", ");
    const offset = (request.page - 1) * request.pageSize;
    const sortExpression = request.sort
      ? `${qualifiedTableName}.${this.quoteIdentifier(request.sort.column)}`
      : "";
    const sortValue = columns.some(
      (column) =>
        column.name === request.sort?.column && column.category === "spatial",
    )
      ? `ST_AsText(${sortExpression})`
      : sortExpression;
    let rowTieBreakers: string[] = [];
    if (!columns.some((column) => column.isPrimaryKey)) {
      const columnsByName = new Map(
        columns.map((column) => [column.name, column]),
      );
      const uniqueKey = (
        await this.getCachedTableIndexes(request, signal)
      ).find(
        (index) =>
          index.unique &&
          index.columns.length > 0 &&
          index.columns.every(
            (name) => columnsByName.get(name)?.nullable === false,
          ),
      );
      rowTieBreakers = (
        uniqueKey
          ? uniqueKey.columns
          : columns
              .filter(
                (column) =>
                  column.category !== "spatial" &&
                  column.category !== "json" &&
                  !/^(?:tiny|medium|long)?(?:text|blob)\b/i.test(
                    column.nativeType.trim(),
                  ),
              )
              .map((column) => column.name)
      )
        .filter((name) => name !== request.sort?.column)
        .map(
          (name) => `${qualifiedTableName}.${this.quoteIdentifier(name)} ASC`,
        );
    }
    const orderByClause = request.sort
      ? `ORDER BY ${[
          `${sortValue} ${request.sort.direction === "desc" ? "DESC" : "ASC"}`,
          ...columns
            .filter(
              (column) =>
                column.isPrimaryKey && column.name !== request.sort?.column,
            )
            .sort(
              (left, right) =>
                (left.primaryKeyOrdinal ?? Number.MAX_SAFE_INTEGER) -
                (right.primaryKeyOrdinal ?? Number.MAX_SAFE_INTEGER),
            )
            .map(
              (column) =>
                `${qualifiedTableName}.${this.quoteIdentifier(column.name)} ASC`,
            ),
          ...rowTieBreakers,
        ].join(", ")}`
      : this.buildOrderByDefault(columns, qualifiedTableName) ||
        (rowTieBreakers.length > 0
          ? `ORDER BY ${rowTieBreakers.join(", ")}`
          : "");

    let totalCount = 0;
    let countFailed = false;
    if (!request.skipCount) {
      try {
        const countResult = await this.query(
          `SELECT COUNT(*) AS cnt FROM ${qualifiedTableName} ${whereClause}`,
          whereParams,
          { signal },
        );
        const countRow = countResult.rows[0] as
          | Record<string, unknown>
          | undefined;
        totalCount = Number(
          countRow?.__col_0 ??
            countRow?.cnt ??
            countRow?.CNT ??
            countRow?.count ??
            0,
        );
      } catch {
        countFailed = true;
      }
    }

    signal.throwIfAborted();
    const fetchPageSize = countFailed ? request.pageSize + 1 : request.pageSize;
    const pagination = this.buildPagination(
      offset,
      fetchPageSize,
      whereParams.length + 1,
    );
    const dataResult = await this.query(
      `SELECT ${selectList} FROM ${qualifiedTableName} ${whereClause} ${orderByClause} ${pagination.sql}`,
      [...whereParams, ...pagination.params],
      { signal },
    );

    const fetchedRows = dataResult.rows.map((row) =>
      Object.fromEntries(
        columns.map((column, index) => [
          column.name,
          this.formatOutputValue(row[`__col_${index}`], column),
        ]),
      ),
    );

    const hasMoreRows = countFailed && fetchedRows.length > request.pageSize;
    const rows = hasMoreRows
      ? fetchedRows.slice(0, request.pageSize)
      : fetchedRows;
    if (countFailed) {
      totalCount = offset + rows.length + (hasMoreRows ? 1 : 0);
    }

    return {
      columns,
      rows,
      totalCount,
      executionTimeMs: Math.round(performance.now() - startTime),
    };
  }
  private _parseQueryResult(
    rawRows: MysqlResultPayload,
    fields: FieldPacket[],
    executionTimeMs: number,
  ): QueryResult {
    if (isMysqlSelectRows(rawRows)) {
      const columns = fields.map((field) => field.name);
      const columnMeta = fields.map(mysqlQueryColumnMeta);
      const boolCols = new Set<number>();
      const floatCols = new Set<number>();
      const geometryCols = new Set<number>();
      const bitIntCols = new Map<number, number>();
      fields.forEach((field, i) => {
        const fieldType = field.type ?? field.columnType;
        const fieldLength = field.length ?? field.columnLength ?? 0;
        if (fieldType === 1 && fieldLength === 1) {
          boolCols.add(i);
        } else if (fieldType === 16) {
          bitIntCols.set(i, fieldLength);
        } else if (fieldType === 255) {
          geometryCols.add(i);
        }
        if (fieldType === 4) {
          floatCols.add(i);
        }
      });
      const rows = rawRows.map((row) =>
        Object.fromEntries(
          row.map((val, i) => {
            let v = val;
            if (boolCols.has(i) && typeof v === "boolean") {
              v = v ? 1 : 0;
            }
            if (bitIntCols.has(i) && v !== null && v !== undefined) {
              if (Buffer.isBuffer(v)) {
                v = decodeMysqlBitBuffer(v, bitIntCols.get(i) ?? 0);
              }
            }
            if (geometryCols.has(i) && Buffer.isBuffer(v)) {
              const decoded = normalizeMysqlQuerySpatialValue(v);
              if (decoded !== null) {
                v = decoded;
              }
            }
            if (geometryCols.has(i) && !Buffer.isBuffer(v)) {
              const decoded = normalizeMysqlQuerySpatialValue(v);
              if (decoded !== null) {
                v = decoded;
              }
            }
            if (floatCols.has(i) && typeof v === "number") {
              v = Number.isFinite(v) ? v : null;
            }
            return [`__col_${i}`, v];
          }),
        ),
      );
      return {
        columns,
        columnMeta,
        rows,
        rowCount: rows.length,
        executionTimeMs,
      };
    }
    const affectedRows = rawRows.affectedRows;
    return {
      columns: [],
      rows: [],
      rowCount: affectedRows ?? 0,
      executionTimeMs,
      affectedRows,
    };
  }

  private _parseQueryResponse(
    rawRows: MysqlQueryRows,
    fields: MysqlQueryFields,
    executionTimeMs: number,
  ): QueryResult {
    const resultGroups = splitMysqlQueryResults(rawRows, fields);
    if (
      resultGroups.length === 1 &&
      (resultGroups[0].fields?.length ?? 0) <= MYSQL_MAX_RETAINED_RESULT_COLUMNS
    ) {
      const result = resultGroups[0];
      return this._parseQueryResult(
        result.rows,
        result.fields ?? [],
        executionTimeMs,
      );
    }

    const resultSets: QueryResultSet[] = [];
    let retainedColumnCount = 0;
    let totalAffected = 0;
    let resultSetsTruncated = false;
    for (const group of resultGroups) {
      if (isMysqlResultSetHeader(group.rows)) {
        totalAffected += group.rows.affectedRows ?? 0;
        continue;
      }
      if (
        resultSets.length >= MYSQL_MAX_RETAINED_RESULT_SETS ||
        retainedColumnCount + (group.fields?.length ?? 0) >
          MYSQL_MAX_RETAINED_RESULT_COLUMNS
      ) {
        resultSetsTruncated = true;
        continue;
      }
      const parsed = this._parseQueryResult(group.rows, group.fields ?? [], 0);
      if (parsed.columns.length === 0) {
        totalAffected += parsed.affectedRows ?? parsed.rowCount ?? 0;
      } else {
        resultSets.push({
          id: `result-${resultSets.length + 1}`,
          columns: parsed.columns,
          columnMeta: parsed.columnMeta,
          rows: parsed.rows,
          rowCount: parsed.rowCount,
          truncated: false,
        });
        retainedColumnCount += parsed.columns.length;
      }
    }

    const firstResultSet = resultSets[0];
    return {
      columns: firstResultSet?.columns ?? [],
      columnMeta: firstResultSet?.columnMeta ?? [],
      rows: firstResultSet?.rows ?? [],
      rowCount:
        resultSets.length > 0
          ? resultSets.reduce(
              (total, resultSet) => total + resultSet.rowCount,
              0,
            )
          : totalAffected,
      executionTimeMs,
      ...(resultSets.length > 0 ? { resultSets } : {}),
      ...(resultSetsTruncated ? { resultSetsTruncated: true } : {}),
      ...(resultSets.length === 0 && totalAffected > 0
        ? { affectedRows: totalAffected }
        : {}),
    };
  }
  private async _executeScript(
    conn: PoolConnection,
    stmts: string[],
    start: number,
  ): Promise<QueryResult> {
    const resultSets: QueryResultSet[] = [];
    let retainedColumnCount = 0;
    let resultSetsTruncated = false;
    let totalAffected = 0;
    const createResult = (error?: unknown): QueryResult => {
      const firstResultSet = resultSets[0];
      return {
        columns: firstResultSet?.columns ?? [],
        columnMeta: firstResultSet?.columnMeta ?? [],
        rows: firstResultSet?.rows ?? [],
        rowCount:
          resultSets.length > 0
            ? resultSets.reduce(
                (total, resultSet) => total + resultSet.rowCount,
                0,
              )
            : totalAffected,
        executionTimeMs: Date.now() - start,
        ...(resultSets.length > 0 ? { resultSets } : {}),
        ...(resultSetsTruncated ? { resultSetsTruncated: true } : {}),
        ...(resultSets.length === 0 && totalAffected > 0
          ? { affectedRows: totalAffected }
          : {}),
        ...(error !== undefined
          ? { error: normalizeUnknownError(error, this.config).message }
          : {}),
      };
    };

    const operation = [...this.activeQueryOperations].find(
      (entry) => entry.connection === conn,
    );
    for (const stmt of stmts) {
      if (operation?.cancelled) throw new Error("SQL query cancelled.");
      try {
        const [rawRows, fields] = await this.queryArrayRows(conn, {
          sql: stmt,
        });
        for (const group of splitMysqlQueryResults(rawRows, fields)) {
          if (isMysqlResultSetHeader(group.rows)) {
            totalAffected += group.rows.affectedRows ?? 0;
            continue;
          }
          if (
            resultSets.length >= MYSQL_MAX_RETAINED_RESULT_SETS ||
            retainedColumnCount + (group.fields?.length ?? 0) >
              MYSQL_MAX_RETAINED_RESULT_COLUMNS
          ) {
            resultSetsTruncated = true;
            continue;
          }
          const result = this._parseQueryResult(
            group.rows,
            group.fields ?? [],
            0,
          );
          if (result.columns.length > 0) {
            resultSets.push({
              id: `result-${resultSets.length + 1}`,
              columns: result.columns,
              columnMeta: result.columnMeta,
              rows: result.rows,
              rowCount: result.rowCount,
              truncated: false,
            });
            retainedColumnCount += result.columns.length;
          } else {
            totalAffected += result.affectedRows ?? result.rowCount ?? 0;
          }
        }
      } catch (error) {
        if (
          isMysqlServerQueryError(error) &&
          (resultSets.length > 0 || resultSetsTruncated)
        ) {
          return createResult(error);
        }
        throw error;
      }
    }
    return createResult();
  }
  private async queryIndexStatistics(
    database: string,
    table: string,
  ): Promise<
    (RowDataPacket & {
      INDEX_NAME: string;
      COLUMN_NAME: string | null;
      NON_UNIQUE: number | string;
      SUB_PART?: number | string | null;
      INDEX_TYPE?: string | null;
      COLLATION?: string | null;
      EXPRESSION?: string | null;
    })[]
  > {
    const select = (withExpression: boolean) =>
      this.queryObjectRows<
        RowDataPacket & {
          INDEX_NAME: string;
          COLUMN_NAME: string | null;
          NON_UNIQUE: number | string;
        }
      >(
        `SELECT INDEX_NAME, COLUMN_NAME, NON_UNIQUE, SUB_PART, INDEX_TYPE, COLLATION${withExpression ? ", EXPRESSION" : ""}
         FROM information_schema.STATISTICS
         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
         ORDER BY INDEX_NAME, SEQ_IN_INDEX`,
        [database, table],
      );
    if (this.statisticsExpressionSupported === false) return select(false);
    try {
      const rows = await select(true);
      this.statisticsExpressionSupported = true;
      return rows;
    } catch (error) {
      const mysqlError = error as { code?: unknown; errno?: unknown };
      if (
        this.statisticsExpressionSupported ||
        (mysqlError?.code !== "ER_BAD_FIELD_ERROR" &&
          mysqlError?.errno !== 1054)
      ) {
        throw error;
      }
      this.statisticsExpressionSupported = false;
      return select(false);
    }
  }
  async getIndexes(
    database: string,
    _schema: string,
    table: string,
  ): Promise<import("./types").IndexMeta[]> {
    const rows = await this.queryIndexStatistics(database, table);
    const map = new Map<string, import("./types").IndexMeta>();
    for (const r of rows) {
      const name = r.INDEX_NAME as string;
      if (!map.has(name)) {
        map.set(name, {
          name,
          columns: [],
          unique: Number(r.NON_UNIQUE) === 0,
          primary: name === "PRIMARY",
        });
      }
      const column = r.COLUMN_NAME ?? r.EXPRESSION?.trim();
      const index = map.get(name);
      if (index && column) {
        index.columns.push(column);
      }
    }
    return [...map.values()];
  }
  override async getIndexDDL(
    database: string,
    schema: string,
    table: string,
    indexName: string,
  ): Promise<string> {
    const rows = (await this.queryIndexStatistics(database, table)).filter(
      (row) => row.INDEX_NAME === indexName,
    );
    const [first] = rows;
    if (!first) {
      throw new Error(`Index "${indexName}" not found`);
    }
    const indexType = String(first.INDEX_TYPE ?? "").toUpperCase();
    const fullTextOrSpatial =
      indexType === "FULLTEXT" || indexType === "SPATIAL";
    const keyParts = rows.map((row) => {
      const expression = row.EXPRESSION?.trim();
      const subPart =
        row.SUB_PART == null || fullTextOrSpatial ? 0 : Number(row.SUB_PART);
      const keyPart = row.COLUMN_NAME
        ? `${this.quoteIdentifier(row.COLUMN_NAME)}${subPart > 0 ? `(${subPart})` : ""}`
        : expression
          ? `(${expression})`
          : undefined;
      if (!keyPart) {
        throw new Error(`Index "${indexName}" has an unsupported key part`);
      }
      return row.COLLATION === "D" ? `${keyPart} DESC` : keyPart;
    });
    const qualifiedTableName = this.qualifiedTableName(database, schema, table);
    if (indexName === "PRIMARY") {
      return `ALTER TABLE ${qualifiedTableName} ADD PRIMARY KEY (${keyParts.join(", ")});`;
    }
    const kind = fullTextOrSpatial
      ? `${indexType} `
      : Number(first.NON_UNIQUE) === 0
        ? "UNIQUE "
        : "";
    const using = indexType === "HASH" ? " USING HASH" : "";
    return `CREATE ${kind}INDEX ${this.quoteIdentifier(indexName)}${using} ON ${qualifiedTableName} (${keyParts.join(", ")});`;
  }
  override async getConstraintDDL(
    database: string,
    schema: string,
    table: string,
    constraintName: string,
    constraintKind?: import("./types").TableConstraintMeta["kind"],
  ): Promise<string> {
    const constraint = (
      await this.getConstraints(database, schema, table)
    ).find(
      (entry) =>
        entry.name === constraintName &&
        (!constraintKind || entry.kind === constraintKind),
    );
    if (constraint?.kind === "primary_key" || constraint?.kind === "unique") {
      return this.getIndexDDL(database, schema, table, constraint.name);
    }
    return super.getConstraintDDL(
      database,
      schema,
      table,
      constraintName,
      constraintKind,
    );
  }
  async getForeignKeys(
    database: string,
    _schema: string,
    table: string,
  ): Promise<import("./types").ForeignKeyMeta[]> {
    const rows = await this.queryObjectRows<
      RowDataPacket & {
        CONSTRAINT_NAME: string;
        COLUMN_NAME: string;
        REFERENCED_TABLE_SCHEMA: string;
        REFERENCED_TABLE_NAME: string;
        REFERENCED_COLUMN_NAME: string;
      }
    >(
      `SELECT kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME,
              kcu.REFERENCED_TABLE_SCHEMA, kcu.REFERENCED_TABLE_NAME, kcu.REFERENCED_COLUMN_NAME
       FROM information_schema.KEY_COLUMN_USAGE kcu
       JOIN information_schema.TABLE_CONSTRAINTS tc
         ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
         AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
         AND tc.TABLE_NAME = kcu.TABLE_NAME
       WHERE kcu.TABLE_SCHEMA = ? AND kcu.TABLE_NAME = ?
         AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
         AND kcu.REFERENCED_TABLE_NAME IS NOT NULL`,
      [database, table],
    );
    return rows.map((r) => ({
      constraintName: r.CONSTRAINT_NAME,
      column: r.COLUMN_NAME,
      referencedSchema: r.REFERENCED_TABLE_SCHEMA,
      referencedTable: r.REFERENCED_TABLE_NAME,
      referencedColumn: r.REFERENCED_COLUMN_NAME,
    }));
  }
  async getConstraints(
    database: string,
    _schema: string,
    table: string,
  ): Promise<import("./types").TableConstraintMeta[]> {
    const rows = await this.queryObjectRows<
      RowDataPacket & {
        CONSTRAINT_NAME: string;
        CONSTRAINT_TYPE: string;
        COLUMN_NAME: string | null;
        REFERENCED_TABLE_SCHEMA: string | null;
        REFERENCED_TABLE_NAME: string | null;
        REFERENCED_COLUMN_NAME: string | null;
      }
    >(
      `SELECT tc.CONSTRAINT_NAME,
              tc.CONSTRAINT_TYPE,
              kcu.COLUMN_NAME,
              kcu.REFERENCED_TABLE_SCHEMA,
              kcu.REFERENCED_TABLE_NAME,
              kcu.REFERENCED_COLUMN_NAME
       FROM information_schema.TABLE_CONSTRAINTS tc
       LEFT JOIN information_schema.KEY_COLUMN_USAGE kcu
         ON kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
        AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
        AND kcu.TABLE_NAME = tc.TABLE_NAME
        AND kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
        AND tc.CONSTRAINT_TYPE <> 'CHECK'
        AND (kcu.REFERENCED_TABLE_NAME IS NOT NULL) = (tc.CONSTRAINT_TYPE = 'FOREIGN KEY')
       WHERE tc.TABLE_SCHEMA = ?
         AND tc.TABLE_NAME = ?
         AND tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY', 'CHECK')
       ORDER BY tc.CONSTRAINT_NAME, tc.CONSTRAINT_TYPE, kcu.ORDINAL_POSITION`,
      [database, table],
    );
    const checkClauses = new Map<string, string>();
    if (rows.some((row) => row.CONSTRAINT_TYPE === "CHECK")) {
      try {
        const checkRows = (
          await this.queryObjectRows<
            RowDataPacket & {
              CONSTRAINT_NAME: string;
              CHECK_CLAUSE: string | null;
              TABLE_NAME?: string | null;
            }
          >(
            `SELECT cc.*
             FROM information_schema.CHECK_CONSTRAINTS cc
             WHERE cc.CONSTRAINT_SCHEMA = ?
               AND cc.CONSTRAINT_NAME IN (
                 SELECT tc.CONSTRAINT_NAME
                 FROM information_schema.TABLE_CONSTRAINTS tc
                 WHERE tc.TABLE_SCHEMA = ?
                   AND tc.TABLE_NAME = ?
                   AND tc.CONSTRAINT_TYPE = 'CHECK'
               )`,
            [database, database, table],
          )
        ).filter((row) => row.TABLE_NAME == null || row.TABLE_NAME === table);
        for (const row of checkRows) {
          if (
            typeof row.CHECK_CLAUSE === "string" &&
            checkRows.filter(
              (other) => other.CONSTRAINT_NAME === row.CONSTRAINT_NAME,
            ).length === 1
          ) {
            checkClauses.set(row.CONSTRAINT_NAME, row.CHECK_CLAUSE);
          }
        }
      } catch (error) {
        if (!isUnavailableCheckMetadataError(error)) throw error;
      }
    }
    const map = new Map<string, import("./types").TableConstraintMeta>();
    for (const row of rows) {
      const name = row.CONSTRAINT_NAME;
      const key = `${row.CONSTRAINT_TYPE}\0${name}`;
      const existing = map.get(key);
      if (existing) {
        if (row.COLUMN_NAME) {
          existing.columns.push(row.COLUMN_NAME);
        }
        if (row.REFERENCED_COLUMN_NAME) {
          existing.referencedColumns = [
            ...(existing.referencedColumns ?? []),
            row.REFERENCED_COLUMN_NAME,
          ];
        }
        continue;
      }

      map.set(key, {
        name,
        kind:
          row.CONSTRAINT_TYPE === "PRIMARY KEY"
            ? "primary_key"
            : row.CONSTRAINT_TYPE === "UNIQUE"
              ? "unique"
              : row.CONSTRAINT_TYPE === "FOREIGN KEY"
                ? "foreign_key"
                : "check",
        columns: row.COLUMN_NAME ? [row.COLUMN_NAME] : [],
        referencedSchema: row.REFERENCED_TABLE_SCHEMA ?? undefined,
        referencedTable: row.REFERENCED_TABLE_NAME ?? undefined,
        referencedColumns: row.REFERENCED_COLUMN_NAME
          ? [row.REFERENCED_COLUMN_NAME]
          : undefined,
        checkExpression:
          row.CONSTRAINT_TYPE === "CHECK" ? checkClauses.get(name) : undefined,
        source: "catalog",
      });
    }
    return [...map.values()];
  }
  async getTriggers(
    database: string,
    _schema: string,
    table: string,
  ): Promise<import("./types").TriggerMeta[] | null> {
    const rows = await this.queryObjectRows<
      RowDataPacket & {
        TRIGGER_NAME: string;
        ACTION_TIMING: string;
        EVENT_MANIPULATION: string;
        ACTION_STATEMENT: string | null;
      }
    >(
      `SELECT TRIGGER_NAME, ACTION_TIMING, EVENT_MANIPULATION, ACTION_STATEMENT
       FROM information_schema.TRIGGERS
       WHERE EVENT_OBJECT_SCHEMA = ?
         AND EVENT_OBJECT_TABLE = ?
       ORDER BY TRIGGER_NAME`,
      [database, table],
    );
    return rows.map((row) => ({
      name: row.TRIGGER_NAME,
      timing:
        row.ACTION_TIMING === "BEFORE"
          ? "before"
          : row.ACTION_TIMING === "AFTER"
            ? "after"
            : "unknown",
      events: [
        row.EVENT_MANIPULATION === "INSERT"
          ? "insert"
          : row.EVENT_MANIPULATION === "UPDATE"
            ? "update"
            : row.EVENT_MANIPULATION === "DELETE"
              ? "delete"
              : "unknown",
      ],
      orientation: "row",
      enabled: true,
      definition: row.ACTION_STATEMENT ?? undefined,
    }));
  }
  override async getTriggerDDL(
    database: string,
    _schema: string,
    table: string,
    triggerName: string,
  ): Promise<string> {
    const rows = await this.queryObjectRows<
      RowDataPacket & {
        TRIGGER_NAME: string;
        ACTION_TIMING: string;
        EVENT_MANIPULATION: string;
        ACTION_ORIENTATION: string;
        ACTION_STATEMENT: string | null;
      }
    >(
      `SELECT TRIGGER_NAME, ACTION_TIMING, EVENT_MANIPULATION, ACTION_ORIENTATION, ACTION_STATEMENT
       FROM information_schema.TRIGGERS
       WHERE EVENT_OBJECT_SCHEMA = ?
         AND EVENT_OBJECT_TABLE = ?
         AND TRIGGER_NAME = ?
       LIMIT 1`,
      [database, table, triggerName],
    );
    const trigger = rows[0];
    if (!trigger) {
      throw new Error(`Trigger "${triggerName}" not found`);
    }

    const orientation =
      trigger.ACTION_ORIENTATION === "ROW" ? "FOR EACH ROW" : "";
    const body =
      trigger.ACTION_STATEMENT?.trim() ||
      "BEGIN\n  -- trigger body is not available\nEND";
    const ddl = [
      `CREATE TRIGGER ${this.quoteIdentifier(trigger.TRIGGER_NAME)}`,
      `${trigger.ACTION_TIMING.toUpperCase()} ${trigger.EVENT_MANIPULATION.toUpperCase()}`,
      `ON ${this.qualifiedTableName(database, "", table)}`,
      orientation,
      body,
    ]
      .filter((part) => part)
      .join("\n")
      .replace(/\n{3,}/g, "\n\n");
    return ddl.endsWith(";") ? ddl : `${ddl};`;
  }
  async getCreateTableDDL(
    database: string,
    _schema: string,
    table: string,
  ): Promise<string> {
    const rows = await this.queryObjectRows<
      RowDataPacket & {
        "Create Table"?: string;
        "Create View"?: string;
      }
    >(
      `SHOW CREATE TABLE \`${database.replace(/`/g, "``")}\`.\`${table.replace(/`/g, "``")}\``,
    );
    const firstRow = rows[0];
    return firstRow?.["Create Table"] ?? firstRow?.["Create View"] ?? "";
  }
  async getRoutineDefinition(
    database: string,
    _schema: string,
    name: string,
    kind: "function" | "procedure",
    _routineIdentity?: string,
  ): Promise<string> {
    const type = kind === "function" ? "FUNCTION" : "PROCEDURE";
    const db = database.replace(/`/g, "``");
    const nm = name.replace(/`/g, "``");
    const rows = await this.queryObjectRows<
      RowDataPacket & {
        "Create Function"?: string;
        "Create Procedure"?: string;
      }
    >(`SHOW CREATE ${type} \`${db}\`.\`${nm}\``);
    const row = rows[0];
    const key = type === "FUNCTION" ? "Create Function" : "Create Procedure";
    return row?.[key] ?? `-- Definition not available for ${name}`;
  }
  async getObjectDefinition(
    _database: string,
    _schema: string,
    _name: string,
    _kind: DdlOnlyDbObjectKind,
  ): Promise<string | null> {
    return null;
  }
  async runTransaction(
    operations: import("./types").TransactionOperation[],
    context?: import("./types").TransactionContext,
    scope?: import("./types").TransactionOptions,
  ): Promise<void> {
    throwIfTransactionCancelled(context);
    const conn = await this.requirePool().getConnection();
    this.activeTransactionConnections.add(conn);
    let discardTransactionConnection = false;
    let commitAttempted = false;
    const cancel = () => {
      if (this.activeTransactionConnections.delete(conn)) {
        this.interruptAndDestroyConnection(conn);
      }
    };
    context?.signal.addEventListener("abort", cancel, { once: true });
    try {
      throwIfTransactionCancelled(context);
      if (
        operations.some(
          (op) =>
            op.captureIdentity?.mysqlKeyGuard ||
            op.captureIdentity?.mysqlInsertId?.zero,
        )
      ) {
        await conn.query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
        throwIfTransactionCancelled(context);
      }
      await conn.beginTransaction();
      await this.assertUnambiguousTimestampWrites(conn, operations, context);
      const identities = new TransactionIdentityStore();
      const generatedZeros = new Set<number>();
      for (const [index, op] of operations.entries()) {
        throwIfTransactionCancelled(context);
        const capture = op.captureIdentity?.mysqlInsertId;
        let literalZero = false;
        if (capture?.zero) {
          const modes = await this.readSessionSqlModes(conn);
          throwIfTransactionCancelled(context);
          if (!modes)
            throw new Error(
              "INSERT verification cannot determine the transaction session SQL mode; the transaction was not committed.",
            );
          literalZero = modes.includes("NO_AUTO_VALUE_ON_ZERO");
          if (!literalZero) generatedZeros.add(index);
        }
        const guard =
          literalZero && capture?.zero
            ? { sql: capture.zero.guardSql, params: [0] }
            : op.captureIdentity?.mysqlKeyGuard;
        if (guard) {
          const [existing] = await conn.query<RowDataPacket[]>(
            this.createQueryOptions(
              guard.sql,
              guard.params as QueryOptions["values"],
            ),
          );
          throwIfTransactionCancelled(context);
          if (existing.length)
            throw new Error(
              "INSERT verification cannot identify a new row at an already existing primary key; the transaction was not committed.",
            );
        }
        const [rows] = await conn.query<ResultSetHeader>(
          this.createQueryOptions(op.sql, op.params as QueryOptions["values"]),
        );
        throwIfTransactionCancelled(context);
        assertTransactionAffectedRows(op, rows.affectedRows);
        let returnedKeys: Record<string, unknown>[] = [];
        if (capture) {
          const id: unknown = rows.insertId;
          const reliable =
            (typeof id === "number" &&
              Number.isSafeInteger(id) &&
              (id !== 0 || literalZero)) ||
            (typeof id === "string" &&
              /^-?\d+$/.test(id) &&
              (BigInt(id) !== 0n || literalZero));
          if (reliable) {
            const [actualKeys] = await conn.query<RowDataPacket[]>(
              this.createQueryOptions(capture.sql, [id]),
            );
            throwIfTransactionCancelled(context);
            returnedKeys = actualKeys;
          }
        }
        await identities.capture(
          index,
          op,
          returnedKeys,
          async (sql, params) => {
            const [identityRows] = await conn.query<RowDataPacket[]>(
              this.createQueryOptions(sql, params as QueryOptions["values"]),
            );
            return identityRows;
          },
        );
      }
      await verifyTransaction(
        this,
        identities.resolve(scope?.verifications)?.map((verification) => {
          const identity = verification.identity;
          if (!identity || !generatedZeros.has(identity.operationIndex))
            return verification;
          const zero =
            operations[identity.operationIndex].captureIdentity?.mysqlInsertId
              ?.zero;
          if (!zero) return verification;
          return {
            ...verification,
            values: verification.values.map((value) =>
              value.column.name === zero.columnName
                ? {
                    ...value,
                    expectedValue:
                      verification.params[
                        identity.parameterIndexes[zero.keyIndex]
                      ],
                  }
                : value,
            ),
          };
        }),
        async (verification) => {
          const [rows] = await conn.query<RowDataPacket[]>(
            this.createQueryOptions(
              verification.sql,
              verification.params as QueryOptions["values"],
            ),
          );
          return rows;
        },
        context,
      );
      throwIfTransactionCancelled(context);
      commitAttempted = true;
      await conn.commit();
    } catch (error) {
      let rollbackResult: ResultSetHeader;
      try {
        [rollbackResult] = await conn.query<ResultSetHeader>(
          this.createQueryOptions("ROLLBACK"),
        );
      } catch (rollbackError) {
        discardTransactionConnection = true;
        throw new AggregateError(
          [error, rollbackError],
          "MySQL transaction and rollback failed; the connection was discarded.",
        );
      }
      if (rollbackResult?.warningStatus !== 0) {
        discardTransactionConnection = true;
        const detail = error instanceof Error ? error.message : String(error);
        throw new AggregateError(
          [error],
          `MySQL transaction failed; a complete rollback could not be confirmed${
            typeof rollbackResult?.warningStatus === "number"
              ? ` (ROLLBACK reported ${rollbackResult.warningStatus} warning(s))`
              : " (ROLLBACK warning metadata was unavailable)"
          }. Some changes may persist; the connection was discarded. ${detail}`,
        );
      }
      if (!commitAttempted && error instanceof TransactionVerificationError) {
        error.rollbackConfirmed = true;
      }
      if (
        !commitAttempted &&
        !mayMysqlServerHaveRolledBackTransaction(error) &&
        !(error instanceof TransactionVerificationError)
      ) {
        const message = error instanceof Error ? error.message : String(error);
        throw new MutationNotExecutedError(message, error);
      }
      throw error;
    } finally {
      context?.signal.removeEventListener("abort", cancel);
      if (this.activeTransactionConnections.delete(conn)) {
        if (discardTransactionConnection) conn.destroy();
        else conn.release();
      }
    }
  }
  private async assertUnambiguousTimestampWrites(
    conn: PoolConnection,
    operations: import("./types").TransactionOperation[],
    context?: import("./types").TransactionContext,
  ): Promise<void> {
    const values = [
      ...new Set(operations.flatMap(mysqlUtcTimestampWriteValues)),
    ];
    if (values.length === 0) return;
    const [rows] = await conn.query<RowDataPacket[]>(
      this.createQueryOptions(
        `SELECT @@session.time_zone AS __rapidb_time_zone, ${values
          .map(
            (_, index) =>
              `UNIX_TIMESTAMP(${MYSQL_UTC_TIMESTAMP_OPERAND}) AS __rapidb_epoch_${index}`,
          )
          .join(", ")}`,
        values,
      ),
    );
    throwIfTransactionCancelled(context);
    const row = rows[0];
    const timeZone = String(row?.__rapidb_time_zone ?? "unknown");
    for (const [index, value] of values.entries()) {
      const expected = mysqlTimestampEpoch(`${value}+00:00`);
      const actual = canonicalizeExactNumeric(
        row?.[`__rapidb_epoch_${index}`],
        null,
      )?.canonical;
      if (expected !== null && actual === expected) continue;
      throw new Error(
        expected !== null &&
          actual !== undefined &&
          actual !== "0" &&
          !actual.startsWith("-")
          ? `[RapiDB] TIMESTAMP value ${value} UTC falls into a repeated local hour of the session time zone ${timeZone} (for example when daylight saving time ends), so MySQL cannot store that exact instant. Nothing was written.`
          : `[RapiDB] TIMESTAMP value ${value} UTC is outside the range MySQL can store in the session time zone ${timeZone}. Nothing was written.`,
      );
    }
  }
  private async assertUnambiguousQueryTimestampWrites(
    conn: PoolConnection,
    statements: string[],
    params: unknown[] | undefined,
  ): Promise<void> {
    try {
      await this.assertUnambiguousTimestampWrites(
        conn,
        statements.map((sql) => ({ sql, params })),
      );
    } catch (error) {
      throw new MutationNotExecutedError(
        error instanceof Error ? error.message : String(error),
        error,
      );
    }
  }
  override async getMutationAtomicityRisk(
    database: string,
    _schema: string,
    table: string,
  ): Promise<string | null> {
    const rows = await this.queryObjectRows<
      RowDataPacket & {
        ENGINE: string | null;
        TRANSACTIONS: string | null;
      }
    >(
      `SELECT t.ENGINE, e.TRANSACTIONS
       FROM information_schema.TABLES t
       LEFT JOIN information_schema.ENGINES e ON e.ENGINE = t.ENGINE
       WHERE t.TABLE_SCHEMA = ? AND t.TABLE_NAME = ?
       LIMIT 1`,
      [database, table],
    );
    const engine =
      typeof rows[0]?.ENGINE === "string"
        ? rows[0].ENGINE.trim().toUpperCase()
        : "";
    const transactions =
      typeof rows[0]?.TRANSACTIONS === "string"
        ? rows[0].TRANSACTIONS.trim().toUpperCase()
        : "";
    if (engine && transactions === "YES") {
      return null;
    }
    const reason =
      engine && transactions === "NO"
        ? `Table \`${database}\`.\`${table}\` uses the non-transactional MySQL engine ${engine}.`
        : `Cannot confirm rollback support for table \`${database}\`.\`${table}\`${engine ? ` (engine ${engine})` : ""}: storage engine transaction metadata is unavailable or inconclusive.`;
    return `${reason} Multi-statement table mutations and mutations requiring verification are blocked to avoid partial writes. Use a transactional engine such as InnoDB and ensure its engine metadata is visible.`;
  }
  mapTypeCategory(nativeType: string): TypeCategory {
    const ct = nativeType.toLowerCase();
    const base = mysqlTypeName(nativeType);
    if (base === "bool" || base === "boolean") return "boolean";
    if (
      ["tinyint", "smallint", "mediumint", "int", "integer", "bigint"].includes(
        base,
      )
    )
      return "integer";
    if (base === "bit") return "integer";
    if (base === "year") return "integer";
    if (base === "float" || base === "double" || base === "real")
      return "float";
    if (base === "decimal" || base === "numeric") return "decimal";
    if (base === "date") return "date";
    if (base === "time") return "time";
    if (base === "datetime" || base === "timestamp") return "datetime";
    if (
      [
        "binary",
        "varbinary",
        "tinyblob",
        "blob",
        "mediumblob",
        "longblob",
      ].includes(base)
    )
      return "binary";
    if (base === "json") return "json";
    if (MYSQL_SPATIAL_TYPES.has(base)) return "spatial";
    if (ct.startsWith("enum") || ct.startsWith("set")) return "enum";
    if (
      [
        "char",
        "varchar",
        "tinytext",
        "text",
        "mediumtext",
        "longtext",
      ].includes(base)
    )
      return "text";
    return "other";
  }
  protected override formatPreviewSqlLiteral(value: unknown): string {
    if (typeof value === "string" && /[\\\p{Cc}]/u.test(value)) {
      return formatMysqlStringLiteral(value);
    }
    return super.formatPreviewSqlLiteral(value);
  }
  protected getValueSemantics(
    nativeType: string,
    _category: TypeCategory,
  ): ValueSemantics {
    const base = mysqlTypeName(nativeType);
    if (base === "bool" || base === "boolean") return "boolean";
    if (base === "bit") return "bit";
    return "plain";
  }
  isDatetimeWithTime(nativeType: string): boolean {
    const base = mysqlTypeName(nativeType);
    return base === "datetime" || base === "timestamp";
  }
  protected override isFilterable(
    nativeType: string,
    category: TypeCategory,
  ): boolean {
    if (category === "spatial") {
      return true;
    }
    return super.isFilterable(nativeType, category);
  }
  override quoteIdentifier(name: string): string {
    return `\`${name.replace(/`/g, "``")}\``;
  }
  override qualifiedTableName(
    database: string,
    _schema: string,
    table: string,
  ): string {
    return database
      ? `${this.quoteIdentifier(database)}.${this.quoteIdentifier(table)}`
      : this.quoteIdentifier(table);
  }
  protected override qualifiedReferencedTableName(
    _database: string,
    schema: string | undefined,
    table: string,
  ): string {
    return this.qualifiedTableName(schema ?? "", "", table);
  }
  override buildInsertDefaultValuesSql(
    qualifiedTableName: string,
    _columns?: readonly ColumnTypeMeta[],
  ): string {
    return `INSERT INTO ${qualifiedTableName} () VALUES ()`;
  }
  override buildInsertValueExpr(
    column: ColumnTypeMeta,
    _paramIndex: number,
  ): string {
    if (this.hasBitSemantics(column)) {
      return "CAST(? AS UNSIGNED)";
    }
    if (column.category === "spatial") {
      return "ST_GeomFromText(?)";
    }
    if (column.category === "decimal") {
      const { precision, scale } = this.parseExactNumericConstraint(
        column.nativeType,
      );
      const p = precision ?? 10;
      const s = scale ?? 0;
      if (p >= 1 && p <= 65 && s >= 0 && s <= 38 && s <= p) {
        return `CAST(? AS DECIMAL(${p},${s}))`;
      }
    }
    return "?";
  }
  override buildSetExpr(column: ColumnTypeMeta, _paramIndex: number): string {
    if (column.category === "decimal") {
      return `${this.quoteIdentifier(column.name)} = ?`;
    }
    return `${this.quoteIdentifier(column.name)} = ${this.buildInsertValueExpr(column, _paramIndex)}`;
  }
  buildMutationValueExpr(
    column: ColumnTypeMeta,
    value: unknown,
    operation: "insert" | "update",
  ): { sql: string; params: unknown[] } | null {
    if (column.category === "decimal") return { sql: "?", params: [value] };
    if (mysqlTypeName(column.nativeType) === "timestamp") {
      const utcValue = mysqlUtcTimestampValue(value);
      return utcValue === null
        ? null
        : { sql: MYSQL_UTC_TIMESTAMP_OPERAND, params: [utcValue] };
    }
    if (column.category !== "spatial") return null;
    if (value === null) return { sql: "NULL", params: [] };
    if (typeof value !== "string") {
      throw new TypeError(`Column "${column.name}" expects spatial WKT text.`);
    }
    const { wkt, srid } = parseMysqlSpatialSridPrefix(value);
    if (srid !== null) {
      return { sql: "ST_GeomFromText(?, ?)", params: [wkt, srid] };
    }
    return {
      sql:
        operation === "update"
          ? `ST_GeomFromText(?, COALESCE(ST_SRID(${this.quoteIdentifier(column.name)}), 0))`
          : "ST_GeomFromText(?)",
      params: [wkt],
    };
  }
  buildOriginalValueComparisonWithParams(
    column: ColumnTypeMeta,
    value: unknown,
  ): { sql: string; params: unknown[] } | null {
    if (
      mysqlTypeName(column.nativeType) === "timestamp" &&
      typeof value === "string" &&
      hasMysqlTimezoneOffset(value) &&
      normalizeMysqlDatetimeInput(value) !== null
    ) {
      const name = this.quoteIdentifier(column.name);
      const epoch = mysqlTimestampEpochParam("[RapiDB]", column.name, value);
      const exact = `UNIX_TIMESTAMP(${name}) = ${MYSQL_TIMESTAMP_EPOCH_OPERAND}`;
      const seconds = BigInt(epoch.split(".")[0]);
      if (
        seconds <= MYSQL_TIMESTAMP_PREFILTER_SECONDS ||
        seconds + MYSQL_TIMESTAMP_PREFILTER_SECONDS >=
          MYSQL_FROM_UNIXTIME_SAFE_MAX
      ) {
        return { sql: exact, params: [epoch] };
      }
      return {
        sql: `${name} BETWEEN FROM_UNIXTIME(?) AND FROM_UNIXTIME(?) AND ${exact}`,
        params: [
          Number(seconds - MYSQL_TIMESTAMP_PREFILTER_SECONDS),
          Number(seconds + MYSQL_TIMESTAMP_PREFILTER_SECONDS + 1n),
          epoch,
        ],
      };
    }
    if (column.category !== "spatial") return null;
    if (typeof value !== "string") {
      throw new TypeError(
        `Refresh spatial column "${column.name}" before editing.`,
      );
    }
    const { wkt, srid } = parseMysqlSpatialSridPrefix(value);
    const name = this.quoteIdentifier(column.name);
    return {
      sql: `ST_SRID(${name}) = ? AND ST_AsBinary(${name}) = ST_AsBinary(ST_GeomFromText(?, ?))`,
      params: [srid ?? 0, wkt, srid ?? 0],
    };
  }
  override buildOriginalValueComparison(
    column: ColumnTypeMeta,
    paramIndex: number,
  ): string {
    const name = this.quoteIdentifier(column.name);
    if (column.category === "json") return `${name} = CAST(? AS JSON)`;
    if (column.category === "spatial")
      return `ST_AsBinary(${name}) = ST_AsBinary(ST_GeomFromText(?))`;
    if (column.category === "text") {
      const characterSet = column.characterSet?.trim();
      return characterSet && /^[a-z0-9_]+$/i.test(characterSet)
        ? `CAST(${name} AS BINARY) = CAST(CONVERT(? USING ${characterSet}) AS BINARY)`
        : `CAST(${name} AS BINARY) = CAST(? AS BINARY)`;
    }
    if (
      column.category === "float" &&
      mysqlTypeName(column.nativeType) === "float"
    ) {
      return `${name} = CAST(? AS FLOAT)`;
    }
    return super.buildOriginalValueComparison(column, paramIndex);
  }
  coerceOriginalValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (
      typeof value === "string" &&
      value !== NULL_SENTINEL &&
      ["date", "datetime", "timestamp"].includes(
        mysqlTypeName(column.nativeType),
      )
    ) {
      return value;
    }
    return this.coerceInputValue(value, column);
  }
  protected override coerceBooleanTrue(): unknown {
    return 1;
  }
  protected override coerceBooleanFalse(): unknown {
    return 0;
  }
  override coerceInputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (value === null || value === undefined || value === "") return value;
    if (value === NULL_SENTINEL) return null;
    if (column.category === "json" && typeof value === "object") {
      return JSON.stringify(value);
    }
    if (typeof value !== "string") return value;
    if (this.hasBooleanSemantics(column)) {
      const normalized = this.parseBooleanInput(value);
      if (normalized !== null) {
        return normalized ? 1 : 0;
      }
    }
    if (this.hasBitSemantics(column)) {
      const parsed = parseMysqlBitValue(value, column.nativeType);
      if (!parsed) {
        throw new Error(
          `[RapiDB] ${mysqlBitValidationMessage(column.name, column.nativeType)}`,
        );
      }
      return parsed.parameter;
    }
    if (column.category === "spatial") {
      const normalized = normalizeMysqlSpatialInput(value, column.nativeType);
      if (normalized === null) {
        throw new Error(`[RapiDB] Column ${column.name} expects WKT text.`);
      }
      return normalized;
    }
    if (column.category === "binary")
      return super.coerceInputValue(value, column);
    const typeName = mysqlTypeName(column.nativeType);
    if (typeName === "date") {
      const normalized = normalizeMysqlDateInput(value);
      if (normalized !== null) {
        return normalized;
      }
      if (looksLikeMysqlDateInput(value)) {
        throw invalidMysqlTemporalInputError(column.name, typeName);
      }
      return value;
    }
    if (typeName === "datetime" || typeName === "timestamp") {
      const normalized = normalizeMysqlDatetimeInput(value);
      if (normalized !== null) {
        return typeName === "timestamp" && hasMysqlTimezoneOffset(value)
          ? `${normalized}+00:00`
          : normalized;
      }
      if (looksLikeMysqlDatetimeInput(value)) {
        throw invalidMysqlTemporalInputError(column.name, typeName);
      }
      return value;
    }
    return value;
  }
  override formatOutputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (value === null || value === undefined) return value;
    if (Buffer.isBuffer(value)) return super.formatOutputValue(value, column);
    if (this.hasBooleanSemantics(column)) {
      if (typeof value === "boolean") {
        return value ? 1 : 0;
      }
      if (typeof value === "string") {
        const normalized = this.parseBooleanInput(value);
        if (normalized !== null) {
          return normalized ? 1 : 0;
        }
      }
      if (typeof value === "number") {
        return value;
      }
    }
    if (column.category === "spatial") {
      if (typeof value === "string") {
        const normalized = normalizeMysqlSpatialInput(value, column.nativeType);
        if (normalized !== null) {
          return normalized;
        }
      }
      const formattedSpatial = buildMysqlSpatialWkt(value, column.nativeType);
      if (formattedSpatial !== null) {
        return formattedSpatial;
      }
      return String(value);
    }
    if (typeof value === "bigint") return value.toString();
    if (this.isDatetimeWithTime(column.nativeType)) {
      if (typeof value === "string") {
        const normalized = normalizeMysqlDatetimeInput(value);
        if (normalized !== null) return normalized;
      }
      const formatted = formatDatetimeForDisplay(value);
      if (formatted !== null) return formatted;
    }
    return value;
  }
  override normalizeFilterValue(
    column: ColumnTypeMeta,
    operator: FilterOperator,
    value: string | [string, string] | undefined,
  ): string | [string, string] | undefined {
    if (!this.hasBooleanSemantics(column)) {
      return super.normalizeFilterValue(column, operator, value);
    }
    if (operator === "is_null" || operator === "is_not_null") {
      return undefined;
    }
    if (value === undefined) {
      return undefined;
    }
    if (operator === "between" && Array.isArray(value)) {
      const normalizedStart = value[0].trim();
      const normalizedEnd = value[1].trim();
      if (
        (normalizedStart !== "0" && normalizedStart !== "1") ||
        (normalizedEnd !== "0" && normalizedEnd !== "1")
      ) {
        throw new Error(
          `[RapiDB Filter] Column ${column.name} expects boolean filter values as 0 or 1.`,
        );
      }
      return [normalizedStart, normalizedEnd];
    }
    if (typeof value !== "string") {
      return value;
    }
    const normalized = value.trim();
    if (operator === "in") {
      const parts = normalized
        .split(",")
        .map((part) => part.trim())
        .filter(Boolean);
      if (
        parts.length === 0 ||
        parts.some((part) => part !== "0" && part !== "1")
      ) {
        throw new Error(
          `[RapiDB Filter] Column ${column.name} expects boolean filter values as 0 or 1.`,
        );
      }
      return parts.join(", ");
    }
    if (normalized !== "0" && normalized !== "1") {
      throw new Error(
        `[RapiDB Filter] Column ${column.name} expects boolean filter values as 0 or 1.`,
      );
    }
    return normalized;
  }
  override checkPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null {
    if (column.category === "json") {
      return this.checkJsonPersistedEdit(column, expectedValue, options, true);
    }
    if (
      column.category === "date" ||
      column.category === "time" ||
      column.category === "datetime"
    ) {
      return {
        ok: true,
        shouldVerify: false,
      };
    }
    if (this.hasBitSemantics(column)) {
      return this.checkNormalizedPersistedEdit(
        column,
        expectedValue,
        options,
        (value) =>
          canonicalizeMysqlBitPersistedEditValue(value, column.nativeType),
        mysqlBitValidationMessage(column.name, column.nativeType),
      );
    }
    if (column.category === "integer") {
      return this.checkExactNumericPersistedEdit(
        column,
        expectedValue,
        { precision: null, scale: 0 },
        options,
      );
    }
    const baseType = mysqlTypeName(column.nativeType);
    if (column.category === "decimal") {
      if (!["decimal", "numeric", "dec", "fixed"].includes(baseType)) {
        return null;
      }
      return this.checkExactNumericPersistedEdit(
        column,
        expectedValue,
        this.parseExactNumericConstraint(column.nativeType),
        options,
      );
    }
    if (
      column.category === "float" &&
      this.isApproximateNumericType(column.nativeType)
    ) {
      const significantDigits =
        baseType === "float" ? 7 : baseType === "double" ? 17 : 15;
      if (baseType === "double" && options === undefined) {
        const requested = canonicalizeExactNumeric(expectedValue, null);
        if (requested) {
          const parsed = Number(expectedValue);
          if (Number.isFinite(parsed)) {
            const roundTrip = canonicalizeExactNumeric(
              numberToDecimalString(parsed),
              null,
            );
            if (roundTrip && requested.canonical !== roundTrip.canonical) {
              return {
                ok: false,
                shouldVerify: false,
                message:
                  `Column "${column.name}" exceeds the reliable precision of DOUBLE ` +
                  `and would round to ${roundTrip.canonical}.`,
              };
            }
            return { ok: true, shouldVerify: true };
          }
        }
      }
      return this.checkApproximateNumericPersistedEdit(
        column,
        expectedValue,
        significantDigits,
        options,
      );
    }
    if (this.hasBooleanSemantics(column)) {
      return this.checkBooleanPersistedEdit(column, expectedValue, options);
    }
    if (column.category === "binary") {
      return this.checkBinaryPersistedEdit(column, expectedValue, options);
    }
    if (column.category === "enum" || column.category === "text") {
      if (baseType === "char") {
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
    _paramIndex: number,
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
    if (column.category === "array") {
      if (operator !== "like" && operator !== "ilike") {
        return null;
      }
      const arrayValue = typeof val === "string" ? val : val[0];
      return this.buildContainsFilter(col, literalContainsPattern(arrayValue));
    }
    if (
      column.category === "binary" &&
      typeof val === "string" &&
      (operator === "eq" || operator === "neq")
    ) {
      const sqlOp = operator === "neq" ? "!=" : "=";
      return {
        sql: `${col} ${sqlOp} ?`,
        params: [this.coerceInputValue(val, column)],
      };
    }
    if (
      this.hasBooleanSemantics(column) &&
      (operator === "eq" || operator === "neq")
    ) {
      const strVal = (typeof val === "string" ? val : val[0]).toLowerCase();
      if (strVal === "1" || strVal === "0") {
        const boolVal = strVal === "1" ? 1 : 0;
        const op = operator === "neq" ? "!=" : "=";
        return { sql: `${col} ${op} ?`, params: [boolVal] };
      }
      return null;
    }
    if (column.category === "spatial" && typeof val === "string") {
      if (operator !== "eq" && operator !== "neq") {
        return null;
      }
      const normalizedSpatial = normalizeMysqlSpatialInput(
        val,
        column.nativeType,
      );
      const searchValue = (normalizedSpatial ?? val).trim();
      if (!searchValue) {
        return null;
      }
      const parsedSpatial = parseMysqlSpatialSridPrefix(searchValue);
      if (parsedSpatial.srid !== null && Number.isFinite(parsedSpatial.srid)) {
        return {
          sql: `ST_Equals(${col}, ST_GeomFromText(?, ?)) ${operator === "neq" ? "= 0" : "= 1"}`,
          params: [parsedSpatial.wkt, parsedSpatial.srid],
        };
      }
      return {
        sql: `ST_Equals(${col}, ST_GeomFromText(?, ST_SRID(${col}))) ${operator === "neq" ? "= 0" : "= 1"}`,
        params: [searchValue],
      };
    }
    if (this.hasBitSemantics(column)) {
      const parseFilterValue = (raw: string): number | string => {
        const parsed = parseMysqlBitValue(raw, column.nativeType);
        if (!parsed) {
          throw new Error(
            `[RapiDB Filter] ${mysqlBitValidationMessage(column.name, column.nativeType)}`,
          );
        }
        return parsed.parameter;
      };
      if (typeof val === "string") {
        if (operator === "in") {
          const parts = val
            .split(",")
            .map((part) => part.trim())
            .filter(Boolean);
          if (parts.length === 0) {
            throw new Error(
              `[RapiDB Filter] ${mysqlBitValidationMessage(column.name, column.nativeType)}`,
            );
          }
          return {
            sql: `${col} IN (${parts.map(() => "?").join(", ")})`,
            params: parts.map(parseFilterValue),
          };
        }
        if (["eq", "neq", "gt", "gte", "lt", "lte"].includes(operator)) {
          const op = this.sqlOperator(operator);
          return { sql: `${col} ${op} ?`, params: [parseFilterValue(val)] };
        }
      }
      if (operator === "between" && Array.isArray(val)) {
        return {
          sql: `${col} BETWEEN ? AND ?`,
          params: [parseFilterValue(val[0]), parseFilterValue(val[1])],
        };
      }
    }
    if (column.category === "date") {
      if (operator === "between" && Array.isArray(val)) {
        return {
          sql: `${col} BETWEEN ? AND ?`,
          params: [
            normalizeMysqlDateInput(val[0]) ?? val[0],
            normalizeMysqlDateInput(val[1]) ?? val[1],
          ],
        };
      }
      if (typeof val === "string" && operator === "in") {
        const parts = val
          .split(",")
          .map((part) => part.trim())
          .filter(Boolean)
          .map((part) => normalizeMysqlDateInput(part) ?? part);
        return {
          sql: `${col} IN (${parts.map(() => "?").join(", ")})`,
          params: parts,
        };
      }
      if (
        typeof val === "string" &&
        ["eq", "neq", "gt", "gte", "lt", "lte"].includes(operator)
      ) {
        const sqlOp = operator === "neq" ? "!=" : this.sqlOperator(operator);
        return {
          sql: `${col} ${sqlOp} ?`,
          params: [normalizeMysqlDateInput(val) ?? val],
        };
      }
    }
    if (column.category === "time") {
      if (operator === "between" && Array.isArray(val)) {
        return {
          sql: `${col} BETWEEN CAST(? AS TIME(6)) AND CAST(? AS TIME(6))`,
          params: [val[0], val[1]],
        };
      }
      if (typeof val === "string" && operator === "in") {
        const parts = val
          .split(",")
          .map((part) => part.trim())
          .filter(Boolean);
        return {
          sql: `${col} IN (${parts.map(() => "CAST(? AS TIME(6))").join(", ")})`,
          params: parts,
        };
      }
      if (
        typeof val === "string" &&
        ["eq", "neq", "gt", "gte", "lt", "lte"].includes(operator)
      ) {
        const sqlOp = operator === "neq" ? "!=" : this.sqlOperator(operator);
        return {
          sql: `${col} ${sqlOp} CAST(? AS TIME(6))`,
          params: [val],
        };
      }
    }
    if (column.category === "datetime") {
      const epochCol = `UNIX_TIMESTAMP(${col})`;
      const operand = (raw: string) => {
        const normalized = normalizeMysqlDatetimeInput(raw);
        if (
          normalized === null ||
          mysqlTypeName(column.nativeType) !== "timestamp" ||
          !hasMysqlTimezoneOffset(raw)
        ) {
          return { target: col, sql: "?", value: normalized ?? raw };
        }
        const epoch = mysqlTimestampEpoch(raw);
        return {
          target: epochCol,
          sql: MYSQL_TIMESTAMP_EPOCH_OPERAND,
          value: epoch ?? "1",
          beforeFirstTimestamp: epoch === null,
        };
      };
      const compare = (
        part: ReturnType<typeof operand>,
        sqlOp: string,
      ): { sql: string; params: unknown[] } => {
        if (!part.beforeFirstTimestamp) {
          return {
            sql: `${part.target} ${sqlOp} ${part.sql}`,
            params: [part.value],
          };
        }
        if (sqlOp === ">" || sqlOp === ">=") {
          return { sql: `${part.target} >= ${part.sql}`, params: ["1"] };
        }
        if (sqlOp === "!=") return { sql: `${col} IS NOT NULL`, params: [] };
        return { sql: "1 = 0", params: [] };
      };
      if (operator === "between" && Array.isArray(val)) {
        const [low, high] = val.map(operand);
        if (low.beforeFirstTimestamp || high.beforeFirstTimestamp) {
          const bounds = [compare(low, ">="), compare(high, "<=")];
          return {
            sql: `(${bounds.map((bound) => bound.sql).join(" AND ")})`,
            params: bounds.flatMap((bound) => bound.params),
          };
        }
        return {
          sql:
            low.target === high.target
              ? `${low.target} BETWEEN ${low.sql} AND ${high.sql}`
              : `(${low.target} >= ${low.sql} AND ${high.target} <= ${high.sql})`,
          params: [low.value, high.value],
        };
      }
      if (typeof val === "string" && operator === "in") {
        const allOperands = val
          .split(",")
          .map((part) => part.trim())
          .filter(Boolean)
          .map(operand);
        const operands = allOperands.filter(
          (part) => !part.beforeFirstTimestamp,
        );
        if (operands.length === 0 && allOperands.length > 0) {
          return { sql: "1 = 0", params: [] };
        }
        const groups = [col, epochCol]
          .map((target) => operands.filter((part) => part.target === target))
          .filter((group) => group.length > 0);
        if (groups.length < 2) {
          return {
            sql: `${groups[0]?.[0]?.target ?? col} IN (${operands.map((part) => part.sql).join(", ")})`,
            params: operands.map((part) => part.value),
          };
        }
        return {
          sql: `(${groups.map((group) => `${group[0].target} IN (${group.map((part) => part.sql).join(", ")})`).join(" OR ")})`,
          params: groups.flat().map((part) => part.value),
        };
      }
      if (
        typeof val === "string" &&
        ["eq", "neq", "gt", "gte", "lt", "lte"].includes(operator)
      ) {
        const sqlOp = operator === "neq" ? "!=" : this.sqlOperator(operator);
        return compare(operand(val), sqlOp);
      }
    }
    if (
      this.isNumericCategory(column.category) &&
      this.isApproximateNumericType(column.nativeType) &&
      typeof val === "string" &&
      Number.isFinite(Number(val)) &&
      val !== "" &&
      (operator === "eq" || operator === "neq")
    ) {
      const numericValue = Number(val);
      const tolerance = this.approximateNumericTolerance(val);
      const comparisonTolerance = `GREATEST(?, ABS(?) * ?)`;
      if (operator === "neq") {
        return {
          sql: `ABS(${col} - ?) >= ${comparisonTolerance}`,
          params: [numericValue, tolerance, numericValue, tolerance],
        };
      }
      return {
        sql: `ABS(${col} - ?) < ${comparisonTolerance}`,
        params: [numericValue, tolerance, numericValue, tolerance],
      };
    }
    if (
      this.isNumericCategory(column.category) &&
      typeof val === "string" &&
      !Number.isNaN(Number(val)) &&
      val !== ""
    ) {
      const sqlOp = this.sqlOperator(operator);
      if (
        column.category === "decimal" ||
        (column.category === "integer" && !/^[+-]?\d+$/.test(val))
      ) {
        const operand = this.exactNumericFilterOperand(column, val);
        return {
          sql: `${col} ${sqlOp} ${operand.sql}`,
          params: [operand.value],
        };
      }
      const param =
        column.category === "integer"
          ? toMysqlIntegerFilterParam(val)
          : Number(val);
      return { sql: `${col} ${sqlOp} ?`, params: [param] };
    }
    if (operator === "between" && Array.isArray(val)) {
      if (column.category === "integer" || column.category === "decimal") {
        const operands = val.map((part) =>
          this.exactNumericFilterOperand(column, part),
        );
        return {
          sql: `${col} BETWEEN ${operands[0].sql} AND ${operands[1].sql}`,
          params: operands.map((part) => part.value),
        };
      }
      return { sql: `${col} BETWEEN ? AND ?`, params: [val[0], val[1]] };
    }
    if (operator === "in" && typeof val === "string") {
      const parts = val.split(",").map((s) => s.trim());
      if (column.category === "integer" || column.category === "decimal") {
        const operands = parts.map((part) =>
          this.exactNumericFilterOperand(column, part),
        );
        return {
          sql: `${col} IN (${operands.map((part) => part.sql).join(", ")})`,
          params: operands.map((part) => part.value),
        };
      }
      return {
        sql: `${col} IN (${parts.map(() => "?").join(", ")})`,
        params: parts,
      };
    }
    if (operator !== "like" && operator !== "ilike") {
      throw new Error(
        `[RapiDB Filter] Column ${column.name} does not support ${operator} filters for ${column.category} values.`,
      );
    }
    const v = typeof val === "string" ? val : val[0];
    if (!["date", "time", "datetime"].includes(column.category)) {
      return this.buildContainsFilter(
        col,
        literalContainsPattern(v),
        column.category === "text" || column.category === "enum",
        column,
      );
    }
    let finalVal = v;
    if (ISO_DATETIME_RE.test(v)) {
      finalVal = v
        .replace(/(\.\d*?[1-9])0+(?=[Z+-]|$)/, "$1")
        .replace(/\.0+(?=[Z+-]|$)/, "")
        .replace("T", "%")
        .replace("Z", "%")
        .replace(/[+-]\d{2}:\d{2}$/, "%");
    }
    const mysqlVal = DATETIME_SQL_RE.test(finalVal)
      ? `${finalVal}%`
      : ISO_DATETIME_RE.test(v)
        ? `%${finalVal}%`
        : literalContainsPattern(finalVal);
    return this.buildContainsFilter(col, mysqlVal);
  }
  private exactNumericFilterOperand(
    column: ColumnTypeMeta,
    value: string,
  ): { sql: string; value: string } {
    if (column.category === "integer" && /^[+-]?\d+$/.test(value)) {
      return { sql: "?", value };
    }
    const columnScale =
      column.category === "decimal"
        ? (this.parseExactNumericConstraint(column.nativeType).scale ?? 0)
        : 0;
    const literal = exactNumericFilterLiteral(
      value,
      column.name,
      65,
      Math.min(38, Math.max(30, columnScale)),
    );
    return {
      sql: `CAST(? AS DECIMAL(65,${literal.scale}))`,
      value: literal.value,
    };
  }
  private buildContainsFilter(
    col: string,
    pattern: string,
    preserveColumnCollation = false,
    column?: ColumnTypeMeta,
  ): FilterConditionResult {
    const leftExpression =
      column?.category === "spatial"
        ? `ST_AsText(${col})`
        : preserveColumnCollation
          ? col
          : `CAST(${col} AS CHAR)`;
    const collation =
      preserveColumnCollation && column?.characterSet && column.collation
        ? mysqlFilterCollation(column.characterSet, column.collation)
        : null;
    const convertedPattern = collation
      ? `CONVERT(CONVERT(UNHEX(?) USING utf8mb4) USING ${collation.characterSet})`
      : null;
    const rightExpression = collation
      ? `${convertedPattern} COLLATE ${collation.collation}`
      : "CAST(UNHEX(?) AS CHAR)";
    const predicate = `${leftExpression} LIKE ${rightExpression} ESCAPE '!'`;
    const encodedPattern = Buffer.from(pattern, "utf8").toString("hex");
    if (collation && collation.characterSet !== "utf8mb4") {
      return {
        sql: `(${predicate} AND CAST(CONVERT(${convertedPattern} USING utf8mb4) AS BINARY) = UNHEX(?))`,
        params: [encodedPattern, encodedPattern, encodedPattern],
      };
    }
    return {
      sql: predicate,
      params: [encodedPattern],
    };
  }
  private isApproximateNumericType(nativeType: string): boolean {
    const base = mysqlTypeName(nativeType);
    return base === "float" || base === "real" || base === "double";
  }
  private approximateNumericTolerance(rawValue: string): number {
    const fraction = /\.(\d+)/.exec(rawValue)?.[1].length ?? 0;
    const precision = Math.min(Math.max(fraction + 2, 6), 12);
    return 10 ** -precision;
  }
}
