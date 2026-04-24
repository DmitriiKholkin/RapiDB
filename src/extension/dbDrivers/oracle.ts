import oracledb from "oracledb";
import type { ConnectionConfig } from "../connectionManager";
import { BaseDBDriver, formatDatetimeForDisplay } from "./BaseDBDriver";
import type {
  ColumnMeta,
  ColumnTypeMeta,
  DatabaseInfo,
  FilterConditionResult,
  FilterOperator,
  ForeignKeyMeta,
  IndexMeta,
  PaginationResult,
  PersistedEditCheckOptions,
  PersistedEditCheckResult,
  QueryResult,
  SchemaInfo,
  TableInfo,
  TypeCategory,
  ValueSemantics,
} from "./types";
import { DATETIME_SQL_RE, NULL_SENTINEL } from "./types";

function oracleFullType(
  dataType: string,
  dataPrecision: number | null,
  dataScale: number | null,
  dataLength: number,
): string {
  const trimmedType = dataType.trim();
  const normalizedType = trimmedType.toUpperCase();

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

  if (
    ["VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "RAW"].includes(normalizedType)
  ) {
    return `${trimmedType}(${dataLength})`;
  }
  return trimmedType;
}

oracledb.fetchAsString = [oracledb.CLOB];
oracledb.fetchAsBuffer = [oracledb.BLOB];

let _thickInitDone = false;

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

const ORACLE_INTERVAL_DS_NANOS_PER_SECOND = 1_000_000_000n;
const ORACLE_INTERVAL_DS_NANOS_PER_MINUTE =
  60n * ORACLE_INTERVAL_DS_NANOS_PER_SECOND;
const ORACLE_INTERVAL_DS_NANOS_PER_HOUR =
  60n * ORACLE_INTERVAL_DS_NANOS_PER_MINUTE;
const ORACLE_INTERVAL_DS_NANOS_PER_DAY =
  24n * ORACLE_INTERVAL_DS_NANOS_PER_HOUR;
const ORACLE_MAX_SAFE_INTEGER = BigInt(Number.MAX_SAFE_INTEGER);
const ORACLE_MIN_SAFE_INTEGER = BigInt(Number.MIN_SAFE_INTEGER);

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

function normalizeOracleFloatValue(
  value: unknown,
  nativeType?: string,
): string | null {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return null;
  }

  const precision = oracleFloatPrecision(nativeType);
  if (precision === null) {
    return null;
  }

  return Number.parseFloat(value.toPrecision(precision)).toString();
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

function ensureThickMode(libDir?: string): void {
  if (_thickInitDone) {
    return;
  }
  try {
    oracledb.initOracleClient(libDir ? { libDir } : undefined);
    _thickInitDone = true;
  } catch (err: unknown) {
    throw new Error(
      `[RapiDB] Oracle thick mode init failed: ${oracleErrorMessage(err)}\n` +
        `Make sure Oracle Instant Client is installed and the path is correct.`,
    );
  }
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
    .replace(/\(\s*\d+(?:\s*,\s*-?\d+)?\s*\)/g, "")
    .replace(/\s+/g, " ")
    .trim();
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
  options: { assumeUtcWhenMissingTimezone?: boolean } = {},
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
    };
  }

  const digits = rawFrac.slice(1).slice(0, 3).padEnd(3, "0");
  const ms = Number.parseInt(digits, 10);
  const frac =
    ms > 0 ? `.${String(ms).padStart(3, "0").replace(/0+$/, "")}` : "";
  return {
    text: `${date} ${time}${frac}`,
    hasExplicitTimezone: !!rawTimezone,
  };
}

function stripOracleTemporalTimezone(value: string): string {
  return value.replace(/[ ]?(Z|[+-]\d{2}(?::\d{2})?)$/i, "").trim();
}

function trimOracleTemporalFraction(value: string): string {
  return value.replace(/(\.\d*?[1-9])0+$/, "$1").replace(/\.0+$/, "");
}

function formatOracleWallClockDate(value: Date): string | null {
  if (Number.isNaN(value.getTime())) return null;
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
  options: { preserveExplicitTimezoneText?: boolean } = {},
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
      return normalizedText.text;
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

function oracleTemporalFilterExpr(column: ColumnTypeMeta): string {
  const typeName = oracleTypeName(column.nativeType);
  const col = `"${column.name.replace(/"/g, '""')}"`;

  if (typeName === "DATE") {
    return `TO_CHAR(${col}, 'YYYY-MM-DD HH24:MI:SS')`;
  }

  if (isTimezoneAwareOracleTemporal(column.nativeType)) {
    return `RTRIM(RTRIM(TO_CHAR(SYS_EXTRACT_UTC(CAST(${col} AS TIMESTAMP WITH TIME ZONE)), 'YYYY-MM-DD HH24:MI:SS.FF3'), '0'), '.')`;
  }

  return `RTRIM(RTRIM(TO_CHAR(${col}, 'YYYY-MM-DD HH24:MI:SS.FF3'), '0'), '.')`;
}

function formatOracleQueryValue(
  value: unknown,
  meta: oracledb.Metadata<unknown>,
): unknown {
  if (meta.dbType === oracledb.DB_TYPE_BINARY_FLOAT) {
    return normalizeOracleFloatValue(value, "BINARY_FLOAT") ?? value;
  }

  if (meta.dbType === oracledb.DB_TYPE_BINARY_DOUBLE) {
    return normalizeOracleFloatValue(value, "BINARY_DOUBLE") ?? value;
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

  if (
    meta.dbType === oracledb.DB_TYPE_DATE ||
    meta.dbType === oracledb.DB_TYPE_TIMESTAMP
  ) {
    return formatOracleWallClockDate(value) ?? value;
  }

  if (
    meta.dbType === oracledb.DB_TYPE_TIMESTAMP_TZ ||
    meta.dbType === oracledb.DB_TYPE_TIMESTAMP_LTZ
  ) {
    return normalizeOracleTemporalValue(value) ?? value;
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

function splitOracleStatements(src: string): string[] {
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

    if (src[i] === "'") {
      let j = i + 1;
      while (j < len) {
        if (src[j] === "'" && src[j + 1] === "'") {
          j += 2;
        } else if (src[j] === "'") {
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

const ORACLE_NON_FILTERABLE = new Set([
  "blob",
  "clob",
  "nclob",
  "bfile",
  "raw",
  "long raw",
  "long",
  "xmltype",
  "sdo_geometry",
  "anydata",
  "anytype",
]);

export class OracleDriver extends BaseDBDriver {
  private pool: oracledb.Pool | null = null;
  private readonly config: ConnectionConfig;

  constructor(config: ConnectionConfig) {
    super();
    this.config = config;
  }

  async connect(): Promise<void> {
    if (this.pool !== null) {
      try {
        await this.pool.close(0);
      } catch {}
      this.pool = null;
    }

    if (this.config.thickMode) {
      ensureThickMode(this.config.clientPath || undefined);
    }

    const serviceName = this.config.serviceName || this.config.database;
    const host = this.config.host ?? "localhost";
    const port = this.config.port ?? 1521;

    const connectString = serviceName
      ? `${host}:${port}/${serviceName}`
      : `${host}:${port}`;

    this.pool = (await oracledb.createPool({
      user: this.config.username ?? "",
      password: this.config.password ?? "",
      connectString,
      poolMin: 1,
      poolMax: 5,
      poolIncrement: 1,
      poolTimeout: 30,
      poolPingInterval: 60,
    })) as unknown as oracledb.Pool;

    const conn = await this.pool.getConnection();
    try {
      await conn.ping();
    } finally {
      await conn.close();
    }
  }

  async disconnect(): Promise<void> {
    if (this.pool !== null) {
      try {
        await this.pool.close(0);
      } catch {}
      this.pool = null;
    }
  }

  isConnected(): boolean {
    if (!this.pool) {
      return false;
    }

    return this.pool.connectionsInUse > 0 || this.pool.connectionsOpen > 0;
  }

  private async getConnection(): Promise<oracledb.Connection> {
    if (!this.pool) {
      throw new Error("[RapiDB] Oracle connection pool is not initialized.");
    }

    return this.pool.getConnection();
  }

  async listDatabases(): Promise<DatabaseInfo[]> {
    const conn = await this.getConnection();
    try {
      const res = await conn.execute<{ ORA_DATABASE_NAME: string }>(
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
      const res = await conn.execute<{ OWNER: string }>(
        `SELECT username AS owner
        FROM all_users
        WHERE oracle_maintained = 'N'
            OR username = 'SYSTEM'
        ORDER BY username`,
        {},
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );
      return (res.rows ?? []).map((r) => ({ name: r.OWNER }));
    } catch {
      const res2 = await conn.execute<{ USERNAME: string }>(
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
           AND object_type IN ('TABLE','VIEW','FUNCTION','PROCEDURE','PACKAGE')
           AND status = 'VALID'
         ORDER BY object_type, object_name`,
        [schema.toUpperCase()],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );

      for (const r of tableRes.rows ?? []) {
        const rawType = r.OBJECT_TYPE.trim();
        let type: TableInfo["type"];
        if (rawType === "TABLE") {
          type = "table";
        } else if (rawType === "VIEW") {
          type = "view";
        } else if (rawType === "FUNCTION") {
          type = "function";
        } else if (rawType === "PROCEDURE" || rawType === "PACKAGE") {
          type = "procedure";
        } else {
          continue;
        }
        objects.push({ schema, name: r.OBJECT_NAME, type });
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
      const colRes = await conn.execute<{
        COLUMN_NAME: string;
        DATA_TYPE: string;
        DATA_PRECISION: number | null;
        DATA_SCALE: number | null;
        DATA_LENGTH: number;
        NULLABLE: string;
        DATA_DEFAULT: string | null;
        COLUMN_ID: number;
      }>(
        `SELECT column_name, data_type, data_precision, data_scale,
                data_length, nullable, data_default, column_id
         FROM all_tab_columns
         WHERE owner = :1 AND table_name = :2
         ORDER BY column_id`,
        [schema.toUpperCase(), table.toUpperCase()],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );

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

      const fkRes = await conn.execute<{ COLUMN_NAME: string }>(
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

      return (colRes.rows ?? []).map((r) => {
        const genType = identityMap.get(r.COLUMN_NAME);
        const primaryKeyOrdinal = pkOrdinalByColumn.get(r.COLUMN_NAME);
        return {
          name: r.COLUMN_NAME,
          type: oracleFullType(
            r.DATA_TYPE,
            r.DATA_PRECISION,
            r.DATA_SCALE,
            r.DATA_LENGTH,
          ),
          nullable: r.NULLABLE === "Y",
          defaultValue:
            genType !== undefined
              ? undefined
              : (r.DATA_DEFAULT?.trim() ?? undefined),
          isPrimaryKey: primaryKeyOrdinal !== undefined,
          primaryKeyOrdinal,
          isForeignKey: fkCols.has(r.COLUMN_NAME),
          isAutoIncrement: genType !== undefined,
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

  async getCreateTableDDL(
    _database: string,
    schema: string,
    table: string,
  ): Promise<string> {
    const conn = await this.getConnection();
    try {
      const objectType = await this._resolveDdlObjectType(conn, schema, table);

      if (objectType) {
        const ddl = await this._getMetadataDDL(conn, objectType, schema, table);
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

  private async _resolveDdlObjectType(
    conn: oracledb.Connection,
    schema: string,
    objectName: string,
  ): Promise<"TABLE" | "VIEW" | null> {
    const owner = schema.toUpperCase();
    const name = objectName.toUpperCase();
    const obj = oracledb.OUT_FORMAT_OBJECT;

    try {
      const objRes = await conn.execute<{ OBJECT_TYPE: string }>(
        `SELECT object_type FROM all_objects
         WHERE owner = :1 AND object_name = :2
           AND object_type IN ('TABLE','VIEW') AND ROWNUM = 1`,
        [owner, name],
        { outFormat: obj },
      );
      const objectType = objRes.rows?.[0]?.OBJECT_TYPE;
      if (objectType === "TABLE" || objectType === "VIEW") {
        return objectType;
      }
    } catch {}

    try {
      const viewRes = await conn.execute<{ VIEW_NAME: string }>(
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
      const tableRes = await conn.execute<{ TABLE_NAME: string }>(
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
    const res = await conn.execute<{ TEXT: unknown }>(
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
    objectType: "TABLE" | "VIEW",
    schema: string,
    objectName: string,
  ): Promise<string | null> {
    try {
      await conn.execute(ORACLE_DDL_TRANSFORM_BLOCK);
    } catch {}

    try {
      const res = await conn.execute<{ DDL: unknown }>(
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

    const colRes = await conn.execute<{
      COLUMN_NAME: string;
      DATA_TYPE: string;
      DATA_PRECISION: number | null;
      DATA_SCALE: number | null;
      DATA_LENGTH: number;
      NULLABLE: string;
      DATA_DEFAULT: string | null;
    }>(
      `SELECT column_name, data_type, data_precision, data_scale,
              data_length, nullable, data_default
       FROM all_tab_columns
       WHERE owner = :1 AND table_name = :2
       ORDER BY column_id`,
      [schema.toUpperCase(), table.toUpperCase()],
      { outFormat: OBJ },
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

    const colLines = (colRes.rows ?? []).map((r) => {
      const typ = oracleFullType(
        r.DATA_TYPE,
        r.DATA_PRECISION,
        r.DATA_SCALE,
        r.DATA_LENGTH,
      );

      const nullable = r.NULLABLE === "Y" ? "" : " NOT NULL";

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

  async getRoutineDefinition(
    _database: string,
    schema: string,
    name: string,
    _kind: "function" | "procedure",
  ): Promise<string> {
    const conn = await this.getConnection();
    try {
      const res = await conn.execute<{ TEXT: string }>(
        `SELECT text FROM all_source
         WHERE owner = :1 AND name = :2
           AND type IN ('FUNCTION','PROCEDURE','PACKAGE','PACKAGE BODY')
         ORDER BY type, line`,
        [schema.toUpperCase(), name.toUpperCase()],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );
      const lines = (res.rows ?? []).map((r) => r.TEXT);
      if (lines.length === 0) {
        return `-- Source not available for ${schema}.${name}`;
      }
      return `CREATE OR REPLACE ${lines.join("")}`;
    } finally {
      await conn.close();
    }
  }

  async query(sql: string, params?: unknown[]): Promise<QueryResult> {
    const start = Date.now();

    if (params && params.length > 0) {
      return this._execOne(sql, params, start);
    }

    const statements = splitOracleStatements(sql.trim());

    let selectResult: QueryResult | null = null;
    let totalAffected = 0;

    for (const stmt of statements) {
      const result = await this._execOne(stmt, undefined, start);
      totalAffected += result.rowCount;
      if (result.columns.length > 0) {
        selectResult = result;
      }
    }

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

  private async _execOne(
    sql: string,
    params: unknown[] | undefined,
    start: number,
  ): Promise<QueryResult> {
    const conn = await this.getConnection();
    try {
      let finalSql = sql;
      let binds: unknown[] = [];

      if (params && params.length > 0) {
        const replaced = replacePositionalParams(sql, params);
        finalSql = replaced.sql;
        binds = replaced.binds;
      }

      const options: oracledb.ExecuteOptions = {
        outFormat: oracledb.OUT_FORMAT_ARRAY,
        fetchArraySize: 100,
        autoCommit: true,
        fetchTypeHandler: this._fetchTypeHandler.bind(this),
      };

      const res = await conn.execute(finalSql, binds, options);

      const executionTimeMs = Date.now() - start;

      if (res.metaData && res.rows && res.rows.length > 0) {
        const metaData = res.metaData;
        const columns = metaData.map((m) => m.name);
        const rows = (res.rows as unknown[][]).map((row) =>
          Object.fromEntries(
            row.map((val, i) => [
              `__col_${i}`,
              formatOracleQueryValue(val, metaData[i]),
            ]),
          ),
        );
        return { columns, rows, rowCount: rows.length, executionTimeMs };
      }

      if (res.metaData) {
        const columns = res.metaData.map((m) => m.name);
        return {
          columns,
          rows: [],
          rowCount: res.rowsAffected ?? 0,
          executionTimeMs,
        };
      }

      const affectedRows = res.rowsAffected ?? 0;
      return {
        columns: [],
        rows: [],
        rowCount: affectedRows,
        executionTimeMs,
        affectedRows,
      };
    } finally {
      await conn.close();
    }
  }

  async runTransaction(
    operations: import("./types").TransactionOperation[],
  ): Promise<void> {
    const conn = await this.getConnection();
    try {
      for (const op of operations) {
        let finalSql = op.sql;
        let binds: Record<string, unknown> | unknown[] = {};
        if (op.params && op.params.length > 0) {
          const replaced = replacePositionalParams(op.sql, op.params);
          finalSql = replaced.sql;
          binds = replaced.binds;
        }

        const res = await conn.execute(
          finalSql,
          binds as oracledb.BindParameters,
          {
            outFormat: oracledb.OUT_FORMAT_OBJECT,
            autoCommit: false,
          },
        );

        if (op.checkAffectedRows && (res.rowsAffected ?? 0) === 0) {
          throw new Error(
            "Row not found — the row may have been modified or deleted by another user",
          );
        }
      }
      await conn.commit();
    } catch (e) {
      try {
        await conn.rollback();
      } catch {}
      throw e;
    } finally {
      await conn.close();
    }
  }

  // ─── Oracle type system ───

  mapTypeCategory(nativeType: string): TypeCategory {
    const normalizedType = nativeType.toUpperCase().trim();
    const ct = normalizedType.split("(")[0].trim();
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
    // VARCHAR2, NVARCHAR2, CHAR, NCHAR
    if (ct.includes("CHAR") || ct.includes("VARCHAR")) return "text";
    return "other";
  }

  protected getValueSemantics(
    _nativeType: string,
    _category: TypeCategory,
  ): ValueSemantics {
    return "plain";
  }

  isDatetimeWithTime(nativeType: string): boolean {
    const ct = nativeType.toUpperCase().split("(")[0].trim();
    return ct === "DATE" || ct.startsWith("TIMESTAMP");
  }

  protected override isFilterable(
    nativeType: string,
    category: TypeCategory,
  ): boolean {
    return (
      super.isFilterable(nativeType, category) &&
      category !== "interval" &&
      !ORACLE_NON_FILTERABLE.has(oracleTypeName(nativeType).toLowerCase())
    );
  }

  // ─── Oracle SQL helpers ───

  override quoteIdentifier(name: string): string {
    return `"${name.replace(/"/g, '""')}"`;
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

  override buildInsertValueExpr(
    _column: ColumnTypeMeta,
    paramIndex: number,
  ): string {
    return `:${paramIndex}`;
  }

  override buildSetExpr(column: ColumnTypeMeta, paramIndex: number): string {
    return `${this.quoteIdentifier(column.name)} = :${paramIndex}`;
  }

  // ─── Oracle type-aware data helpers ───

  override coerceInputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (value === null || value === undefined || value === "") return value;
    if (value === NULL_SENTINEL) return null;
    if (typeof value !== "string") return value;

    if (column.category === "float") {
      const trimmed = value.trim();
      if (trimmed !== "") {
        const parsed = Number(trimmed);
        if (Number.isFinite(parsed)) return parsed;
      }
    }

    // Binary
    if (column.category === "binary")
      return super.coerceInputValue(value, column);

    // ISO datetime → Oracle-friendly
    if (this.isDatetimeWithTime(column.nativeType)) {
      const normalizedText = normalizeOracleTemporalText(value);
      if (
        normalizedText?.hasExplicitTimezone &&
        !isTimezoneAwareOracleTemporal(column.nativeType)
      ) {
        const parsedLocal = parseOracleTemporalInput(normalizedText.text, {
          assumeUtcWhenMissingTimezone: false,
        });
        if (parsedLocal) return parsedLocal;
      }

      const parsed = parseOracleTemporalInput(value, {
        assumeUtcWhenMissingTimezone: isTimezoneAwareOracleTemporal(
          column.nativeType,
        ),
      });
      if (parsed) return parsed;
    }

    return value;
  }

  override formatOutputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (value === null || value === undefined) return value;
    if (Buffer.isBuffer(value)) return super.formatOutputValue(value, column);
    if (typeof value === "bigint") return value.toString();
    if (column.category === "float") {
      const normalizedFloat = normalizeOracleFloatValue(
        value,
        column.nativeType,
      );
      if (normalizedFloat !== null) return normalizedFloat;
    }
    if (isOracleIntervalType(column.nativeType)) {
      const intervalText = normalizeOracleIntervalValue(value);
      if (intervalText !== null) return intervalText;
    }
    if (this.isDatetimeWithTime(column.nativeType)) {
      if (
        value instanceof Date &&
        !isTimezoneAwareOracleTemporal(column.nativeType)
      ) {
        const formatted = formatOracleWallClockDate(value);
        if (formatted !== null) return formatted;
      }
      const formatted = normalizeOracleTemporalValue(value);
      if (formatted !== null) return formatted;
    }
    // LOB-like values already consumed as string/Buffer via fetchAsString/fetchAsBuffer
    return super.formatOutputValue(value, column);
  }

  override checkPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null {
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
      column.category === "text" ||
      column.category === "date" ||
      column.category === "time" ||
      column.category === "datetime"
    ) {
      return this.checkTextPersistedEdit(column, expectedValue, options);
    }

    return null;
  }

  // ─── Oracle filter building ───

  override buildFilterCondition(
    column: ColumnTypeMeta,
    operator: FilterOperator,
    value: string | [string, string] | undefined,
    paramIndex: number,
  ): FilterConditionResult | null {
    const col = this.quoteIdentifier(column.name);

    if (operator === "is_null") return { sql: `${col} IS NULL`, params: [] };
    if (operator === "is_not_null")
      return { sql: `${col} IS NOT NULL`, params: [] };

    if (!column.filterable) return null;
    if (value === undefined) return null;

    const val = typeof value === "string" ? value.trim() : value;

    // Numeric
    if (
      this.isNumericCategory(column.category) &&
      typeof val === "string" &&
      !Number.isNaN(Number(val)) &&
      val !== ""
    ) {
      const sqlOp = this.sqlOperator(operator);
      return {
        sql: `${col} ${sqlOp} :${paramIndex}`,
        params: [column.category === "float" ? Number(val) : val],
      };
    }

    // Between
    if (operator === "between" && Array.isArray(val)) {
      return {
        sql: `${col} BETWEEN :${paramIndex} AND :${paramIndex + 1}`,
        params: [val[0], val[1]],
      };
    }

    // In
    if (operator === "in" && typeof val === "string") {
      const parts = val.split(",").map((s) => s.trim());
      return {
        sql: `${col} IN (${parts.map((_, i) => `:${paramIndex + i}`).join(", ")})`,
        params: parts,
      };
    }

    // Datetime: TO_CHAR LIKE
    if (this.isDatetimeWithTime(column.nativeType)) {
      const v = typeof val === "string" ? val : val[0];
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
      return {
        sql: `${oracleTemporalFilterExpr(column)} LIKE :${paramIndex}`,
        params: [`%${comparable}%`],
      };
    }

    // Default text LIKE (case-insensitive)
    const v = typeof val === "string" ? val : val[0];
    return {
      sql: `UPPER(${col}) LIKE UPPER(:${paramIndex})`,
      params: [`%${v}%`],
    };
  }
}
