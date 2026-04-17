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
  QueryResult,
  SchemaInfo,
  TableInfo,
  TypeCategory,
} from "./types";
import { DATETIME_SQL_RE, NULL_SENTINEL } from "./types";

function oracleFullType(
  dataType: string,
  dataPrecision: number | null,
  dataScale: number | null,
  dataLength: number,
): string {
  if (dataType === "NUMBER") {
    if (dataPrecision !== null && dataScale !== null) {
      return `NUMBER(${dataPrecision},${dataScale})`;
    }
    if (dataPrecision !== null) {
      return `NUMBER(${dataPrecision})`;
    }
    return "NUMBER";
  }
  if (dataType === "FLOAT") {
    return dataPrecision !== null ? `FLOAT(${dataPrecision})` : "FLOAT";
  }

  if (["VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "RAW"].includes(dataType)) {
    return `${dataType}(${dataLength})`;
  }
  return dataType;
}

oracledb.fetchAsString = [oracledb.CLOB];
oracledb.fetchAsBuffer = [oracledb.BLOB];

let _thickInitDone = false;

const pad2 = (n: number) => String(n).padStart(2, "0");

function ensureThickMode(libDir?: string): void {
  if (_thickInitDone) {
    return;
  }
  try {
    oracledb.initOracleClient(libDir ? { libDir } : undefined);
    _thickInitDone = true;
  } catch (err: any) {
    throw new Error(
      `[RapiDB] Oracle thick mode init failed: ${err.message}\n` +
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

const ORACLE_NON_EDITABLE = new Set([
  "blob",
  "clob",
  "nclob",
  "bfile",
  "long raw",
  "long",
  "xmltype",
  "sdo_geometry",
  "anydata",
  "anytype",
  "anydataset",
  "object",
  "ref",
  "opaque",
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

  async listDatabases(): Promise<DatabaseInfo[]> {
    const conn = await this.pool!.getConnection();
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
    const conn = await this.pool!.getConnection();
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
    const conn = await this.pool!.getConnection();
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
    const conn = await this.pool!.getConnection();
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

      const pkRes = await conn.execute<{ COLUMN_NAME: string }>(
        `SELECT cols.column_name
         FROM all_constraints cons
         JOIN all_cons_columns cols
           ON cons.constraint_name = cols.constraint_name
           AND cons.owner = cols.owner
         WHERE cons.constraint_type = 'P'
           AND cons.owner = :1
           AND cons.table_name = :2`,
        [schema.toUpperCase(), table.toUpperCase()],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );
      const pkCols = new Set((pkRes.rows ?? []).map((r) => r.COLUMN_NAME));

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
          isPrimaryKey: pkCols.has(r.COLUMN_NAME),
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
    const conn = await this.pool!.getConnection();
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
        map.get(r.INDEX_NAME)!.columns.push(r.COLUMN_NAME);
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
    const conn = await this.pool!.getConnection();
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
    const conn = await this.pool!.getConnection();
    try {
      const objRes = await conn.execute<{ OBJECT_TYPE: string }>(
        `SELECT object_type FROM all_objects
         WHERE owner = :1 AND object_name = :2
           AND object_type IN ('TABLE','VIEW') AND ROWNUM = 1`,
        [schema.toUpperCase(), table.toUpperCase()],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      );
      const objectType = objRes.rows?.[0]?.OBJECT_TYPE ?? "TABLE";

      if (objectType === "VIEW") {
        try {
          const res = await conn.execute<{ DDL: string }>(
            `SELECT DBMS_METADATA.GET_DDL('VIEW', :2, :1) AS ddl FROM dual`,
            [schema.toUpperCase(), table.toUpperCase()],
            { outFormat: oracledb.OUT_FORMAT_OBJECT },
          );
          const ddl = res.rows?.[0]?.DDL;
          if (typeof ddl === "string" && ddl.trim()) {
            return ddl.trim();
          }
          if (ddl && typeof (ddl as any).getData === "function") {
            const text = ((await (ddl as any).getData()) as string).trim();
            if (text) return text;
          }
        } catch {}
        return await this._fallbackViewDDL(conn, schema, table);
      }

      return await this._fallbackDDL(conn, schema, table);
    } catch {
      try {
        return await this._fallbackDDL(conn, schema, table);
      } catch {
        return `-- DDL not available for "${schema}"."${table}"`;
      }
    } finally {
      await conn.close();
    }
  }

  private async _fallbackViewDDL(
    conn: oracledb.Connection,
    schema: string,
    view: string,
  ): Promise<string> {
    const res = await conn.execute<{ TEXT: any }>(
      `SELECT text FROM all_views WHERE owner = :1 AND view_name = :2`,
      [schema.toUpperCase(), view.toUpperCase()],
      { outFormat: oracledb.OUT_FORMAT_OBJECT },
    );

    const row = res.rows?.[0];
    if (!row || !row.TEXT) {
      return `-- View source not available for "${schema}"."${view}"`;
    }

    const text = await this._consumeLob(row.TEXT);
    const cleanText = typeof text === "string" ? text.trim() : "";

    return `CREATE OR REPLACE VIEW "${schema}"."${view}" AS\n${cleanText}`;
  }

  private async _consumeLob(lob: any): Promise<string | Buffer | null> {
    if (!lob) return null;

    try {
      if (typeof lob === "string" || Buffer.isBuffer(lob)) {
        return lob;
      }

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
      let typ = r.DATA_TYPE;
      if (r.DATA_PRECISION !== null && r.DATA_SCALE !== null) {
        typ = `${typ}(${r.DATA_PRECISION},${r.DATA_SCALE})`;
      } else if (r.DATA_PRECISION !== null) {
        typ = `${typ}(${r.DATA_PRECISION})`;
      } else if (
        ["VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "RAW"].includes(r.DATA_TYPE)
      ) {
        typ = `${typ}(${r.DATA_LENGTH})`;
      }

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
    const conn = await this.pool!.getConnection();
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
      return { type: oracledb.NUMBER };
    }
    return undefined;
  }

  private async _execOne(
    sql: string,
    params: unknown[] | undefined,
    start: number,
  ): Promise<QueryResult> {
    const conn = await this.pool!.getConnection();
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
    const conn = await this.pool!.getConnection();
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
    if (ct.startsWith("INTERVAL")) return "text";
    if (["BLOB", "RAW", "LONG RAW"].includes(ct)) return "binary";
    if (["CLOB", "NCLOB", "LONG"].includes(ct)) return "text";
    if (ct === "XMLTYPE") return "text";
    if (ct === "SDO_GEOMETRY") return "spatial";
    if (ct === "ROWID" || ct === "UROWID") return "text";
    // VARCHAR2, NVARCHAR2, CHAR, NCHAR
    if (ct.includes("CHAR") || ct.includes("VARCHAR")) return "text";
    return "other";
  }

  isBooleanType(_nativeType: string): boolean {
    return false;
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
      !ORACLE_NON_FILTERABLE.has(oracleTypeName(nativeType).toLowerCase())
    );
  }

  protected override isEditable(
    nativeType: string,
    category: TypeCategory,
  ): boolean {
    return (
      super.isEditable(nativeType, category) &&
      !ORACLE_NON_EDITABLE.has(oracleTypeName(nativeType).toLowerCase())
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
    return value;
  }

  // ─── Oracle filter building ───

  override buildFilterCondition(
    column: ColumnTypeMeta,
    operator: FilterOperator,
    value: string | [string, string] | undefined,
    paramIndex: number,
  ): FilterConditionResult | null {
    if (!column.filterable) return null;
    if (value === undefined) return null;
    const col = this.quoteIdentifier(column.name);
    const val = typeof value === "string" ? value.trim() : value;

    if (operator === "is_null") return { sql: `${col} IS NULL`, params: [] };
    if (operator === "is_not_null")
      return { sql: `${col} IS NOT NULL`, params: [] };

    // Numeric
    if (
      this.isNumericCategory(column.category) &&
      typeof val === "string" &&
      !Number.isNaN(Number(val)) &&
      val !== ""
    ) {
      const sqlOp = this.sqlOperator(operator);
      return { sql: `${col} ${sqlOp} :${paramIndex}`, params: [Number(val)] };
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
