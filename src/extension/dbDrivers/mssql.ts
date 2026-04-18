import * as mssql from "mssql";
import type { ConnectionConfig } from "../connectionManager";
import {
  BaseDBDriver,
  formatDatetimeForDisplay,
  isoToLocalDateStr,
} from "./BaseDBDriver";
import type {
  ColumnMeta,
  ColumnTypeMeta,
  DatabaseInfo,
  FilterConditionResult,
  FilterOperator,
  PaginationResult,
  QueryResult,
  SchemaInfo,
  TableInfo,
  TypeCategory,
} from "./types";
import {
  DATE_ONLY_RE,
  DATETIME_SQL_RE,
  ISO_DATETIME_RE,
  NULL_SENTINEL,
} from "./types";

type MssqlSqlType = (() => mssql.ISqlType) | mssql.ISqlType;

interface NamedRow {
  name: string;
}

interface ObjectRow {
  name: string;
  type: string;
}

interface DescribeColumnRow {
  COLUMN_NAME: string;
  DATA_TYPE: string;
  max_length: number;
  precision: number;
  scale: number;
  IS_NULLABLE: boolean | number;
  is_identity: boolean | number;
  COLUMN_DEFAULT: string | null;
  IS_PK: number;
  PK_ORDINAL: number | null;
  IS_FK: number;
}

interface IndexRow {
  idx_name: string;
  col_name: string;
  is_unique: boolean | number;
  is_pk: boolean | number;
}

interface ForeignKeyRow {
  constraint_name: string;
  column_name: string;
  ref_schema: string;
  ref_table: string;
  ref_column: string;
}

interface DdlColumnRow {
  COLUMN_NAME: string;
  DATA_TYPE: string;
  max_length: number;
  precision: number;
  scale: number;
  IS_NULLABLE: boolean | number;
  is_identity: boolean | number;
  COLUMN_DEFAULT: string | null;
  IS_PK: number;
  PK_ORDINAL: number | null;
}

interface RoutineDefinitionRow {
  def: string | null;
}

interface MssqlArrayColumnMeta {
  index: number;
  name: string;
  type: MssqlSqlType;
  scale?: number;
  precision?: number;
  nullable: boolean;
  identity: boolean;
  readOnly: boolean;
}

interface MssqlArrayResult extends mssql.IResult<unknown[]> {
  columns?: MssqlArrayColumnMeta[][];
}

const MSSQL_TIME_RE = /^\d{2}:\d{2}:\d{2}(?:\.\d{1,7})?$/;
const MSSQL_UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const INTEGER_RE = /^-?\d+$/;
const DECIMAL_RE = /^-?\d+(?:\.\d+)?$/;

function mssqlFullType(
  typeName: string,
  maxLength: number,
  precision: number,
  scale: number,
): string {
  const t = typeName.toLowerCase();

  if (["varchar", "char", "varbinary", "binary"].includes(t)) {
    return maxLength === -1 ? `${t}(MAX)` : `${t}(${maxLength})`;
  }
  if (["nvarchar", "nchar"].includes(t)) {
    return maxLength === -1 ? `${t}(MAX)` : `${t}(${maxLength / 2})`;
  }

  if (["decimal", "numeric"].includes(t)) {
    return `${t}(${precision},${scale})`;
  }

  if (t === "float") {
    return `float(${precision})`;
  }

  if (["datetime2", "datetimeoffset", "time"].includes(t)) {
    return `${t}(${scale})`;
  }
  return typeName;
}

function cleanMssqlDefault(raw: string): string {
  let s = raw.trim();
  while (s.startsWith("(") && s.endsWith(")")) {
    s = s.slice(1, -1).trim();
  }
  if (s.startsWith("N'") && s.endsWith("'")) {
    s = s.slice(1);
  }
  return s;
}

const escapeMssqlId = (s: string) => s.replace(/]/g, "]]");

function baseTypeName(typeName: string): string {
  return typeName.toLowerCase().split("(")[0].trim();
}

function hasExplicitTimezone(value: string): boolean {
  return /[zZ]|[+-]\d{2}:\d{2}$/.test(value);
}

function normalizeSqlDatetimeOffsetSpacing(value: string): string {
  return value.replace(/ ([+-]\d{2}:\d{2})$/, "$1");
}

function normalizeDatetimeLiteral(value: string): string {
  const trimmed = normalizeSqlDatetimeOffsetSpacing(value.trim());
  if (DATETIME_SQL_RE.test(trimmed)) {
    return trimmed.replace(" ", "T");
  }
  return trimmed;
}

function normalizeDateLiteral(value: string): string {
  const trimmed = value.trim();
  if (DATE_ONLY_RE.test(trimmed)) {
    return trimmed;
  }
  if (ISO_DATETIME_RE.test(trimmed)) {
    if (!hasExplicitTimezone(trimmed)) {
      return trimmed.slice(0, 10);
    }
    return isoToLocalDateStr(trimmed) ?? trimmed.slice(0, 10);
  }

  const normalizedSql = normalizeSqlDatetimeOffsetSpacing(trimmed);
  if (DATETIME_SQL_RE.test(normalizedSql)) {
    if (hasExplicitTimezone(normalizedSql)) {
      return (
        isoToLocalDateStr(normalizedSql.replace(" ", "T")) ??
        normalizedSql.slice(0, 10)
      );
    }
    return normalizedSql.slice(0, 10);
  }

  return trimmed;
}

function detectScale(value: unknown): number | null {
  if (typeof value !== "string") {
    return null;
  }

  const match = /\.(\d+)/.exec(value);
  if (!match) return null;
  return Math.min(match[1].length, 7);
}

function isSetFlag(value: boolean | number | null | undefined): boolean {
  return value === true || value === 1;
}

function columnTypeName(meta: MssqlArrayColumnMeta | undefined): string {
  const rawType = meta?.type;
  if (!rawType) return "";
  if (typeof rawType === "function") {
    return rawType.name;
  }
  return typeof rawType.type === "function" ? rawType.type.name : "";
}

function temporalSearchLiteral(value: string): string {
  return normalizeDatetimeLiteral(value).replace(" ", "T");
}

const MSSQL_NON_FILTERABLE = new Set([
  "image",
  "text",
  "ntext",
  "xml",
  "geography",
  "geometry",
  "hierarchyid",
  "sql_variant",
  "timestamp",
  "rowversion",
]);

const MSSQL_NON_EDITABLE = new Set(MSSQL_NON_FILTERABLE);

export class MSSQLDriver extends BaseDBDriver {
  private pool: mssql.ConnectionPool | null = null;
  private readonly config: ConnectionConfig;

  constructor(config: ConnectionConfig) {
    super();
    this.config = config;
  }

  private requirePool(): mssql.ConnectionPool {
    if (this.pool) {
      return this.pool;
    }
    throw new Error("[RapiDB] MSSQL connection is not open");
  }

  private poolConfig(): mssql.config {
    if (!this.config.host) {
      throw new Error("[RapiDB] MSSQL host is required");
    }

    const sslEnabled = this.config.ssl ?? true;
    const trustCert = !(this.config.rejectUnauthorized ?? true);

    return {
      server: this.config.host,
      port: this.config.port,
      database: this.config.database,
      user: this.config.username,
      password: this.config.password,
      connectionTimeout: 10000,
      requestTimeout: 30000,
      options: {
        encrypt: sslEnabled,
        trustServerCertificate: trustCert,
        enableArithAbort: true,
        abortTransactionOnError: true,
        useUTC: true,
      },
    };
  }

  private createNVarCharType(value: unknown): MssqlSqlType {
    const length = typeof value === "string" ? value.length : 0;
    return mssql.NVarChar(length === 0 || length > 4000 ? mssql.MAX : length);
  }

  private normalizeInputValue(
    value: unknown,
    column?: ColumnTypeMeta,
  ): unknown {
    if (value === NULL_SENTINEL) return null;
    if (value === null || value === undefined || value === "") return value;
    if (typeof value !== "string") return value;

    const trimmed = value.trim();
    if (trimmed === "") return value;

    if (column?.isBoolean) {
      const lower = trimmed.toLowerCase();
      if (lower === "true" || lower === "1") return true;
      if (lower === "false" || lower === "0") return false;
    }

    if (column?.category === "binary") {
      return super.coerceInputValue(trimmed, column);
    }

    if (column?.category === "integer") {
      if (INTEGER_RE.test(trimmed)) {
        return baseTypeName(column.nativeType) === "bigint"
          ? BigInt(trimmed)
          : Number(trimmed);
      }
      return value;
    }

    if (column?.category === "float") {
      return DECIMAL_RE.test(trimmed) ? Number(trimmed) : value;
    }

    if (column?.category === "decimal") {
      return DECIMAL_RE.test(trimmed) ? trimmed : value;
    }

    if (column?.category === "date") {
      return normalizeDateLiteral(trimmed);
    }

    if (column?.category === "time") {
      return MSSQL_TIME_RE.test(trimmed) ? trimmed : value;
    }

    if (column?.category === "datetime") {
      const normalized = normalizeDatetimeLiteral(trimmed);
      return ISO_DATETIME_RE.test(normalized) ||
        DATETIME_SQL_RE.test(normalized.replace("T", " "))
        ? normalized
        : value;
    }

    if (DATE_ONLY_RE.test(trimmed) || MSSQL_TIME_RE.test(trimmed)) {
      return trimmed;
    }

    const normalizedDateTime = normalizeDatetimeLiteral(trimmed);
    if (
      ISO_DATETIME_RE.test(normalizedDateTime) ||
      DATETIME_SQL_RE.test(normalizedDateTime.replace("T", " "))
    ) {
      return normalizedDateTime;
    }

    return value;
  }

  private typeForValue(value: unknown): MssqlSqlType {
    if (Buffer.isBuffer(value)) {
      return mssql.VarBinary(value.length === 0 ? mssql.MAX : value.length);
    }
    if (typeof value === "bigint") return mssql.BigInt;
    if (typeof value === "number") {
      if (!Number.isFinite(value) || !Number.isInteger(value)) {
        return mssql.Float;
      }
      if (value >= 0 && value <= 255) return mssql.TinyInt;
      if (value >= -32768 && value <= 32767) return mssql.SmallInt;
      if (value >= -2147483648 && value <= 2147483647) return mssql.Int;
      return mssql.BigInt;
    }
    if (typeof value === "boolean") return mssql.Bit;
    if (value instanceof Date) return mssql.DateTime2(7);
    if (typeof value === "string") {
      if (MSSQL_UUID_RE.test(value)) return mssql.UniqueIdentifier;
      if (DATE_ONLY_RE.test(value)) return mssql.Date;
      if (MSSQL_TIME_RE.test(value)) {
        return mssql.Time(detectScale(value) ?? 7);
      }
      const normalized = normalizeDatetimeLiteral(value);
      if (ISO_DATETIME_RE.test(normalized)) {
        return hasExplicitTimezone(normalized)
          ? mssql.DateTimeOffset(detectScale(normalized) ?? 7)
          : mssql.DateTime2(detectScale(normalized) ?? 7);
      }
      if (DATETIME_SQL_RE.test(normalized.replace("T", " "))) {
        return hasExplicitTimezone(normalized)
          ? mssql.DateTimeOffset(detectScale(normalized) ?? 7)
          : mssql.DateTime2(detectScale(normalized) ?? 7);
      }
      return this.createNVarCharType(value);
    }
    return this.createNVarCharType(value);
  }

  private bindRequestInput(
    request: mssql.Request,
    name: string,
    rawValue: unknown,
  ): void {
    const normalizedValue = this.normalizeInputValue(rawValue);
    const value = normalizedValue === undefined ? null : normalizedValue;
    const type = this.typeForValue(value);
    request.input(name, type, value);
  }

  private bindPositionalParameters(
    request: mssql.Request,
    sql: string,
    params?: readonly unknown[],
  ): string {
    if (!params || params.length === 0) {
      return sql;
    }

    const placeholderCount = (sql.match(/\?/g) ?? []).length;
    if (placeholderCount !== params.length) {
      throw new Error(
        `[RapiDB] MSSQL parameter mismatch: SQL has ${placeholderCount} placeholder(s) but ${params.length} value(s) were supplied.`,
      );
    }

    let index = 0;
    return sql.replace(/\?/g, () => {
      const name = `p${++index}`;
      this.bindRequestInput(request, name, params[index - 1]);
      return `@${name}`;
    });
  }

  private formatQueryValue(
    value: unknown,
    columnMeta: MssqlArrayColumnMeta | undefined,
  ): unknown {
    const typeName = columnTypeName(columnMeta);
    const pad = (n: number) => String(n).padStart(2, "0");

    if (
      typeName === "Real" &&
      typeof value === "number" &&
      !Number.isInteger(value)
    ) {
      return Number.parseFloat(value.toPrecision(7));
    }

    if (value instanceof Date && !Number.isNaN(value.getTime())) {
      if (typeName === "Date") {
        return `${value.getUTCFullYear()}-${pad(value.getUTCMonth() + 1)}-${pad(value.getUTCDate())}`;
      }
      if (typeName === "Time") {
        const ms = value.getUTCMilliseconds();
        const frac =
          ms > 0 ? `.${String(ms).padStart(3, "0").replace(/0+$/, "")}` : "";
        return `${pad(value.getUTCHours())}:${pad(value.getUTCMinutes())}:${pad(value.getUTCSeconds())}${frac}`;
      }
      if (typeName === "DateTimeOffset") {
        const ms = value.getUTCMilliseconds();
        const frac =
          ms > 0 ? `.${String(ms).padStart(3, "0").replace(/0+$/, "")}` : "";
        return (
          `${value.getUTCFullYear()}-${pad(value.getUTCMonth() + 1)}-${pad(value.getUTCDate())} ` +
          `${pad(value.getUTCHours())}:${pad(value.getUTCMinutes())}:${pad(value.getUTCSeconds())}${frac} +00:00`
        );
      }
    }

    return value;
  }

  async connect(): Promise<void> {
    if (this.pool !== null) {
      try {
        await this.pool.close();
      } catch {}
      this.pool = null;
    }

    const pool = new mssql.ConnectionPool(this.poolConfig());
    pool.on("error", (err: unknown) => {
      console.error(
        "[RapiDB] MSSQL pool error:",
        err instanceof Error ? err.message : err,
      );
    });
    this.pool = await pool.connect();
  }

  async disconnect(): Promise<void> {
    await this.pool?.close();
    this.pool = null;
  }

  isConnected(): boolean {
    return this.pool?.connected ?? false;
  }

  async listDatabases(): Promise<DatabaseInfo[]> {
    const res = await this.requirePool()
      .request()
      .query<NamedRow>(
        `SELECT name FROM sys.databases WHERE state_desc = 'ONLINE' ORDER BY name`,
      );
    return res.recordset.map((row) => ({
      name: row.name,
      schemas: [],
    }));
  }

  async listSchemas(database: string): Promise<SchemaInfo[]> {
    const res = await this.requirePool()
      .request()
      .query<NamedRow>(
        `SELECT SCHEMA_NAME AS name FROM [${escapeMssqlId(database)}].INFORMATION_SCHEMA.SCHEMATA
         WHERE SCHEMA_NAME NOT IN ('sys','INFORMATION_SCHEMA','db_accessadmin','db_backupoperator',
           'db_datareader','db_datawriter','db_ddladmin','db_denydatareader','db_denydatawriter',
           'db_owner','db_securityadmin','guest')
         ORDER BY SCHEMA_NAME`,
      );
    return res.recordset.map((row) => ({ name: row.name }));
  }

  async listObjects(database: string, schema: string): Promise<TableInfo[]> {
    const objects: TableInfo[] = [];
    const esc = (s: string) => s.replace(/'/g, "''");
    const tableRes = await this.requirePool()
      .request()
      .query<ObjectRow>(
        `SELECT TABLE_NAME AS name, TABLE_TYPE AS type
         FROM [${escapeMssqlId(database)}].INFORMATION_SCHEMA.TABLES
         WHERE TABLE_SCHEMA = '${esc(schema)}' ORDER BY TABLE_NAME`,
      );
    for (const row of tableRes.recordset) {
      objects.push({
        schema,
        name: row.name,
        type: (row.type.includes("VIEW")
          ? "view"
          : "table") as TableInfo["type"],
      });
    }
    try {
      const routineRes = await this.requirePool()
        .request()
        .query<ObjectRow>(
          `SELECT o.name,
                  CASE o.type WHEN 'P' THEN 'procedure' WHEN 'PC' THEN 'procedure'
                              WHEN 'FN' THEN 'function'  WHEN 'IF' THEN 'function'
                              WHEN 'TF' THEN 'function'  WHEN 'AF' THEN 'function'
                              ELSE 'function' END AS type
           FROM [${escapeMssqlId(database)}].sys.objects o
           JOIN [${escapeMssqlId(database)}].sys.schemas s ON s.schema_id = o.schema_id
           WHERE s.name = '${esc(schema)}' AND o.type IN ('P','PC','FN','IF','TF','AF')
           ORDER BY o.name`,
        );
      for (const row of routineRes.recordset) {
        objects.push({
          schema,
          name: row.name,
          type: row.type as TableInfo["type"],
        });
      }
    } catch {}
    return objects;
  }

  async describeTable(
    database: string,
    schema: string,
    table: string,
  ): Promise<ColumnMeta[]> {
    const esc = (s: string) => s.replace(/'/g, "''");
    const res = await this.requirePool()
      .request()
      .query<DescribeColumnRow>(
        `SELECT
           c.name                                                AS COLUMN_NAME,
           TYPE_NAME(c.user_type_id)                            AS DATA_TYPE,
           c.max_length,
           c.precision,
           c.scale,
           c.is_nullable                                        AS IS_NULLABLE,
           c.is_identity,
           OBJECT_DEFINITION(c.default_object_id)               AS COLUMN_DEFAULT,
           CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END AS IS_PK,
           pk.key_ordinal                                       AS PK_ORDINAL,
           CASE WHEN fk.parent_column_id IS NOT NULL THEN 1 ELSE 0 END AS IS_FK
         FROM [${escapeMssqlId(database)}].sys.columns c
         JOIN [${escapeMssqlId(database)}].sys.objects  o ON o.object_id = c.object_id
         JOIN [${escapeMssqlId(database)}].sys.schemas  s ON s.schema_id  = o.schema_id
         LEFT JOIN (
           SELECT ic.object_id, ic.column_id, ic.key_ordinal
           FROM [${escapeMssqlId(database)}].sys.index_columns ic
           JOIN [${escapeMssqlId(database)}].sys.indexes       i
             ON i.object_id = ic.object_id AND i.index_id = ic.index_id
           WHERE i.is_primary_key = 1
         ) pk ON pk.object_id = c.object_id AND pk.column_id = c.column_id
         LEFT JOIN (
           SELECT DISTINCT fkc.parent_object_id, fkc.parent_column_id
           FROM [${escapeMssqlId(database)}].sys.foreign_key_columns fkc
         ) fk ON fk.parent_object_id = c.object_id AND fk.parent_column_id = c.column_id
         WHERE s.name = '${esc(schema)}' AND o.name = '${esc(table)}'
         ORDER BY c.column_id`,
      );
    return res.recordset.map((row) => ({
      name: row.COLUMN_NAME,
      type: mssqlFullType(
        row.DATA_TYPE,
        row.max_length,
        row.precision,
        row.scale,
      ),
      nullable: isSetFlag(row.IS_NULLABLE),
      defaultValue:
        row.COLUMN_DEFAULT != null
          ? cleanMssqlDefault(row.COLUMN_DEFAULT)
          : undefined,
      isPrimaryKey: row.IS_PK === 1,
      primaryKeyOrdinal: row.PK_ORDINAL ?? undefined,
      isForeignKey: row.IS_FK === 1,
      isAutoIncrement: isSetFlag(row.is_identity),
    }));
  }

  async query(sql: string, params?: unknown[]): Promise<QueryResult> {
    const start = Date.now();

    const batches = sql
      .split(/\r?\n/)
      .reduce<string[]>(
        (acc, line) => {
          const isGo = /^GO(?:\s+\d+)?$/i.test(line.trim());

          if (isGo) {
            acc.push("");
          } else {
            const lastIdx = acc.length - 1;
            acc[lastIdx] += (acc[lastIdx] ? "\n" : "") + line;
          }
          return acc;
        },
        [""],
      )
      .map((b) => b.trim())
      .filter((b) => b.length > 0);

    if (batches.length === 0) {
      return {
        columns: [],
        rows: [],
        rowCount: 0,
        executionTimeMs: Date.now() - start,
      };
    }

    if (batches.length > 1 && params && params.length > 0) {
      throw new Error(
        "[RapiDB] MSSQL:parameters are not supported in multi-batch scripts (GO). " +
          "Use parameters only in single queries without the GO separator.",
      );
    }

    let lastResult: QueryResult = {
      columns: [],
      rows: [],
      rowCount: 0,
      executionTimeMs: 0,
    };

    for (const batch of batches) {
      const currentParams = batches.length === 1 ? params : undefined;
      lastResult = await this._executeBatch(batch, currentParams, start);
    }

    lastResult.executionTimeMs = Date.now() - start;
    return lastResult;
  }

  private async _executeBatch(
    sql: string,
    params?: unknown[],
    start = Date.now(),
  ): Promise<QueryResult> {
    const req = this.requirePool().request();
    req.arrayRowMode = true;

    const finalSql = this.bindPositionalParameters(req, sql, params);
    const res = (await req.query(finalSql)) as MssqlArrayResult;
    const executionTimeMs = Date.now() - start;
    const columnsMeta = res.columns?.[0] ?? [];
    const affectedRows = res.rowsAffected.at(-1) ?? 0;

    if (columnsMeta.length === 0) {
      return {
        columns: [],
        rows: [],
        rowCount: affectedRows,
        affectedRows,
        executionTimeMs,
      };
    }

    const columns = columnsMeta.map((column) =>
      column.name === "" ? " " : column.name,
    );
    const rows = ((res.recordset ?? []) as unknown[][]).map((row) =>
      Object.fromEntries(
        row.map((value, index) => [
          `__col_${index}`,
          this.formatQueryValue(value, columnsMeta[index]),
        ]),
      ),
    );

    return {
      columns,
      rows,
      rowCount: rows.length,
      affectedRows,
      executionTimeMs,
    };
  }

  async getIndexes(
    database: string,
    schema: string,
    table: string,
  ): Promise<import("./types").IndexMeta[]> {
    const esc = (s: string) => s.replace(/'/g, "''");
    const res = await this.requirePool()
      .request()
      .query<IndexRow>(
        `SELECT i.name AS idx_name, c.name AS col_name,
                i.is_unique AS is_unique, i.is_primary_key AS is_pk
         FROM [${escapeMssqlId(database)}].sys.indexes i
         JOIN [${escapeMssqlId(database)}].sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
         JOIN [${escapeMssqlId(database)}].sys.columns c ON c.object_id = i.object_id AND c.column_id = ic.column_id
         JOIN [${escapeMssqlId(database)}].sys.objects o ON o.object_id = i.object_id
         JOIN [${escapeMssqlId(database)}].sys.schemas s ON s.schema_id = o.schema_id
         WHERE s.name = '${esc(schema)}' AND o.name = '${esc(table)}'
         ORDER BY i.name, ic.key_ordinal`,
      );
    const map = new Map<string, import("./types").IndexMeta>();
    for (const row of res.recordset) {
      if (!map.has(row.idx_name)) {
        map.set(row.idx_name, {
          name: row.idx_name,
          columns: [],
          unique: isSetFlag(row.is_unique),
          primary: isSetFlag(row.is_pk),
        });
      }
      const entry = map.get(row.idx_name);
      if (entry) {
        entry.columns.push(row.col_name);
      }
    }
    return [...map.values()];
  }

  async getForeignKeys(
    database: string,
    schema: string,
    table: string,
  ): Promise<import("./types").ForeignKeyMeta[]> {
    const esc = (s: string) => s.replace(/'/g, "''");
    const res = await this.requirePool()
      .request()
      .query<ForeignKeyRow>(
        `SELECT fk.name AS constraint_name,
                pc.name AS column_name,
                rs.name AS ref_schema,
                ro.name AS ref_table,
                rc.name AS ref_column
         FROM [${escapeMssqlId(database)}].sys.foreign_keys fk
         JOIN [${escapeMssqlId(database)}].sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
         JOIN [${escapeMssqlId(database)}].sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
         JOIN [${escapeMssqlId(database)}].sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
         JOIN [${escapeMssqlId(database)}].sys.objects ro ON ro.object_id = fkc.referenced_object_id
         JOIN [${escapeMssqlId(database)}].sys.schemas rs ON rs.schema_id = ro.schema_id
         JOIN [${escapeMssqlId(database)}].sys.objects po ON po.object_id = fkc.parent_object_id
         JOIN [${escapeMssqlId(database)}].sys.schemas ps ON ps.schema_id = po.schema_id
         WHERE ps.name = '${esc(schema)}' AND po.name = '${esc(table)}'`,
      );
    return res.recordset.map((row) => ({
      constraintName: row.constraint_name,
      column: row.column_name,
      referencedSchema: row.ref_schema,
      referencedTable: row.ref_table,
      referencedColumn: row.ref_column,
    }));
  }

  async getCreateTableDDL(
    database: string,
    schema: string,
    table: string,
  ): Promise<string> {
    const esc = (s: string) => s.replace(/'/g, "''");
    const cols = await this.requirePool()
      .request()
      .query<DdlColumnRow>(
        `SELECT
           c.name                              AS COLUMN_NAME,
           TYPE_NAME(c.user_type_id)           AS DATA_TYPE,
           c.max_length,
           c.precision,
           c.scale,
           c.is_nullable                       AS IS_NULLABLE,
           c.is_identity,
           OBJECT_DEFINITION(c.default_object_id) AS COLUMN_DEFAULT,
           CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END AS IS_PK,
           pk.key_ordinal                      AS PK_ORDINAL
         FROM [${escapeMssqlId(database)}].sys.columns c
         JOIN [${escapeMssqlId(database)}].sys.objects  o ON o.object_id = c.object_id
         JOIN [${escapeMssqlId(database)}].sys.schemas  s ON s.schema_id  = o.schema_id
         LEFT JOIN (
           SELECT ic.object_id, ic.column_id, ic.key_ordinal
           FROM [${escapeMssqlId(database)}].sys.index_columns ic
           JOIN [${escapeMssqlId(database)}].sys.indexes       i
             ON i.object_id = ic.object_id AND i.index_id = ic.index_id
           WHERE i.is_primary_key = 1
         ) pk ON pk.object_id = c.object_id AND pk.column_id = c.column_id
         WHERE s.name = '${esc(schema)}' AND o.name = '${esc(table)}'
         ORDER BY c.column_id`,
      );

    const pkCols = cols.recordset
      .filter((row) => row.IS_PK === 1)
      .sort(
        (left, right) =>
          (left.PK_ORDINAL ?? Number.MAX_SAFE_INTEGER) -
          (right.PK_ORDINAL ?? Number.MAX_SAFE_INTEGER),
      )
      .map((row) => this.quoteIdentifier(row.COLUMN_NAME));

    const colDefs = cols.recordset.map((row) => {
      const typ = mssqlFullType(
        row.DATA_TYPE,
        row.max_length,
        row.precision,
        row.scale,
      );
      const nullable = isSetFlag(row.IS_NULLABLE) ? "" : " NOT NULL";
      const identity = isSetFlag(row.is_identity) ? " IDENTITY(1,1)" : "";
      const def = row.COLUMN_DEFAULT ? ` DEFAULT ${row.COLUMN_DEFAULT}` : "";
      const pk = pkCols.length === 1 && row.IS_PK === 1 ? " PRIMARY KEY" : "";
      return `  ${this.quoteIdentifier(row.COLUMN_NAME)} ${typ}${identity}${nullable}${def}${pk}`;
    });

    if (pkCols.length > 1) {
      colDefs.push(`  PRIMARY KEY (${pkCols.join(", ")})`);
    }

    return `CREATE TABLE ${this.qualifiedTableName(database, schema, table)} (\n${colDefs.join(",\n")}\n);`;
  }

  async getRoutineDefinition(
    database: string,
    schema: string,
    name: string,
    _kind: "function" | "procedure",
  ): Promise<string> {
    const res = await this.requirePool()
      .request()
      .query<RoutineDefinitionRow>(
        `SELECT OBJECT_DEFINITION(OBJECT_ID('[${escapeMssqlId(database)}].[${escapeMssqlId(schema)}].[${escapeMssqlId(name)}]')) AS def`,
      );
    const def = res.recordset[0]?.def ?? null;
    return def ?? `-- Definition not available for [${schema}].[${name}]`;
  }

  async runTransaction(
    operations: import("./types").TransactionOperation[],
  ): Promise<void> {
    const tx = new mssql.Transaction(this.requirePool());
    await tx.begin();
    try {
      for (const op of operations) {
        const req = tx.request();
        const finalSql = this.bindPositionalParameters(req, op.sql, op.params);
        const res = await req.query(finalSql);
        if (op.checkAffectedRows && (res.rowsAffected?.[0] ?? 0) === 0) {
          throw new Error(
            "Row not found — the row may have been modified or deleted by another user",
          );
        }
      }
      await tx.commit();
    } catch (e) {
      try {
        await tx.rollback();
      } catch {}
      throw e;
    }
  }

  mapTypeCategory(nativeType: string): TypeCategory {
    const ct = baseTypeName(nativeType);
    if (ct === "bit") return "boolean";
    if (["tinyint", "smallint", "int", "bigint"].includes(ct)) return "integer";
    if (["real", "float"].includes(ct)) return "float";
    if (["decimal", "numeric", "money", "smallmoney"].includes(ct))
      return "decimal";
    if (ct === "date") return "date";
    if (ct === "time") return "time";
    if (
      ["datetime", "datetime2", "datetimeoffset", "smalldatetime"].includes(ct)
    ) {
      return "datetime";
    }
    if (ct === "timestamp" || ct === "rowversion") return "binary";
    if (["binary", "varbinary", "image"].includes(ct)) return "binary";
    if (ct === "uniqueidentifier") return "uuid";
    if (["text", "ntext", "xml"].includes(ct)) return "text";
    if (["geography", "geometry"].includes(ct)) return "spatial";
    if (ct === "hierarchyid" || ct === "sql_variant") return "other";
    if (ct.includes("char") || ct.includes("varchar")) return "text";
    return "other";
  }

  isBooleanType(nativeType: string): boolean {
    return baseTypeName(nativeType) === "bit";
  }

  isDatetimeWithTime(nativeType: string): boolean {
    const ct = baseTypeName(nativeType);
    return [
      "datetime",
      "datetime2",
      "datetimeoffset",
      "smalldatetime",
    ].includes(ct);
  }

  protected override isFilterable(
    nativeType: string,
    category: TypeCategory,
  ): boolean {
    return (
      super.isFilterable(nativeType, category) &&
      !MSSQL_NON_FILTERABLE.has(baseTypeName(nativeType))
    );
  }

  protected override isEditable(
    nativeType: string,
    category: TypeCategory,
  ): boolean {
    return (
      super.isEditable(nativeType, category) &&
      !MSSQL_NON_EDITABLE.has(baseTypeName(nativeType))
    );
  }

  override quoteIdentifier(name: string): string {
    return `[${name.replace(/]/g, "]]")}]`;
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
    _paramIndex: number,
  ): PaginationResult {
    return {
      sql: `OFFSET ? ROWS FETCH NEXT ? ROWS ONLY`,
      params: [offset, limit],
    };
  }

  override buildInsertValueExpr(
    column: ColumnTypeMeta,
    _paramIndex: number,
  ): string {
    if (column.category === "decimal") {
      return `CAST(? AS ${column.nativeType})`;
    }
    return "?";
  }

  override buildSetExpr(column: ColumnTypeMeta, _paramIndex: number): string {
    const expr = this.buildInsertValueExpr(column, _paramIndex);
    return `${this.quoteIdentifier(column.name)} = ${expr}`;
  }

  override buildOrderByDefault(columns: ColumnTypeMeta[]): string {
    const pkCols = columns
      .filter((column) => column.isPrimaryKey)
      .sort(
        (left, right) =>
          (left.primaryKeyOrdinal ?? Number.MAX_SAFE_INTEGER) -
          (right.primaryKeyOrdinal ?? Number.MAX_SAFE_INTEGER),
      );
    if (pkCols.length > 0) {
      return `ORDER BY ${pkCols.map((column) => this.quoteIdentifier(column.name)).join(", ")}`;
    }
    if (columns.length > 0) {
      return `ORDER BY ${this.quoteIdentifier(columns[0].name)}`;
    }
    return "ORDER BY (SELECT NULL)";
  }

  override coerceInputValue(value: unknown, column: ColumnTypeMeta): unknown {
    return this.normalizeInputValue(value, column);
  }

  override formatOutputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (value === null || value === undefined) return value;
    if (Buffer.isBuffer(value)) return super.formatOutputValue(value, column);
    if (typeof value === "bigint") return value.toString();
    if (value instanceof Date && !Number.isNaN(value.getTime())) {
      const ct = baseTypeName(column.nativeType);
      const pad = (n: number) => String(n).padStart(2, "0");
      if (ct === "date") {
        return `${value.getUTCFullYear()}-${pad(value.getUTCMonth() + 1)}-${pad(value.getUTCDate())}`;
      }
      if (ct === "time") {
        const ms = value.getUTCMilliseconds();
        const frac =
          ms > 0 ? `.${String(ms).padStart(3, "0").replace(/0+$/, "")}` : "";
        return `${pad(value.getUTCHours())}:${pad(value.getUTCMinutes())}:${pad(value.getUTCSeconds())}${frac}`;
      }
      if (ct === "datetimeoffset") {
        const ms = value.getUTCMilliseconds();
        const frac =
          ms > 0 ? `.${String(ms).padStart(3, "0").replace(/0+$/, "")}` : "";
        return `${value.getUTCFullYear()}-${pad(value.getUTCMonth() + 1)}-${pad(value.getUTCDate())} ${pad(value.getUTCHours())}:${pad(value.getUTCMinutes())}:${pad(value.getUTCSeconds())}${frac} +00:00`;
      }
      const formatted = formatDatetimeForDisplay(value);
      if (formatted !== null) return formatted;
    }
    return value;
  }

  override buildFilterCondition(
    column: ColumnTypeMeta,
    operator: FilterOperator,
    value: string | [string, string] | undefined,
    _paramIndex: number,
  ): FilterConditionResult | null {
    const col = this.quoteIdentifier(column.name);

    if (operator === "is_null") return { sql: `${col} IS NULL`, params: [] };
    if (operator === "is_not_null") {
      return { sql: `${col} IS NOT NULL`, params: [] };
    }

    if (!column.filterable || value === undefined) return null;

    const val = typeof value === "string" ? value.trim() : value;

    if (column.isBoolean && (operator === "eq" || operator === "neq")) {
      const strVal = (typeof val === "string" ? val : val[0]).toLowerCase();
      if (strVal === "true" || strVal === "false") {
        const boolVal = strVal === "true" ? 1 : 0;
        const sqlOp = operator === "neq" ? "<>" : "=";
        return { sql: `${col} ${sqlOp} ?`, params: [boolVal] };
      }
    }

    if (column.category === "binary") {
      const v = typeof val === "string" ? val : val[0];
      const hexVal = v.replace(/^(0x|\\x)/i, "").toUpperCase();
      return {
        sql: `CONVERT(VARCHAR(MAX), ${col}, 2) LIKE ?`,
        params: [`%${hexVal}%`],
      };
    }

    if (this.isNumericCategory(column.category) && Array.isArray(val)) {
      return {
        sql: `${col} BETWEEN ? AND ?`,
        params: [Number(val[0]), Number(val[1])],
      };
    }

    if (this.isNumericCategory(column.category) && typeof val === "string") {
      if (operator === "in") {
        const parts = val.split(",").map((part) => Number(part.trim()));
        return {
          sql: `${col} IN (${parts.map(() => "?").join(", ")})`,
          params: parts,
        };
      }

      if (!Number.isNaN(Number(val)) && val !== "") {
        const sqlOp = this.sqlOperator(operator);
        return { sql: `${col} ${sqlOp} ?`, params: [Number(val)] };
      }
    }

    if (column.category === "date") {
      const v = typeof val === "string" ? val : val[0];
      if (Array.isArray(val)) {
        return {
          sql: `CONVERT(date, ${col}) BETWEEN ? AND ?`,
          params: [val[0], val[1]],
        };
      }
      if (operator === "in") {
        const parts = v.split(",").map((part) => part.trim());
        return {
          sql: `CONVERT(date, ${col}) IN (${parts.map(() => "?").join(", ")})`,
          params: parts,
        };
      }

      if (["eq", "neq", "gt", "gte", "lt", "lte"].includes(operator)) {
        const sqlOp = operator === "neq" ? "<>" : this.sqlOperator(operator);
        return { sql: `CONVERT(date, ${col}) ${sqlOp} ?`, params: [v] };
      }
      return {
        sql: `CONVERT(CHAR(10), ${col}, 23) LIKE ?`,
        params: [`%${v}%`],
      };
    }

    if (column.category === "time") {
      const v = typeof val === "string" ? val : val[0];
      if (Array.isArray(val)) {
        return { sql: `${col} BETWEEN ? AND ?`, params: [val[0], val[1]] };
      }
      if (operator === "in") {
        const parts = v.split(",").map((part) => part.trim());
        return {
          sql: `${col} IN (${parts.map(() => "?").join(", ")})`,
          params: parts,
        };
      }
      return {
        sql: `CONVERT(VARCHAR(16), ${col}, 114) LIKE ?`,
        params: [`%${v}%`],
      };
    }

    if (operator === "between" && Array.isArray(val)) {
      return { sql: `${col} BETWEEN ? AND ?`, params: [val[0], val[1]] };
    }

    if (operator === "in" && typeof val === "string") {
      const parts = val.split(",").map((part) => part.trim());
      return {
        sql: `${col} IN (${parts.map(() => "?").join(", ")})`,
        params: parts,
      };
    }

    if (this.isDatetimeWithTime(column.nativeType)) {
      const v = typeof val === "string" ? val : val[0];
      if (["eq", "neq", "gt", "gte", "lt", "lte"].includes(operator)) {
        const sqlOp = operator === "neq" ? "<>" : this.sqlOperator(operator);
        return {
          sql: `${col} ${sqlOp} ?`,
          params: [normalizeDatetimeLiteral(v)],
        };
      }
      return {
        sql:
          baseTypeName(column.nativeType) === "datetimeoffset"
            ? `CONVERT(VARCHAR(40), ${col}, 127) LIKE ?`
            : `CONVERT(VARCHAR(33), ${col}, 126) LIKE ?`,
        params: [`%${temporalSearchLiteral(v)}%`],
      };
    }

    const v = typeof val === "string" ? val : val[0];
    if (operator === "eq" || operator === "neq") {
      const sqlOp = operator === "neq" ? "<>" : "=";
      return { sql: `${col} ${sqlOp} ?`, params: [v] };
    }
    return {
      sql: `CAST(${col} AS NVARCHAR(MAX)) LIKE ?`,
      params: [`%${v}%`],
    };
  }
}
