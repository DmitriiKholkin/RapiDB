import * as mssql from "mssql";
import type { ConnectionConfig } from "../connectionManager";
import { BaseDBDriver, formatDatetimeForDisplay } from "./BaseDBDriver";
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
import { DATETIME_SQL_RE, ISO_DATETIME_RE, NULL_SENTINEL } from "./types";

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

const MSSQL_NON_FILTERABLE = new Set([
  "image",
  "text",
  "ntext",
  "xml",
  "geography",
  "geometry",
  "hierarchyid",
  "sql_variant",
]);

export class MSSQLDriver extends BaseDBDriver {
  private pool: mssql.ConnectionPool | null = null;
  private readonly config: ConnectionConfig;

  constructor(config: ConnectionConfig) {
    super();
    this.config = config;
  }

  async connect(): Promise<void> {
    if (this.pool !== null) {
      try {
        await this.pool.close();
      } catch {}
      this.pool = null;
    }
    const sslEnabled = this.config.ssl ?? true;
    const trustCert = !(this.config.rejectUnauthorized ?? true);
    this.pool = await mssql.connect({
      server: this.config.host!,
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
    });
  }

  async disconnect(): Promise<void> {
    await this.pool?.close();
    this.pool = null;
  }

  isConnected(): boolean {
    return this.pool?.connected ?? false;
  }

  async listDatabases(): Promise<DatabaseInfo[]> {
    const res = await this.pool!.request().query(
      `SELECT name FROM sys.databases WHERE state_desc = 'ONLINE' ORDER BY name`,
    );
    return res.recordset.map((r: any) => ({
      name: r.name as string,
      schemas: [],
    }));
  }

  async listSchemas(database: string): Promise<SchemaInfo[]> {
    const res = await this.pool!.request().query(
      `SELECT SCHEMA_NAME AS name FROM [${escapeMssqlId(database)}].INFORMATION_SCHEMA.SCHEMATA
       WHERE SCHEMA_NAME NOT IN ('sys','INFORMATION_SCHEMA','db_accessadmin','db_backupoperator',
         'db_datareader','db_datawriter','db_ddladmin','db_denydatareader','db_denydatawriter',
         'db_owner','db_securityadmin','guest')
       ORDER BY SCHEMA_NAME`,
    );
    return res.recordset.map((r: any) => ({ name: r.name as string }));
  }

  async listObjects(database: string, schema: string): Promise<TableInfo[]> {
    const objects: TableInfo[] = [];
    const esc = (s: string) => s.replace(/'/g, "''");
    const tableRes = await this.pool!.request().query(
      `SELECT TABLE_NAME AS name, TABLE_TYPE AS type
       FROM [${escapeMssqlId(database)}].INFORMATION_SCHEMA.TABLES
       WHERE TABLE_SCHEMA = '${esc(schema)}' ORDER BY TABLE_NAME`,
    );
    for (const r of tableRes.recordset) {
      objects.push({
        schema,
        name: r.name as string,
        type: ((r.type as string).includes("VIEW")
          ? "view"
          : "table") as TableInfo["type"],
      });
    }
    try {
      const routineRes = await this.pool!.request().query(
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
      for (const r of routineRes.recordset) {
        objects.push({
          schema,
          name: r.name as string,
          type: r.type as TableInfo["type"],
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
    const res = await this.pool!.request().query(
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
         CASE WHEN fk.parent_column_id IS NOT NULL THEN 1 ELSE 0 END AS IS_FK
       FROM [${escapeMssqlId(database)}].sys.columns c
       JOIN [${escapeMssqlId(database)}].sys.objects  o ON o.object_id = c.object_id
       JOIN [${escapeMssqlId(database)}].sys.schemas  s ON s.schema_id  = o.schema_id
       LEFT JOIN (
         SELECT ic.object_id, ic.column_id
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
    return res.recordset.map((r: any) => ({
      name: r.COLUMN_NAME as string,
      type: mssqlFullType(r.DATA_TYPE, r.max_length, r.precision, r.scale),
      nullable: r.IS_NULLABLE === true || r.IS_NULLABLE === 1,
      defaultValue:
        r.COLUMN_DEFAULT != null
          ? cleanMssqlDefault(r.COLUMN_DEFAULT as string)
          : undefined,
      isPrimaryKey: r.IS_PK === 1,
      isForeignKey: r.IS_FK === 1,
      isAutoIncrement: r.is_identity === true || r.is_identity === 1,
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
    let finalSql = sql;
    const req = this.pool!.request();
    if (params && params.length > 0) {
      const placeholderCount = (sql.match(/\?/g) ?? []).length;
      if (placeholderCount !== params.length) {
        throw new Error(
          `[RapiDB] MSSQL parameter mismatch: SQL has ${placeholderCount} placeholder(s) but ${params.length} value(s) were supplied.`,
        );
      }
      let idx = 0;
      finalSql = sql.replace(/\?/g, () => {
        const name = `p${++idx}`;
        req.input(name, this.guessMssqlType(params[idx - 1]), params[idx - 1]);
        return `@${name}`;
      });
    }
    const res = await req.query(finalSql);
    const executionTimeMs = Date.now() - start;

    const columnEntries = Object.entries(
      (res.recordset as any)?.columns ?? {},
    ) as [string, any][];

    if (columnEntries.length === 0 && (res.rowsAffected?.[0] ?? 0) > 0) {
      return {
        columns: [],
        rows: [],
        rowCount: res.rowsAffected?.[0] ?? 0,
        executionTimeMs,
      };
    }

    const pad = (n: number) => String(n).padStart(2, "0");

    const columns = columnEntries.map(([name]) => (name === "" ? " " : name));
    const rows = (res.recordset ?? []).map((row: any) =>
      Object.fromEntries(
        columnEntries.map(([name, colMeta], i) => {
          let v = row[name];
          const typeName: string = (colMeta as any)?.type?.name ?? "";

          if (
            typeName === "Real" &&
            typeof v === "number" &&
            !Number.isInteger(v)
          ) {
            v = parseFloat((v as number).toPrecision(7));
          }

          if (
            typeName === "Date" &&
            v instanceof Date &&
            !isNaN((v as Date).getTime())
          ) {
            const d = v as Date;
            v = `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`;
          }

          if (
            typeName === "Time" &&
            v instanceof Date &&
            !isNaN((v as Date).getTime())
          ) {
            const d = v as Date;
            const ms = d.getUTCMilliseconds();
            const frac =
              ms > 0
                ? `.${String(ms).padStart(3, "0").replace(/0+$/, "")}`
                : "";
            v = `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}${frac}`;
          }

          if (
            typeName === "DateTimeOffset" &&
            v instanceof Date &&
            !isNaN((v as Date).getTime())
          ) {
            const d = v as Date;
            const ms = d.getUTCMilliseconds();
            const frac =
              ms > 0
                ? `.${String(ms).padStart(3, "0").replace(/0+$/, "")}`
                : "";
            v =
              `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ` +
              `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}${frac} +00:00`;
          }

          return [`__col_${i}`, v];
        }),
      ),
    );
    return {
      columns,
      rows,
      rowCount: res.rowsAffected?.[0] ?? rows.length,
      executionTimeMs,
    };
  }

  private guessMssqlType(value: any) {
    if (Buffer.isBuffer(value)) return mssql.TYPES.VarBinary;
    if (typeof value === "bigint") return mssql.TYPES.BigInt;
    if (typeof value === "number") {
      return Number.isInteger(value) ? mssql.TYPES.Int : mssql.TYPES.Float;
    }
    if (typeof value === "boolean") return mssql.TYPES.Bit;
    if (value instanceof Date) return mssql.TYPES.DateTime2;
    if (typeof value === "string") {
      if (ISO_DATETIME_RE.test(value)) return mssql.TYPES.DateTimeOffset;
      if (DATETIME_SQL_RE.test(value)) return mssql.TYPES.DateTime2;
      if (/^\d{4}-\d{2}-\d{2}$/.test(value)) return mssql.TYPES.Date;
      if (/^\d{2}:\d{2}:\d{2}(\.\d+)?$/.test(value)) return mssql.TYPES.Time;
    }
    return mssql.TYPES.NVarChar;
  }

  async getIndexes(
    database: string,
    schema: string,
    table: string,
  ): Promise<import("./types").IndexMeta[]> {
    const esc = (s: string) => s.replace(/'/g, "''");
    const res = await this.pool!.request().query(
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
    for (const r of res.recordset) {
      if (!map.has(r.idx_name)) {
        map.set(r.idx_name, {
          name: r.idx_name,
          columns: [],
          unique: !!r.is_unique,
          primary: !!r.is_pk,
        });
      }
      map.get(r.idx_name)!.columns.push(r.col_name);
    }
    return [...map.values()];
  }

  async getForeignKeys(
    database: string,
    schema: string,
    table: string,
  ): Promise<import("./types").ForeignKeyMeta[]> {
    const esc = (s: string) => s.replace(/'/g, "''");
    const res = await this.pool!.request().query(
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
    return res.recordset.map((r: any) => ({
      constraintName: r.constraint_name,
      column: r.column_name,
      referencedSchema: r.ref_schema,
      referencedTable: r.ref_table,
      referencedColumn: r.ref_column,
    }));
  }

  async getCreateTableDDL(
    database: string,
    schema: string,
    table: string,
  ): Promise<string> {
    const esc = (s: string) => s.replace(/'/g, "''");
    try {
      const res = await this.pool!.request().query(
        `SELECT OBJECT_DEFINITION(OBJECT_ID('[${escapeMssqlId(database)}].[${escapeMssqlId(schema)}].[${escapeMssqlId(table)}]')) AS ddl`,
      );
      if (res.recordset[0]?.ddl) {
        return res.recordset[0].ddl as string;
      }
    } catch {}

    const cols = await this.pool!.request().query(
      `SELECT
         c.name                              AS COLUMN_NAME,
         TYPE_NAME(c.user_type_id)           AS DATA_TYPE,
         c.max_length,
         c.precision,
         c.scale,
         c.is_nullable                       AS IS_NULLABLE,
         c.is_identity,
         OBJECT_DEFINITION(c.default_object_id) AS COLUMN_DEFAULT,
         CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END AS IS_PK
       FROM [${escapeMssqlId(database)}].sys.columns c
       JOIN [${escapeMssqlId(database)}].sys.objects  o ON o.object_id = c.object_id
       JOIN [${escapeMssqlId(database)}].sys.schemas  s ON s.schema_id  = o.schema_id
       LEFT JOIN (
         SELECT ic.object_id, ic.column_id
         FROM [${escapeMssqlId(database)}].sys.index_columns ic
         JOIN [${escapeMssqlId(database)}].sys.indexes       i
           ON i.object_id = ic.object_id AND i.index_id = ic.index_id
         WHERE i.is_primary_key = 1
       ) pk ON pk.object_id = c.object_id AND pk.column_id = c.column_id
       WHERE s.name = '${esc(schema)}' AND o.name = '${esc(table)}'
       ORDER BY c.column_id`,
    );

    const pkCols = cols.recordset
      .filter((r: any) => r.IS_PK === 1)
      .map((r: any) => `[${r.COLUMN_NAME}]`);
    const colDefs = cols.recordset.map((r: any) => {
      const typ = mssqlFullType(
        r.DATA_TYPE,
        r.max_length,
        r.precision,
        r.scale,
      );
      const nullable =
        r.IS_NULLABLE === true || r.IS_NULLABLE === 1 ? "" : " NOT NULL";
      const identity = r.is_identity ? " IDENTITY(1,1)" : "";
      const def = r.COLUMN_DEFAULT ? ` DEFAULT ${r.COLUMN_DEFAULT}` : "";
      const pk = pkCols.length === 1 && r.IS_PK === 1 ? " PRIMARY KEY" : "";
      return `  [${r.COLUMN_NAME}] ${typ}${identity}${nullable}${def}${pk}`;
    });

    if (pkCols.length > 1) {
      colDefs.push(`  PRIMARY KEY (${pkCols.join(", ")})`);
    }

    return `CREATE TABLE [${schema}].[${table}] (\n${colDefs.join(",\n")}\n);`;
  }

  async getRoutineDefinition(
    database: string,
    schema: string,
    name: string,
    _kind: "function" | "procedure",
  ): Promise<string> {
    const res = await this.pool!.request().query(
      `SELECT OBJECT_DEFINITION(OBJECT_ID('[${escapeMssqlId(database)}].[${escapeMssqlId(schema)}].[${escapeMssqlId(name)}]')) AS def`,
    );
    const def = (res.recordset[0] as Record<string, unknown>)?.def as
      | string
      | null;
    return def ?? `-- Definition not available for [${schema}].[${name}]`;
  }

  async runTransaction(
    operations: import("./types").TransactionOperation[],
  ): Promise<void> {
    const tx = new mssql.Transaction(this.pool!);
    await tx.begin();
    try {
      for (const op of operations) {
        let finalSql = op.sql;
        const req = tx.request();
        if (op.params && op.params.length > 0) {
          let idx = 0;
          finalSql = op.sql.replace(/\?/g, () => {
            const name = `p${++idx}`;
            req.input(
              name,
              this.guessMssqlType(op.params![idx - 1]),
              op.params![idx - 1],
            );
            return `@${name}`;
          });
        }
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

  // ─── MSSQL type system ───

  mapTypeCategory(nativeType: string): TypeCategory {
    const ct = nativeType.toLowerCase().split("(")[0].trim();
    if (ct === "bit") return "boolean";
    if (["tinyint", "smallint", "int", "bigint"].includes(ct)) return "integer";
    if (["real", "float"].includes(ct)) return "float";
    if (["decimal", "numeric", "money", "smallmoney"].includes(ct))
      return "decimal";
    if (ct === "date") return "date";
    if (ct === "time") return "time";
    if (
      ["datetime", "datetime2", "datetimeoffset", "smalldatetime"].includes(ct)
    )
      return "datetime";
    if (["binary", "varbinary", "image"].includes(ct)) return "binary";
    if (ct === "uniqueidentifier") return "uuid";
    if (["text", "ntext", "xml"].includes(ct)) return "text";
    if (["geography", "geometry"].includes(ct)) return "spatial";
    if (ct === "hierarchyid" || ct === "sql_variant") return "other";
    // char, varchar, nchar, nvarchar
    if (ct.includes("char") || ct.includes("varchar")) return "text";
    return "other";
  }

  isBooleanType(nativeType: string): boolean {
    return nativeType.toLowerCase().split("(")[0].trim() === "bit";
  }

  isDatetimeWithTime(nativeType: string): boolean {
    const ct = nativeType.toLowerCase().split("(")[0].trim();
    return (
      ct === "datetime" ||
      ct === "datetime2" ||
      ct === "datetimeoffset" ||
      ct === "smalldatetime"
    );
  }

  override isFilterable(_nativeType: string): boolean {
    return !MSSQL_NON_FILTERABLE.has(
      _nativeType.toLowerCase().split("(")[0].trim(),
    );
  }

  // ─── MSSQL SQL helpers ───

  override quoteIdentifier(name: string): string {
    return `[${name.replace(/]/g, "]]")}]`;
  }

  override qualifiedTableName(
    database: string,
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
      sql: `OFFSET ? ROWS FETCH NEXT ? ROWS ONLY`,
      params: [offset, limit],
    };
  }

  override buildOrderByDefault(columns: ColumnTypeMeta[]): string {
    const pk = columns.find((c) => c.isPrimaryKey);
    if (pk) return `ORDER BY ${this.quoteIdentifier(pk.name)}`;
    if (columns.length > 0)
      return `ORDER BY ${this.quoteIdentifier(columns[0].name)}`;
    return "ORDER BY (SELECT NULL)";
  }

  // ─── MSSQL type-aware data helpers ───

  override coerceInputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (value === null || value === undefined || value === "") return value;
    if (value === NULL_SENTINEL) return null;
    if (typeof value !== "string") return value;

    if (column.isBoolean) {
      const lower = value.toLowerCase();
      if (lower === "true" || lower === "1") return true;
      if (lower === "false" || lower === "0") return false;
    }

    // Binary
    if (column.category === "binary")
      return super.coerceInputValue(value, column);

    // Time → Date object for mssql
    if (column.category === "time" && /^\d{2}:\d{2}/.test(value)) {
      const d = new Date(`1970-01-01T${value}Z`);
      if (!Number.isNaN(d.getTime())) return d;
    }

    // Date/datetime → ISO string
    if (ISO_DATETIME_RE.test(value) || DATETIME_SQL_RE.test(value)) {
      return value;
    }

    return value;
  }

  override formatOutputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (value === null || value === undefined) return value;
    if (Buffer.isBuffer(value)) return super.formatOutputValue(value, column);
    if (typeof value === "bigint") return value.toString();
    if (value instanceof Date && !Number.isNaN(value.getTime())) {
      const ct = column.nativeType.toLowerCase().split("(")[0].trim();
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

  // ─── MSSQL filter building ───

  override buildFilterCondition(
    column: ColumnTypeMeta,
    operator: FilterOperator,
    value: string | [string, string],
    paramIndex: number,
  ): FilterConditionResult | null {
    if (!column.filterable) return null;
    const col = this.quoteIdentifier(column.name);
    const val = typeof value === "string" ? value.trim() : value;

    if (operator === "is_null") return { sql: `${col} IS NULL`, params: [] };
    if (operator === "is_not_null")
      return { sql: `${col} IS NOT NULL`, params: [] };

    // Boolean
    if (column.isBoolean && (operator === "eq" || operator === "neq")) {
      const strVal = (typeof val === "string" ? val : val[0]).toLowerCase();
      if (strVal === "true" || strVal === "false") {
        const boolVal = strVal === "true" ? 1 : 0;
        const op = operator === "neq" ? "<>" : "=";
        return { sql: `${col} ${op} ?`, params: [boolVal] };
      }
    }

    // Binary: CONVERT(VARCHAR, col, 2) LIKE
    if (column.category === "binary") {
      const v = typeof val === "string" ? val : val[0];
      const hexVal = v.replace(/^(0x|\\x)/i, "").toUpperCase();
      return {
        sql: `CONVERT(VARCHAR(MAX), ${col}, 2) LIKE ?`,
        params: [`%${hexVal}%`],
      };
    }

    // Numeric
    if (
      this.isNumericCategory(column.category) &&
      typeof val === "string" &&
      !Number.isNaN(Number(val)) &&
      val !== ""
    ) {
      const sqlOp = this.sqlOperator(operator);
      return { sql: `${col} ${sqlOp} ?`, params: [Number(val)] };
    }

    if (column.category === "date") {
      const v = typeof val === "string" ? val : val[0];
      if (operator === "eq" || operator === "neq") {
        const sqlOp = operator === "neq" ? "<>" : "=";
        return { sql: `CONVERT(date, ${col}) ${sqlOp} ?`, params: [v] };
      }
      return {
        sql: `CONVERT(VARCHAR, ${col}, 23) LIKE ?`,
        params: [`%${v}%`],
      };
    }

    // Between
    if (operator === "between" && Array.isArray(val)) {
      return { sql: `${col} BETWEEN ? AND ?`, params: [val[0], val[1]] };
    }

    // In
    if (operator === "in" && typeof val === "string") {
      const parts = val.split(",").map((s) => s.trim());
      return {
        sql: `${col} IN (${parts.map(() => "?").join(", ")})`,
        params: parts,
      };
    }

    // Datetime
    if (this.isDatetimeWithTime(column.nativeType)) {
      const v = typeof val === "string" ? val : val[0];
      return {
        sql: `CONVERT(VARCHAR, ${col}, 120) LIKE ?`,
        params: [`%${v}%`],
      };
    }

    // Default text
    const v = typeof val === "string" ? val : val[0];
    return { sql: `${col} LIKE ?`, params: [`%${v}%`] };
  }

  override buildLegacyFilter(
    column: ColumnTypeMeta,
    rawValue: string,
    paramIndex: number,
  ): FilterConditionResult | null {
    const val = rawValue.trim();
    if (val === "") return null;
    if (val === NULL_SENTINEL)
      return this.buildFilterCondition(column, "is_null", val, paramIndex);

    if (column.isBoolean) {
      const lower = val.toLowerCase();
      if (lower === "true" || lower === "false") {
        return this.buildFilterCondition(column, "eq", val, paramIndex);
      }
    }

    if (
      !Number.isNaN(Number(val)) &&
      val !== "" &&
      !this.isTextualType(column.nativeType) &&
      !column.isBoolean
    ) {
      return this.buildFilterCondition(column, "eq", val, paramIndex);
    }

    return this.buildFilterCondition(column, "like", val, paramIndex);
  }

  private isTextualType(nativeType: string): boolean {
    const ct = nativeType.toLowerCase();
    return (
      ct.includes("char") ||
      ct.includes("varchar") ||
      ct.includes("text") ||
      ct.includes("xml")
    );
  }
}
