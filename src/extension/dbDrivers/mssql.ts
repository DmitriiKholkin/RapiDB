import * as mssql from "mssql";
import type { ConnectionConfig } from "../connectionManager";
import { ISO_DATETIME_RE } from "../tableDataService";
import type {
  ColumnMeta,
  DatabaseInfo,
  IDBDriver,
  QueryResult,
  SchemaInfo,
  TableInfo,
} from "./types";

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

export class MSSQLDriver implements IDBDriver {
  private pool: mssql.ConnectionPool | null = null;
  private readonly config: ConnectionConfig;

  constructor(config: ConnectionConfig) {
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
      `SELECT SCHEMA_NAME AS name FROM [${database}].INFORMATION_SCHEMA.SCHEMATA
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
       FROM [${database}].INFORMATION_SCHEMA.TABLES
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
         FROM [${database}].sys.objects o
         JOIN [${database}].sys.schemas s ON s.schema_id = o.schema_id
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
       FROM [${database}].sys.columns c
       JOIN [${database}].sys.objects  o ON o.object_id = c.object_id
       JOIN [${database}].sys.schemas  s ON s.schema_id  = o.schema_id
       LEFT JOIN (
         SELECT ic.object_id, ic.column_id
         FROM [${database}].sys.index_columns ic
         JOIN [${database}].sys.indexes       i
           ON i.object_id = ic.object_id AND i.index_id = ic.index_id
         WHERE i.is_primary_key = 1
       ) pk ON pk.object_id = c.object_id AND pk.column_id = c.column_id
       LEFT JOIN (
         SELECT DISTINCT fkc.parent_object_id, fkc.parent_column_id
         FROM [${database}].sys.foreign_key_columns fkc
       ) fk ON fk.parent_object_id = c.object_id AND fk.parent_column_id = c.column_id
       WHERE s.name = '${esc(schema)}' AND o.name = '${esc(table)}'
       ORDER BY c.column_id`,
    );
    return res.recordset.map((r: any) => ({
      name: r.COLUMN_NAME as string,
      type: mssqlFullType(r.DATA_TYPE, r.max_length, r.precision, r.scale),
      nullable: r.IS_NULLABLE === true || r.IS_NULLABLE === 1,
      defaultValue: r.COLUMN_DEFAULT ?? undefined,
      isPrimaryKey: r.IS_PK === 1,
      isForeignKey: r.IS_FK === 1,
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
    ) as [string, unknown][];

    if (columnEntries.length === 0 && (res.rowsAffected?.[0] ?? 0) > 0) {
      return {
        columns: [],
        rows: [],
        rowCount: res.rowsAffected?.[0] ?? 0,
        executionTimeMs,
      };
    }

    const columns = columnEntries.map(([name]) => (name === "" ? " " : name));
    const rows = (res.recordset ?? []).map((row: any) =>
      Object.fromEntries(
        columnEntries.map(([name], i) => [`__col_${i}`, row[name]]),
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
    if (typeof value === "number") return mssql.TYPES.Int;
    if (typeof value === "boolean") return mssql.TYPES.Bit;
    if (value instanceof Date) return mssql.TYPES.DateTime2;
    if (typeof value === "string" && ISO_DATETIME_RE.test(value)) {
      return mssql.TYPES.DateTimeOffset;
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
       FROM [${database}].sys.indexes i
       JOIN [${database}].sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
       JOIN [${database}].sys.columns c ON c.object_id = i.object_id AND c.column_id = ic.column_id
       JOIN [${database}].sys.objects o ON o.object_id = i.object_id
       JOIN [${database}].sys.schemas s ON s.schema_id = o.schema_id
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
       FROM [${database}].sys.foreign_keys fk
       JOIN [${database}].sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
       JOIN [${database}].sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
       JOIN [${database}].sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
       JOIN [${database}].sys.objects ro ON ro.object_id = fkc.referenced_object_id
       JOIN [${database}].sys.schemas rs ON rs.schema_id = ro.schema_id
       JOIN [${database}].sys.objects po ON po.object_id = fkc.parent_object_id
       JOIN [${database}].sys.schemas ps ON ps.schema_id = po.schema_id
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
        `SELECT OBJECT_DEFINITION(OBJECT_ID('[${database}].[${schema}].[${table}]')) AS ddl`,
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
       FROM [${database}].sys.columns c
       JOIN [${database}].sys.objects  o ON o.object_id = c.object_id
       JOIN [${database}].sys.schemas  s ON s.schema_id  = o.schema_id
       LEFT JOIN (
         SELECT ic.object_id, ic.column_id
         FROM [${database}].sys.index_columns ic
         JOIN [${database}].sys.indexes       i
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
      `SELECT OBJECT_DEFINITION(OBJECT_ID('[${database}].[${schema}].[${name}]')) AS def`,
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
}
