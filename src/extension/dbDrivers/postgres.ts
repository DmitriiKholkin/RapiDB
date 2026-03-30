import { Pool, types as pgTypes } from "pg";
import type { ConnectionConfig } from "../connectionManager";
import type {
  ColumnMeta,
  DatabaseInfo,
  IDBDriver,
  QueryResult,
  SchemaInfo,
  TableInfo,
} from "./types";

pgTypes.setTypeParser(1082, (val: string) => val);
pgTypes.setTypeParser(1114, (val: string) => val);
pgTypes.setTypeParser(1184, (val: string) => val);

export class PostgresDriver implements IDBDriver {
  private pool: Pool | null = null;
  private readonly config: ConnectionConfig;
  private _connected = false;

  constructor(config: ConnectionConfig) {
    this.config = config;
  }

  async connect(): Promise<void> {
    if (this.pool !== null) {
      try {
        await this.pool.end();
      } catch {}
      this.pool = null;
    }
    const sslEnabled = this.config.ssl ?? false;
    this.pool = new Pool({
      host: this.config.host,
      port: this.config.port,
      database: this.config.database,
      user: this.config.username,
      password: this.config.password,
      max: 5,
      keepAlive: true,
      keepAliveInitialDelayMillis: 60000,
      connectionTimeoutMillis: 10000,
      idleTimeoutMillis: 30000,
      ssl: sslEnabled
        ? {
            rejectUnauthorized: this.config.rejectUnauthorized ?? true,
          }
        : undefined,
    });
    this.pool.on("error", (err) => {
      console.error("[RapiDB] PostgreSQL pool error:", err.message);
      this._connected = false;
    });

    const client = await this.pool.connect();
    client.release();
    this._connected = true;
  }

  async disconnect(): Promise<void> {
    this._connected = false;
    await this.pool?.end();
    this.pool = null;
  }

  isConnected(): boolean {
    return this.pool !== null && this._connected;
  }

  async listDatabases(): Promise<DatabaseInfo[]> {
    const res = await this.pool!.query(`SELECT current_database() AS name`);
    const name =
      (res.rows[0]?.name as string) ?? this.config.database ?? "postgres";
    return [{ name, schemas: [] }];
  }

  async listSchemas(_database: string): Promise<SchemaInfo[]> {
    const res = await this.pool!.query(
      `SELECT schema_name FROM information_schema.schemata
       WHERE schema_name NOT IN ('information_schema','pg_catalog','pg_toast','pg_temp_1','pg_toast_temp_1')
       ORDER BY schema_name`,
    );
    return res.rows.map((r) => ({ name: r.schema_name as string }));
  }

  async listObjects(database: string, schema: string): Promise<TableInfo[]> {
    const objects: TableInfo[] = [];
    const tableRes = await this.pool!.query(
      `SELECT table_name AS name, table_type AS type
       FROM information_schema.tables WHERE table_schema = $1 ORDER BY table_name`,
      [schema],
    );
    for (const r of tableRes.rows) {
      objects.push({
        schema,
        name: r.name as string,
        type: (r.type === "VIEW" ? "view" : "table") as TableInfo["type"],
      });
    }
    try {
      const routineRes = await this.pool!.query(
        `SELECT p.proname AS name,
                CASE p.prokind WHEN 'f' THEN 'function' WHEN 'p' THEN 'procedure'
                               WHEN 'a' THEN 'function'  ELSE 'function' END AS type
         FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = $1 AND p.prokind IN ('f','p','a') ORDER BY p.proname`,
        [schema],
      );
      for (const r of routineRes.rows) {
        objects.push({
          schema,
          name: r.name as string,
          type: r.type as TableInfo["type"],
        });
      }
    } catch {
      try {
        const routineRes = await this.pool!.query(
          `SELECT routine_name AS name, routine_type AS type
           FROM information_schema.routines WHERE routine_schema = $1 ORDER BY routine_name`,
          [schema],
        );
        for (const r of routineRes.rows) {
          objects.push({
            schema,
            name: r.name as string,
            type: (r.type === "PROCEDURE"
              ? "procedure"
              : "function") as TableInfo["type"],
          });
        }
      } catch {}
    }
    return objects;
  }

  async describeTable(
    _database: string,
    schema: string,
    table: string,
  ): Promise<ColumnMeta[]> {
    const res = await this.pool!.query(
      `SELECT
         a.attname                                AS column_name,
         format_type(a.atttypid, a.atttypmod)    AS data_type,
         NOT a.attnotnull                         AS is_nullable,
         pg_get_expr(d.adbin, d.adrelid)         AS column_default,
         EXISTS (
           SELECT 1 FROM pg_constraint con
           WHERE con.conrelid = a.attrelid
             AND con.contype = 'p'
             AND a.attnum = ANY(con.conkey)
         ) AS is_pk,
         EXISTS (
           SELECT 1 FROM pg_constraint con
           WHERE con.conrelid = a.attrelid
             AND con.contype = 'f'
             AND a.attnum = ANY(con.conkey)
         ) AS is_fk
       FROM pg_attribute a
       JOIN pg_class     c ON c.oid = a.attrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
       LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
       WHERE n.nspname = $1
         AND c.relname = $2
         AND a.attnum > 0
         AND NOT a.attisdropped
       ORDER BY a.attnum`,
      [schema, table],
    );
    return res.rows.map((r) => ({
      name: r.column_name as string,
      type: r.data_type as string,
      nullable: r.is_nullable === true || r.is_nullable === "true",
      defaultValue: r.column_default ?? undefined,
      isPrimaryKey: r.is_pk === true || r.is_pk === "true",
      isForeignKey: r.is_fk === true || r.is_fk === "true",
    }));
  }

  async query(sql: string, params?: unknown[]): Promise<QueryResult> {
    const start = Date.now();

    const res = await this.pool!.query({
      text: sql,
      values: params as any[],
      rowMode: "array",
    });
    const executionTimeMs = Date.now() - start;
    const result = Array.isArray(res) ? res[res.length - 1] : res;
    const columns: string[] = result.fields?.map((f: any) => f.name) ?? [];
    const rawRows: unknown[][] = result.rows ?? [];
    const rows = rawRows.map((row) =>
      Object.fromEntries(row.map((val, i) => [`__col_${i}`, val])),
    );
    return {
      columns,
      rows,
      rowCount: result.rowCount ?? rawRows.length,
      executionTimeMs,
    };
  }

  async getIndexes(
    database: string,
    schema: string,
    table: string,
  ): Promise<import("./types").IndexMeta[]> {
    const res = await this.pool!.query(
      `SELECT i.relname AS name,
              ix.indisunique AS unique,
              ix.indisprimary AS primary,
              a.attname AS column
       FROM pg_class c
       JOIN pg_index ix ON ix.indrelid = c.oid
       JOIN pg_class i  ON i.oid = ix.indexrelid
       JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(ix.indkey)
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = $1 AND c.relname = $2
       ORDER BY i.relname, a.attnum`,
      [schema, table],
    );
    const map = new Map<string, import("./types").IndexMeta>();
    for (const r of res.rows) {
      if (!map.has(r.name)) {
        map.set(r.name, {
          name: r.name,
          columns: [],
          unique: r.unique,
          primary: r.primary,
        });
      }
      map.get(r.name)!.columns.push(r.column);
    }
    return [...map.values()];
  }

  async getForeignKeys(
    _database: string,
    schema: string,
    table: string,
  ): Promise<import("./types").ForeignKeyMeta[]> {
    const res = await this.pool!.query(
      `SELECT kcu.constraint_name,
              kcu.column_name,
              ccu.table_schema AS ref_schema,
              ccu.table_name   AS ref_table,
              ccu.column_name  AS ref_column
       FROM information_schema.table_constraints tc
       JOIN information_schema.key_column_usage kcu
         ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
       JOIN information_schema.constraint_column_usage ccu
         ON ccu.constraint_name = tc.constraint_name
       WHERE tc.constraint_type = 'FOREIGN KEY'
         AND tc.table_schema = $1 AND tc.table_name = $2`,
      [schema, table],
    );
    return res.rows.map((r) => ({
      constraintName: r.constraint_name,
      column: r.column_name,
      referencedSchema: r.ref_schema,
      referencedTable: r.ref_table,
      referencedColumn: r.ref_column,
    }));
  }

  async getCreateTableDDL(
    _database: string,
    schema: string,
    table: string,
  ): Promise<string> {
    const kindRes = await this.pool!.query<{ table_type: string }>(
      `SELECT table_type FROM information_schema.tables
       WHERE table_schema = $1 AND table_name = $2 LIMIT 1`,
      [schema, table],
    );
    const isView = kindRes.rows[0]?.table_type === "VIEW";

    if (isView) {
      const res = await this.pool!.query<{ def: string }>(
        `SELECT 'CREATE OR REPLACE VIEW "' || n.nspname || '"."' || c.relname || '" AS\n' ||
                pg_get_viewdef(c.oid, true) AS def
         FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'v'
         LIMIT 1`,
        [schema, table],
      );
      return (
        res.rows[0]?.def ??
        `-- View definition not available for "${schema}"."${table}"`
      );
    }

    const colRes = await this.pool!.query(
      `SELECT
         a.attname                                AS column_name,
         format_type(a.atttypid, a.atttypmod)    AS data_type,
         NOT a.attnotnull                         AS is_nullable,
         pg_get_expr(d.adbin, d.adrelid)         AS column_default
       FROM pg_attribute a
       JOIN pg_class     c ON c.oid = a.attrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
       LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
       WHERE n.nspname = $1
         AND c.relname = $2
         AND a.attnum > 0
         AND NOT a.attisdropped
       ORDER BY a.attnum`,
      [schema, table],
    );

    const pkRes = await this.pool!.query<{ conkey: number[] }>(
      `SELECT con.conkey
       FROM pg_constraint con
       JOIN pg_class     c ON c.oid = con.conrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = $1 AND c.relname = $2 AND con.contype = 'p'
       LIMIT 1`,
      [schema, table],
    );
    const pkAttNums = new Set<number>(pkRes.rows[0]?.conkey ?? []);

    const attNumRes = await this.pool!.query<{
      attname: string;
      attnum: number;
    }>(
      `SELECT a.attname, a.attnum
       FROM pg_attribute a
       JOIN pg_class     c ON c.oid = a.attrelid
       JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 AND NOT a.attisdropped`,
      [schema, table],
    );
    const attNumMap = new Map<string, number>(
      attNumRes.rows.map((r) => [r.attname, r.attnum]),
    );

    const cols = colRes.rows.map((r) => {
      const isPk = pkAttNums.has(attNumMap.get(r.column_name) ?? -1);
      const nullable = r.is_nullable === true || r.is_nullable === "true";
      const notNull = !nullable && !isPk ? " NOT NULL" : "";
      const defClause = r.column_default ? ` DEFAULT ${r.column_default}` : "";
      const pk = isPk ? " PRIMARY KEY" : "";
      const dataType = (r.data_type as string).toLowerCase();
      return `  "${r.column_name}" ${dataType}${notNull}${defClause}${pk}`;
    });
    return `CREATE TABLE "${schema}"."${table}" (\n${cols.join(",\n")}\n);`;
  }

  async getRoutineDefinition(
    _database: string,
    schema: string,
    name: string,
    _kind: "function" | "procedure",
  ): Promise<string> {
    const res = await this.pool!.query<{ def: string }>(
      `SELECT pg_get_functiondef(p.oid) AS def
       FROM pg_proc p
       JOIN pg_namespace n ON n.oid = p.pronamespace
       WHERE n.nspname = $1 AND p.proname = $2
       LIMIT 1`,
      [schema, name],
    );
    return res.rows[0]?.def ?? `-- Definition not available for ${name}`;
  }

  async runTransaction(
    operations: import("./types").TransactionOperation[],
  ): Promise<void> {
    const client = await this.pool!.connect();
    try {
      await client.query("BEGIN");
      for (const op of operations) {
        const res = await client.query(op.sql, op.params as any[]);
        if (op.checkAffectedRows && res.rowCount === 0) {
          throw new Error(
            "Row not found — the row may have been modified or deleted by another user",
          );
        }
      }
      await client.query("COMMIT");
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }
  }
}
