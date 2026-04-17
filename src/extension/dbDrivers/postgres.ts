import { Pool, types as pgTypes } from "pg";
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
  IDBDriver,
  PaginationResult,
  QueryResult,
  SchemaInfo,
  TableInfo,
  TypeCategory,
} from "./types";
import { DATE_ONLY_RE, ISO_DATETIME_RE, NULL_SENTINEL } from "./types";

pgTypes.setTypeParser(1082, (val: string) => val);
pgTypes.setTypeParser(1114, (val: string) => val);
pgTypes.setTypeParser(1184, (val: string) => val);

const PG_GEOMETRIC_TYPES = new Set([
  "point",
  "line",
  "lseg",
  "box",
  "path",
  "polygon",
  "circle",
]);

function normalizeCidrValue(val: string): string {
  const m = val
    .trim()
    .match(/^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\/(\d{1,2})$/);
  if (!m) return val;
  const prefix = parseInt(m[2], 10);
  if (prefix < 0 || prefix > 32) return val;
  const parts = m[1].split(".").map(Number);
  if (parts.some((p) => p < 0 || p > 255)) return val;
  const mask =
    prefix === 0 ? 0 : ((0xffffffff << (32 - prefix)) & 0xffffffff) >>> 0;
  const ipNum =
    (((parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3]) >>> 0) &
    mask;
  const masked = [
    (ipNum >>> 24) & 0xff,
    (ipNum >>> 16) & 0xff,
    (ipNum >>> 8) & 0xff,
    ipNum & 0xff,
  ].join(".");
  return `${masked}/${prefix}`;
}

function jsonToPgCircle(val: string): string {
  const trimmed = val.trim();
  if (!trimmed.startsWith("{")) return trimmed;
  try {
    const obj = JSON.parse(trimmed) as Record<string, unknown>;
    if (
      typeof obj.x === "number" &&
      typeof obj.y === "number" &&
      typeof obj.radius === "number"
    ) {
      return `<(${obj.x},${obj.y}),${obj.radius}>`;
    }
  } catch {}
  return trimmed;
}

export class PostgresDriver extends BaseDBDriver {
  private pool: Pool | null = null;
  private readonly config: ConnectionConfig;
  private _connected = false;

  constructor(config: ConnectionConfig) {
    super();
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
    return res.rows.map((r) => {
      const rawDefault = r.column_default as string | null | undefined;
      const isAutoIncrement =
        typeof rawDefault === "string" && rawDefault.startsWith("nextval(");
      return {
        name: r.column_name as string,
        type: r.data_type as string,
        nullable: r.is_nullable === true || r.is_nullable === "true",
        defaultValue: isAutoIncrement ? undefined : (rawDefault ?? undefined),
        isPrimaryKey: r.is_pk === true || r.is_pk === "true",
        isForeignKey: r.is_fk === true || r.is_fk === "true",
        isAutoIncrement,
      };
    });
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

  // ─── PG type system ───

  mapTypeCategory(nativeType: string): TypeCategory {
    const ct = nativeType.toLowerCase().split("(")[0].trim();
    if (ct === "boolean" || ct === "bool") return "boolean";
    if (
      ct === "smallint" ||
      ct === "integer" ||
      ct === "bigint" ||
      ct === "serial" ||
      ct === "bigserial" ||
      ct === "smallserial" ||
      ct === "oid" ||
      ct === "xid" ||
      ct === "cid"
    )
      return "integer";
    if (
      ct === "real" ||
      ct === "double precision" ||
      ct === "float4" ||
      ct === "float8"
    )
      return "float";
    if (ct === "numeric" || ct === "decimal" || ct === "money")
      return "decimal";
    if (ct === "date") return "date";
    if (
      ct === "time" ||
      ct === "timetz" ||
      ct === "time with time zone" ||
      ct === "time without time zone"
    )
      return "time";
    if (ct.startsWith("timestamp")) return "datetime";
    if (ct === "bytea") return "binary";
    if (ct === "json" || ct === "jsonb") return "json";
    if (ct === "uuid") return "uuid";
    if (PG_GEOMETRIC_TYPES.has(ct)) return "spatial";
    if (ct === "interval" || nativeType.toLowerCase().startsWith("interval"))
      return "interval";
    if (
      nativeType.toLowerCase().endsWith("[]") ||
      nativeType.toLowerCase().startsWith("_") ||
      ct === "array"
    )
      return "array";
    if (ct === "bit" || ct === "varbit") return "other";
    if (ct === "inet" || ct === "cidr" || ct === "macaddr" || ct === "macaddr8")
      return "text";
    if (ct === "tsvector" || ct === "tsquery") return "text";
    if (
      ct === "text" ||
      ct === "varchar" ||
      ct.startsWith("character") ||
      ct === "name" ||
      ct === "xml"
    )
      return "text";
    return "other";
  }

  isBooleanType(nativeType: string): boolean {
    const ct = nativeType.toLowerCase().split("(")[0].trim();
    return ct === "boolean" || ct === "bool";
  }

  isDatetimeWithTime(nativeType: string): boolean {
    const ct = nativeType.toLowerCase();
    return (
      ct.startsWith("timestamp") ||
      ct === "timetz" ||
      ct === "time with time zone" ||
      ct === "time"
    );
  }

  protected override isFilterable(
    nativeType: string,
    category: TypeCategory,
  ): boolean {
    if (PG_GEOMETRIC_TYPES.has(nativeType.toLowerCase().split("(")[0].trim()))
      return false;
    if (category === "array" || category === "interval") return false;
    return super.isFilterable(nativeType, category);
  }

  // ─── PG SQL helpers ───

  override buildPagination(
    offset: number,
    limit: number,
    paramIndex: number,
  ): PaginationResult {
    return {
      sql: `LIMIT $${paramIndex} OFFSET $${paramIndex + 1}`,
      params: [limit, offset],
    };
  }

  override buildInsertValueExpr(
    _column: ColumnTypeMeta,
    paramIndex: number,
  ): string {
    return `$${paramIndex}`;
  }

  override buildSetExpr(column: ColumnTypeMeta, paramIndex: number): string {
    return `${this.quoteIdentifier(column.name)} = $${paramIndex}`;
  }

  // ─── PG type-aware data helpers ───

  override coerceInputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (value === null || value === undefined || value === "") return value;
    if (value === NULL_SENTINEL) return null;
    if (typeof value !== "string") return value;

    if (column.isBoolean) {
      const lower = value.toLowerCase();
      if (lower === "true" || lower === "1") return true;
      if (lower === "false" || lower === "0") return false;
    }

    // Arrays: JSON array → native array
    if (column.category === "array") {
      if (value.startsWith("[") && value.endsWith("]")) {
        try {
          const parsed = JSON.parse(value);
          if (Array.isArray(parsed)) return parsed;
        } catch {}
      }
    }

    // Interval: JSON object → ISO duration
    if (column.category === "interval" && value.startsWith("{")) {
      try {
        const obj = JSON.parse(value) as Record<string, number>;
        let iso = "P";
        if (obj.years) iso += `${obj.years}Y`;
        if (obj.months) iso += `${obj.months}M`;
        if (obj.days) iso += `${obj.days}D`;
        const hasTime = obj.hours || obj.minutes || obj.seconds;
        if (hasTime) {
          iso += "T";
          if (obj.hours) iso += `${obj.hours}H`;
          if (obj.minutes) iso += `${obj.minutes}M`;
          if (obj.seconds) iso += `${obj.seconds}S`;
        }
        if (iso === "P") iso = "P0D";
        return iso;
      } catch {}
      return value;
    }

    // bytea: hex string → Buffer
    if (column.category === "binary") {
      return super.coerceInputValue(value, column);
    }

    // CIDR: normalize
    const ct = column.nativeType.toLowerCase().split("(")[0].trim();
    if (ct === "cidr") return normalizeCidrValue(value);
    if (ct === "circle") return jsonToPgCircle(value);

    // ISO datetime → date-only for date columns
    if (ISO_DATETIME_RE.test(value) && column.category === "date") {
      return isoToLocalDateStr(value) ?? value;
    }

    return value;
  }

  override formatOutputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (value === null || value === undefined) return value;
    if (Buffer.isBuffer(value)) return super.formatOutputValue(value, column);
    if (typeof value === "bigint") return value.toString();

    // PG point object → "(x, y)" string
    if (
      value !== null &&
      typeof value === "object" &&
      !(value instanceof Date)
    ) {
      if (
        "x" in (value as object) &&
        "y" in (value as object) &&
        Object.keys(value as object).length === 2
      ) {
        return `(${(value as any).x}, ${(value as any).y})`;
      }
      return JSON.stringify(value);
    }

    if (this.isDatetimeWithTime(column.nativeType)) {
      const formatted = formatDatetimeForDisplay(value);
      if (formatted !== null) return formatted;
    }

    return value;
  }

  // ─── PG filter building ───

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

    // Boolean
    if (column.isBoolean && (operator === "eq" || operator === "neq")) {
      const strVal = (typeof val === "string" ? val : val[0]).toLowerCase();
      if (strVal === "true" || strVal === "false") {
        const boolVal = strVal === "true";
        const op = operator === "neq" ? "!=" : "=";
        return { sql: `${col} ${op} $${paramIndex}`, params: [boolVal] };
      }
    }

    // Date exact match
    if (column.category === "date" && typeof val === "string") {
      let dateVal: string | null = null;
      if (DATE_ONLY_RE.test(val)) dateVal = val;
      else if (ISO_DATETIME_RE.test(val)) dateVal = isoToLocalDateStr(val);
      if (dateVal) {
        if (operator === "eq")
          return { sql: `${col} = $${paramIndex}::date`, params: [dateVal] };
        const sqlOp = this.sqlOperator(operator);
        return {
          sql: `${col} ${sqlOp} $${paramIndex}::date`,
          params: [dateVal],
        };
      }
    }

    // Timestamp/time: CAST AS TEXT ILIKE
    if (column.category === "datetime" || column.category === "time") {
      if (operator === "eq" || operator === "like" || operator === "ilike") {
        const v = typeof val === "string" ? val : val[0];
        return {
          sql: `CAST(${col} AS TEXT) ILIKE $${paramIndex}`,
          params: [`%${v}%`],
        };
      }
      if (operator === "between" && Array.isArray(val)) {
        return {
          sql: `${col} BETWEEN $${paramIndex}::timestamp AND $${paramIndex + 1}::timestamp`,
          params: [val[0], val[1]],
        };
      }
      const sqlOp = this.sqlOperator(operator);
      const v = typeof val === "string" ? val : val[0];
      return {
        sql: `CAST(${col} AS TEXT) ILIKE $${paramIndex}`,
        params: [`%${v}%`],
      };
    }

    // Numeric exact
    if (
      this.isNumericCategory(column.category) &&
      typeof val === "string" &&
      !Number.isNaN(Number(val)) &&
      val !== ""
    ) {
      const ct = column.nativeType.toLowerCase().split("(")[0].trim();
      if (ct === "bigint" && /^-?\d+$/.test(val)) {
        const sqlOp = this.sqlOperator(operator);
        return { sql: `${col} ${sqlOp} $${paramIndex}`, params: [BigInt(val)] };
      }
      const sqlOp = this.sqlOperator(operator);
      return { sql: `${col} ${sqlOp} $${paramIndex}`, params: [Number(val)] };
    }

    // Between for numerics
    if (operator === "between" && Array.isArray(val)) {
      return {
        sql: `${col} BETWEEN $${paramIndex} AND $${paramIndex + 1}`,
        params: [val[0], val[1]],
      };
    }

    // In
    if (operator === "in" && typeof val === "string") {
      const parts = val.split(",").map((s) => s.trim());
      const placeholders = parts.map((_, i) => `$${paramIndex + i}`).join(", ");
      return { sql: `${col} IN (${placeholders})`, params: parts };
    }

    // Default: CAST AS TEXT ILIKE
    const v = typeof val === "string" ? val : val[0];
    let finalVal = v;
    if (ISO_DATETIME_RE.test(v)) {
      finalVal = v
        .replace(/(\.\d*?[1-9])0+(?=[Z+-]|$)/, "$1")
        .replace(/\.0+(?=[Z+-]|$)/, "")
        .replace("T", "%")
        .replace("Z", "%")
        .replace(/[+-]\d{2}:\d{2}$/, "%");
    }
    return {
      sql: `CAST(${col} AS TEXT) ILIKE $${paramIndex}`,
      params: [`%${finalVal}%`],
    };
  }

  private isNumericCompareUnsafe(nativeType: string): boolean {
    const ct = nativeType.toLowerCase().split("(")[0].trim();
    return (
      ct === "date" ||
      ct === "time" ||
      ct === "timetz" ||
      ct === "boolean" ||
      ct === "bool" ||
      ct === "uuid" ||
      ct === "inet" ||
      ct === "cidr" ||
      ct === "macaddr" ||
      ct === "macaddr8" ||
      ct === "bit" ||
      ct === "varbit" ||
      ct === "oid" ||
      ct === "xid" ||
      ct === "cid" ||
      ct === "interval" ||
      nativeType.toLowerCase().startsWith("interval") ||
      ct === "bytea" ||
      ct === "tsvector" ||
      ct === "tsquery" ||
      nativeType.toLowerCase().endsWith("[]") ||
      nativeType.toLowerCase().startsWith("_") ||
      nativeType.toLowerCase() === "array"
    );
  }

  private isTextualType(nativeType: string): boolean {
    const ct = nativeType.toLowerCase();
    return (
      ct.includes("json") ||
      ct.includes("text") ||
      ct.includes("char") ||
      ct === "xml" ||
      ct === "uuid" ||
      PG_GEOMETRIC_TYPES.has(ct.split("(")[0].trim())
    );
  }
}
