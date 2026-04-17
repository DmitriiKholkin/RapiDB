import type { Pool } from "mysql2/promise";
import * as mysql from "mysql2/promise";
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
import { DATETIME_SQL_RE, ISO_DATETIME_RE, NULL_SENTINEL } from "./types";

export function splitMySQLScript(sql: string): string[] {
  const stmts: string[] = [];
  let delim = ";";
  let i = 0;
  const n = sql.length;
  let buf = "";
  let compoundDepth = 0;

  function isWordChar(p: number): boolean {
    if (p < 0 || p >= n) return false;
    const ch = sql[p];
    return /[A-Za-z0-9_]/.test(ch);
  }

  function consumeKeyword(kw: string): boolean {
    if (isWordChar(i - 1)) return false;
    const len = kw.length;
    if (sql.slice(i, i + len).toUpperCase() !== kw) return false;
    if (isWordChar(i + len)) return false;
    buf += sql.slice(i, i + len);
    i += len;
    return true;
  }

  while (i < n) {
    const atLineStart = i === 0 || sql[i - 1] === "\n";
    if (atLineStart && /^DELIMITER[ \t]/i.test(sql.slice(i))) {
      const s = buf.trim();
      if (s) stmts.push(s);
      buf = "";

      i += 9;
      while (i < n && (sql[i] === " " || sql[i] === "\t")) i++;

      let nd = "";
      while (
        i < n &&
        sql[i] !== " " &&
        sql[i] !== "\t" &&
        sql[i] !== "\r" &&
        sql[i] !== "\n"
      ) {
        nd += sql[i++];
      }
      if (nd) delim = nd;

      while (i < n && sql[i] !== "\n") i++;
      if (i < n) i++;
      continue;
    }

    const isDashComment =
      sql[i] === "-" &&
      sql[i + 1] === "-" &&
      (sql[i + 2] === " " ||
        sql[i + 2] === "\t" ||
        sql[i + 2] === "\n" ||
        sql[i + 2] === "\r");
    if (isDashComment || sql[i] === "#") {
      while (i < n && sql[i] !== "\n") i++;
      continue;
    }

    if (sql[i] === "/" && sql[i + 1] === "*") {
      i += 2;
      while (i < n) {
        if (sql[i] === "*" && sql[i + 1] === "/") {
          i += 2;
          break;
        }
        i++;
      }
      continue;
    }

    if (sql[i] === "'" || sql[i] === '"' || sql[i] === "`") {
      const q = sql[i];
      buf += q;
      i++;
      while (i < n) {
        const c = sql[i];
        if (c === "\\" && q !== "`") {
          buf += c + (sql[i + 1] ?? "");
          i += 2;
          continue;
        }
        if (c === q && sql[i + 1] === q) {
          buf += c + c;
          i += 2;
          continue;
        }
        if (c === q) {
          buf += c;
          i++;
          break;
        }
        buf += c;
        i++;
      }
      continue;
    }

    if (consumeKeyword("BEGIN")) {
      compoundDepth++;
      continue;
    }
    if (consumeKeyword("CASE")) {
      compoundDepth++;
      continue;
    }
    if (consumeKeyword("IF")) {
      compoundDepth++;
      continue;
    }
    if (consumeKeyword("LOOP")) {
      compoundDepth++;
      continue;
    }
    if (consumeKeyword("REPEAT")) {
      compoundDepth++;
      continue;
    }
    if (consumeKeyword("WHILE")) {
      compoundDepth++;
      continue;
    }
    if (consumeKeyword("END")) {
      if (compoundDepth > 0) compoundDepth--;
      continue;
    }

    if (compoundDepth === 0 && sql.startsWith(delim, i)) {
      const s = buf.trim();
      if (s) stmts.push(s);
      buf = "";
      i += delim.length;
      while (
        i < n &&
        (sql[i] === " " ||
          sql[i] === "\t" ||
          sql[i] === "\r" ||
          sql[i] === "\n")
      )
        i++;
      continue;
    }

    buf += sql[i++];
  }

  const s = buf.trim();
  if (s) stmts.push(s);

  return stmts;
}

const MYSQL_SPATIAL_TYPES = new Set([
  "point",
  "linestring",
  "polygon",
  "multipoint",
  "multilinestring",
  "multipolygon",
  "geometrycollection",
  "geometry",
]);

function isMysqlSpatialType(colType: string): boolean {
  return MYSQL_SPATIAL_TYPES.has(colType.toLowerCase().split("(")[0].trim());
}

function parseMysqlSpatialToWkt(val: string): string {
  const trimmed = val.trim();
  if (
    /^(POINT|LINESTRING|POLYGON|MULTI\w+|GEOMETRYCOLLECTION|GEOMETRY)\s*\(/i.test(
      trimmed,
    )
  ) {
    return trimmed;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch {
    throw new Error(
      "Cannot parse spatial value. Use WKT, e.g. POINT(1 2) or POLYGON((0 0, 1 0, 1 1, 0 0)).",
    );
  }
  try {
    return mysqlSpatialJsonToWkt(parsed);
  } catch (e: any) {
    throw new Error(
      `Cannot convert spatial value to WKT: ${e?.message ?? String(e)}. Use WKT, e.g. POINT(1 2).`,
    );
  }
}

function mysqlSpatialJsonToWkt(obj: unknown): string {
  if (obj === null || typeof obj !== "object")
    throw new Error("Invalid spatial JSON");
  if (!Array.isArray(obj)) {
    const o = obj as Record<string, unknown>;
    if (typeof o.x === "number" && typeof o.y === "number")
      return `POINT(${o.x} ${o.y})`;
    throw new Error("Unknown spatial object format");
  }
  const arr = obj as unknown[];
  if (arr.length === 0) throw new Error("Empty spatial array");
  const first = arr[0];
  if (
    first !== null &&
    typeof first === "object" &&
    !Array.isArray(first) &&
    typeof (first as any).x === "number"
  ) {
    return `LINESTRING(${(arr as Array<{ x: number; y: number }>).map((p) => `${p.x} ${p.y}`).join(", ")})`;
  }
  if (Array.isArray(first)) {
    return `POLYGON(${(arr as Array<Array<{ x: number; y: number }>>).map((ring) => `(${ring.map((p) => `${p.x} ${p.y}`).join(", ")})`).join(", ")})`;
  }
  throw new Error("Unrecognised spatial JSON structure");
}

function mysqlTypeName(nativeType: string): string {
  return nativeType.toLowerCase().split("(")[0].trim();
}

function formatMysqlDatetimeUtc(value: Date): string | null {
  if (Number.isNaN(value.getTime())) return null;

  const pad = (n: number) => String(n).padStart(2, "0");
  const ms = value.getUTCMilliseconds();
  const frac = ms > 0 ? `.${String(ms).padStart(3, "0")}` : "";
  return (
    `${value.getUTCFullYear()}-${pad(value.getUTCMonth() + 1)}-${pad(value.getUTCDate())} ` +
    `${pad(value.getUTCHours())}:${pad(value.getUTCMinutes())}:${pad(value.getUTCSeconds())}${frac}`
  );
}

function normalizeMysqlDateInput(value: string): string | null {
  const trimmed = value.trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    return trimmed;
  }
  if (ISO_DATETIME_RE.test(trimmed)) {
    return isoToLocalDateStr(trimmed);
  }
  if (DATETIME_SQL_RE.test(trimmed)) {
    if (/[+-]\d{2}:\d{2}$/.test(trimmed)) {
      return isoToLocalDateStr(trimmed.replace(" ", "T"));
    }
    return trimmed.slice(0, 10);
  }
  return null;
}

function normalizeMysqlDatetimeInput(value: string): string | null {
  const trimmed = value.trim();
  if (ISO_DATETIME_RE.test(trimmed)) {
    return formatMysqlDatetimeUtc(new Date(trimmed));
  }
  if (!DATETIME_SQL_RE.test(trimmed)) {
    return null;
  }

  if (/[+-]\d{2}:\d{2}$/.test(trimmed)) {
    return formatMysqlDatetimeUtc(new Date(trimmed.replace(" ", "T")));
  }

  return formatDatetimeForDisplay(trimmed) ?? trimmed;
}

export class MySQLDriver extends BaseDBDriver {
  private pool: Pool | null = null;
  private readonly config: ConnectionConfig;

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

    this.pool = mysql.createPool({
      host: this.config.host,
      port: this.config.port,
      database: this.config.database,
      user: this.config.username,
      password: this.config.password,
      waitForConnections: true,
      connectionLimit: 5,
      connectTimeout: 10000,
      idleTimeout: 30000,
      dateStrings: true,
      bigNumberStrings: true,
      supportBigNumbers: true,
      ssl: sslEnabled
        ? {
            rejectUnauthorized: this.config.rejectUnauthorized ?? true,
          }
        : undefined,
    });
    const conn = await this.pool.getConnection();
    conn.release();
  }

  async disconnect(): Promise<void> {
    await this.pool?.end();
    this.pool = null;
  }

  isConnected(): boolean {
    return this.pool !== null;
  }

  async listDatabases(): Promise<DatabaseInfo[]> {
    const [rows] = await this.pool!.query<any[]>("SHOW DATABASES");
    return rows.map((r) => ({
      name: Object.values(r)[0] as string,
      schemas: [],
    }));
  }

  async listSchemas(_database: string): Promise<SchemaInfo[]> {
    return [{ name: _database }];
  }

  async listObjects(database: string, _schema: string): Promise<TableInfo[]> {
    const [tableRows] = await this.pool!.query<any[]>(
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
      const [fnRows] = await this.pool!.query<any[]>(
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
    const [rows] = await this.pool!.query<any[]>(
      `SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY, EXTRA
       FROM information_schema.COLUMNS
       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`,
      [database, table],
    );
    return rows.map((r) => ({
      name: r.COLUMN_NAME as string,
      type: r.COLUMN_TYPE as string,
      nullable: r.IS_NULLABLE === "YES",
      defaultValue: r.COLUMN_DEFAULT ?? undefined,
      isPrimaryKey: r.COLUMN_KEY === "PRI",
      isForeignKey: r.COLUMN_KEY === "MUL",
      isAutoIncrement:
        (r.EXTRA as string)?.toLowerCase().includes("auto_increment") ?? false,
    }));
  }

  async query(sql: string, params?: unknown[]): Promise<QueryResult> {
    const start = Date.now();

    if (params && params.length > 0) {
      const [rawRows, fields] = await this.pool!.query<any[]>({
        sql,
        values: params,
        rowsAsArray: true,
      } as any);
      return this._parseQueryResult(
        rawRows,
        fields as any[],
        Date.now() - start,
      );
    }

    const stmts = splitMySQLScript(sql);
    if (stmts.length === 0) {
      return { columns: [], rows: [], rowCount: 0, executionTimeMs: 0 };
    }

    if (stmts.length === 1) {
      const [rawRows, fields] = await this.pool!.query<any[]>({
        sql: stmts[0],
        rowsAsArray: true,
      } as any);
      return this._parseQueryResult(
        rawRows,
        fields as any[],
        Date.now() - start,
      );
    }

    return this._executeScript(stmts, start);
  }

  private _parseQueryResult(
    rawRows: any,
    fields: any[],
    executionTimeMs: number,
  ): QueryResult {
    const fieldList = Array.isArray(fields) ? fields : [];
    const isSelect =
      Array.isArray(rawRows) &&
      (rawRows.length === 0 || Array.isArray((rawRows as any)[0]));

    if (isSelect) {
      const columns = fieldList.map((f: any) => f.name as string);

      const boolCols = new Set<number>();
      const floatCols = new Set<number>();
      const bitIntCols = new Set<number>();
      fieldList.forEach((f: any, i: number) => {
        if (
          (f.type === 1 && f.length === 1) ||
          (f.type === 16 && f.length === 1)
        ) {
          boolCols.add(i);
        } else if (f.type === 16 && f.length > 1) {
          bitIntCols.add(i);
        }
        if (f.type === 4) {
          floatCols.add(i);
        }
      });

      const rows = (rawRows as unknown[][]).map((row) =>
        Object.fromEntries(
          row.map((val, i) => {
            let v = val;
            if (boolCols.has(i) && v !== null && v !== undefined) {
              if (Buffer.isBuffer(v)) {
                v = (v as Buffer)[0] === 1;
              } else {
                v = v === 1 || v === "1";
              }
            }
            if (bitIntCols.has(i) && v !== null && v !== undefined) {
              if (Buffer.isBuffer(v)) {
                const buf = v as Buffer;
                v =
                  buf.length === 0
                    ? 0
                    : buf.readUIntBE(0, Math.min(buf.length, 6));
              }
            }
            if (
              floatCols.has(i) &&
              typeof v === "number" &&
              !Number.isInteger(v)
            ) {
              v = parseFloat((v as number).toPrecision(7));
            }
            return [`__col_${i}`, v];
          }),
        ),
      );
      return { columns, rows, rowCount: rows.length, executionTimeMs };
    }

    const affectedRows = (rawRows as any)?.affectedRows as number | undefined;
    return {
      columns: [],
      rows: [],
      rowCount: affectedRows ?? 0,
      executionTimeMs,
      affectedRows,
    };
  }

  private async _executeScript(
    stmts: string[],
    start: number,
  ): Promise<QueryResult> {
    const conn = await this.pool!.getConnection();

    let lastResult: QueryResult = {
      columns: [],
      rows: [],
      rowCount: 0,
      executionTimeMs: 0,
    };
    let totalAffected = 0;
    try {
      for (const stmt of stmts) {
        const [rawRows, fields] = await conn.query({
          sql: stmt,
          rowsAsArray: true,
        } as any);
        const r = this._parseQueryResult(rawRows, fields as any[], 0);
        totalAffected += r.affectedRows ?? r.rowCount ?? 0;
        if (r.columns.length > 0) {
          lastResult = r;
        } else if (lastResult.columns.length === 0) {
          lastResult = r;
        }
      }
    } finally {
      conn.release();
    }
    lastResult.executionTimeMs = Date.now() - start;
    if (lastResult.columns.length === 0) {
      lastResult.rowCount = totalAffected;
      lastResult.affectedRows = totalAffected;
    }
    return lastResult;
  }

  async getIndexes(
    database: string,
    _schema: string,
    table: string,
  ): Promise<import("./types").IndexMeta[]> {
    const [rows] = await this.pool!.query<any[]>(
      `SELECT INDEX_NAME, COLUMN_NAME, NON_UNIQUE
       FROM information_schema.STATISTICS
       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
       ORDER BY INDEX_NAME, SEQ_IN_INDEX`,
      [database, table],
    );
    const map = new Map<string, import("./types").IndexMeta>();
    for (const r of rows) {
      const name = r.INDEX_NAME as string;
      if (!map.has(name)) {
        map.set(name, {
          name,
          columns: [],
          unique: r.NON_UNIQUE === 0,
          primary: name === "PRIMARY",
        });
      }
      map.get(name)!.columns.push(r.COLUMN_NAME as string);
    }
    return [...map.values()];
  }

  async getForeignKeys(
    database: string,
    _schema: string,
    table: string,
  ): Promise<import("./types").ForeignKeyMeta[]> {
    const [rows] = await this.pool!.query<any[]>(
      `SELECT kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME,
              kcu.REFERENCED_TABLE_SCHEMA, kcu.REFERENCED_TABLE_NAME, kcu.REFERENCED_COLUMN_NAME
       FROM information_schema.KEY_COLUMN_USAGE kcu
       JOIN information_schema.TABLE_CONSTRAINTS tc
         ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
         AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
         AND tc.TABLE_NAME = kcu.TABLE_NAME
       WHERE kcu.TABLE_SCHEMA = ? AND kcu.TABLE_NAME = ?
         AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'`,
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

  async getCreateTableDDL(
    database: string,
    _schema: string,
    table: string,
  ): Promise<string> {
    const [rows] = await this.pool!.query<any[]>(
      `SHOW CREATE TABLE \`${database.replace(/`/g, "``")}\`.\`${table.replace(/`/g, "``")}\``,
    );
    return (
      (rows[0] as any)["Create Table"] ?? (rows[0] as any)["Create View"] ?? ""
    );
  }

  async getRoutineDefinition(
    database: string,
    _schema: string,
    name: string,
    kind: "function" | "procedure",
  ): Promise<string> {
    const type = kind === "function" ? "FUNCTION" : "PROCEDURE";
    const db = database.replace(/`/g, "``");
    const nm = name.replace(/`/g, "``");
    const [rows] = await this.pool!.query<any[]>(
      `SHOW CREATE ${type} \`${db}\`.\`${nm}\``,
    );
    const row = rows[0] as Record<string, unknown>;
    const key = type === "FUNCTION" ? "Create Function" : "Create Procedure";
    return (row[key] as string) ?? `-- Definition not available for ${name}`;
  }

  async runTransaction(
    operations: import("./types").TransactionOperation[],
  ): Promise<void> {
    const conn = await this.pool!.getConnection();
    await conn.beginTransaction();
    try {
      for (const op of operations) {
        const [rows] = await conn.query<any>(op.sql, op.params);
        if (op.checkAffectedRows) {
          const affectedRows = !Array.isArray(rows)
            ? (rows as any).affectedRows
            : 0;
          if (affectedRows === 0) {
            throw new Error(
              "Row not found — the row may have been modified or deleted by another user",
            );
          }
        }
      }
      await conn.commit();
    } catch (e) {
      await conn.rollback();
      throw e;
    } finally {
      conn.release();
    }
  }

  // ─── MySQL type system ───

  mapTypeCategory(nativeType: string): TypeCategory {
    const ct = nativeType.toLowerCase();
    const base = ct.split("(")[0].trim();
    // Boolean
    if (
      base === "bool" ||
      base === "boolean" ||
      (base === "tinyint" && ct.includes("(1)")) ||
      (base === "bit" && ct.includes("(1)"))
    )
      return "boolean";
    // Integer
    if (
      ["tinyint", "smallint", "mediumint", "int", "integer", "bigint"].includes(
        base,
      )
    )
      return "integer";
    if (base === "bit") return "integer";
    if (base === "year") return "integer";
    // Float
    if (base === "float" || base === "double" || base === "real")
      return "float";
    // Decimal
    if (base === "decimal" || base === "numeric") return "decimal";
    // Date/time
    if (base === "date") return "date";
    if (base === "time") return "time";
    if (base === "datetime" || base === "timestamp") return "datetime";
    // Binary
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
    // JSON
    if (base === "json") return "json";
    // Spatial
    if (MYSQL_SPATIAL_TYPES.has(base)) return "spatial";
    // Enum/Set
    if (ct.startsWith("enum") || ct.startsWith("set")) return "enum";
    // Text
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

  isBooleanType(nativeType: string): boolean {
    const ct = nativeType.toLowerCase();
    const base = ct.split("(")[0].trim();
    if (base === "bool" || base === "boolean") return true;
    if (base === "tinyint" && ct.includes("(1)")) return true;
    if (base === "bit" && ct.includes("(1)")) return true;
    return false;
  }

  isDatetimeWithTime(nativeType: string): boolean {
    const ct = nativeType.toLowerCase();
    return ct === "datetime" || ct === "timestamp";
  }

  // ─── MySQL SQL helpers ───

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

  override buildInsertValueExpr(
    column: ColumnTypeMeta,
    _paramIndex: number,
  ): string {
    if (isMysqlSpatialType(column.nativeType)) return "ST_GeomFromText(?)";
    return "?";
  }

  override buildSetExpr(column: ColumnTypeMeta, _paramIndex: number): string {
    if (isMysqlSpatialType(column.nativeType)) {
      return `${this.quoteIdentifier(column.name)} = ST_GeomFromText(?)`;
    }
    return `${this.quoteIdentifier(column.name)} = ?`;
  }

  protected override coerceBooleanTrue(): unknown {
    return 1;
  }
  protected override coerceBooleanFalse(): unknown {
    return 0;
  }

  // ─── MySQL type-aware data helpers ───

  override coerceInputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (value === null || value === undefined || value === "") return value;
    if (value === NULL_SENTINEL) return null;
    if (typeof value !== "string") return value;

    if (column.isBoolean) {
      const lower = value.toLowerCase();
      if (lower === "true" || lower === "1") return 1;
      if (lower === "false" || lower === "0") return 0;
    }

    // Bit columns
    if (column.nativeType.toLowerCase().startsWith("bit")) {
      const n = parseInt(value, 10);
      if (!Number.isNaN(n)) return n;
    }

    // Binary
    if (column.category === "binary")
      return super.coerceInputValue(value, column);

    // Spatial
    if (column.category === "spatial") return parseMysqlSpatialToWkt(value);

    const typeName = mysqlTypeName(column.nativeType);
    if (typeName === "date") {
      return normalizeMysqlDateInput(value) ?? value;
    }

    if (typeName === "datetime" || typeName === "timestamp") {
      return normalizeMysqlDatetimeInput(value) ?? value;
    }

    return value;
  }

  override formatOutputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (value === null || value === undefined) return value;
    if (Buffer.isBuffer(value)) return super.formatOutputValue(value, column);
    if (typeof value === "bigint") return value.toString();
    if (
      value !== null &&
      typeof value === "object" &&
      !(value instanceof Date)
    ) {
      return JSON.stringify(value);
    }
    if (this.isDatetimeWithTime(column.nativeType)) {
      const formatted = formatDatetimeForDisplay(value);
      if (formatted !== null) return formatted;
    }
    return value;
  }

  // ─── MySQL filter building ───

  override buildFilterCondition(
    column: ColumnTypeMeta,
    operator: FilterOperator,
    value: string | [string, string] | undefined,
    _paramIndex: number,
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
        const boolVal = strVal === "true" ? 1 : 0;
        const op = operator === "neq" ? "!=" : "=";
        return { sql: `${col} ${op} ?`, params: [boolVal] };
      }
    }

    // Spatial: ST_AsText LIKE
    if (column.category === "spatial") {
      const v = typeof val === "string" ? val : val[0];
      return { sql: `ST_AsText(${col}) LIKE ?`, params: [`%${v}%`] };
    }

    // Binary: HEX LIKE
    if (column.category === "binary") {
      const v = typeof val === "string" ? val : val[0];
      const hexVal = v.replace(/^(0x|\\x)/i, "").toUpperCase();
      return { sql: `HEX(${col}) LIKE ?`, params: [`%${hexVal}%`] };
    }

    if (
      column.category === "date" &&
      typeof val === "string" &&
      (operator === "eq" || operator === "neq")
    ) {
      const sqlOp = operator === "neq" ? "!=" : "=";
      return {
        sql: `${col} ${sqlOp} CAST(? AS DATE)`,
        params: [normalizeMysqlDateInput(val) ?? val],
      };
    }

    if (
      this.isNumericCategory(column.category) &&
      this.isNumericCompareUnsafe(column.nativeType) &&
      typeof val === "string" &&
      Number.isFinite(Number(val)) &&
      val !== ""
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

    // Numeric
    if (
      this.isNumericCategory(column.category) &&
      typeof val === "string" &&
      !this.isNumericCompareUnsafe(column.nativeType) &&
      !Number.isNaN(Number(val)) &&
      val !== ""
    ) {
      const sqlOp = this.sqlOperator(operator);
      return { sql: `${col} ${sqlOp} ?`, params: [Number(val)] };
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

    // Datetime text search
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
    const mysqlVal = DATETIME_SQL_RE.test(finalVal)
      ? `${finalVal}%`
      : `%${finalVal}%`;
    return { sql: `CAST(${col} AS CHAR) LIKE ?`, params: [mysqlVal] };
  }

  private isNumericCompareUnsafe(nativeType: string): boolean {
    const ct = nativeType.toLowerCase().split("(")[0].trim();
    return (
      ct === "date" ||
      ct === "datetime" ||
      ct === "timestamp" ||
      ct === "year" ||
      ct === "time" ||
      ct === "float" ||
      ct === "real" ||
      ct === "double"
    );
  }

  private approximateNumericTolerance(rawValue: string): number {
    const fraction = /\.(\d+)/.exec(rawValue)?.[1].length ?? 0;
    const precision = Math.min(Math.max(fraction + 2, 6), 12);
    return 10 ** -precision;
  }
}
