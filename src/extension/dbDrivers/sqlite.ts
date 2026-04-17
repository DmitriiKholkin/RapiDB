import { Database } from "node-sqlite3-wasm";
import type { ConnectionConfig } from "../connectionManager";
import { BaseDBDriver } from "./BaseDBDriver";
import type {
  ColumnMeta,
  ColumnTypeMeta,
  DatabaseInfo,
  FilterConditionResult,
  FilterOperator,
  QueryResult,
  SchemaInfo,
  TableInfo,
  TypeCategory,
} from "./types";
import { NULL_SENTINEL } from "./types";

function classifySql(sql: string): "select" | "dml" {
  const stripped = sql
    .replace(/\/\*[\s\S]*?\*\//g, " ")
    .replace(/--[^\n]*/g, " ")
    .trim()
    .toUpperCase();

  const SELECT_STARTERS = ["SELECT", "PRAGMA", "WITH", "EXPLAIN", "VALUES"];

  return SELECT_STARTERS.some((kw) => stripped.startsWith(kw))
    ? "select"
    : "dml";
}

function splitSQLiteScript(sql: string): string[] {
  const stmts: string[] = [];
  let buf = "";
  let i = 0;
  const n = sql.length;

  while (i < n) {
    if (sql[i] === "-" && sql[i + 1] === "-") {
      while (i < n && sql[i] !== "\n") buf += sql[i++];
      continue;
    }

    if (sql[i] === "/" && sql[i + 1] === "*") {
      buf += sql[i++] + sql[i++];
      while (i < n) {
        if (sql[i] === "*" && sql[i + 1] === "/") {
          buf += sql[i++] + sql[i++];
          break;
        }
        buf += sql[i++];
      }
      continue;
    }

    if (sql[i] === "'" || sql[i] === '"' || sql[i] === "`") {
      const q = sql[i];
      buf += sql[i++];
      while (i < n) {
        const c = sql[i];
        buf += c;
        i++;
        if (c === q) {
          if (sql[i] === q) {
            buf += sql[i++];
          } else {
            break;
          }
        }
      }
      continue;
    }

    if (sql[i] === ";") {
      i++;
      const stmt = buf.trim();
      if (stmt) stmts.push(stmt);
      buf = "";
      continue;
    }

    buf += sql[i++];
  }

  const tail = buf.trim();
  if (tail) stmts.push(tail);

  return stmts;
}

export class SQLiteDriver extends BaseDBDriver {
  private db: Database | null = null;
  private readonly config: ConnectionConfig;

  constructor(config: ConnectionConfig) {
    super();
    this.config = config;
  }

  async connect(): Promise<void> {
    if (!this.config.filePath) {
      throw new Error("[RapiDB] SQLite requires a filePath");
    }
    if (this.db !== null) {
      try {
        this.db.close();
      } catch {}
      this.db = null;
    }
    this.db = new Database(this.config.filePath);
  }

  async disconnect(): Promise<void> {
    this.db?.close();
    this.db = null;
  }

  isConnected(): boolean {
    return this.db !== null && this.db.isOpen;
  }

  async listDatabases(): Promise<DatabaseInfo[]> {
    return [{ name: "main", schemas: [] }];
  }

  async listSchemas(_database: string): Promise<SchemaInfo[]> {
    return [{ name: "main" }];
  }

  async listObjects(_database: string, _schema: string): Promise<TableInfo[]> {
    const rows = this.db!.all(
      `SELECT name, type
       FROM sqlite_master
       WHERE type IN ('table','view')
         AND name NOT LIKE 'sqlite_%'
       ORDER BY type DESC, name`,
    ) as { name: string; type: string }[];

    return rows.map((r) => ({
      schema: "main",
      name: r.name,
      type: r.type as TableInfo["type"],
    }));
  }

  async describeTable(
    _database: string,
    _schema: string,
    table: string,
  ): Promise<ColumnMeta[]> {
    const safeName = table.replace(/"/g, '""');

    const rows = this.db!.all(`PRAGMA table_info("${safeName}")`) as {
      name: string;
      type: string;
      notnull: number;
      dflt_value: string | null;
      pk: number;
    }[];

    const fkRows = this.db!.all(`PRAGMA foreign_key_list("${safeName}")`) as {
      from: string;
    }[];
    const fkCols = new Set(fkRows.map((r) => r.from));

    const autoIncrementCols = new Set<string>();
    try {
      const master = this.db!.all(
        `SELECT sql FROM sqlite_master WHERE type='table' AND name=?`,
        [table],
      ) as { sql: string }[];
      const createSql = master[0]?.sql ?? "";
      const re =
        /[`"[]?(\w+)[`"\]]?\s+INTEGER\b[^,)]*\bPRIMARY\s+KEY\b[^,)]*\bAUTOINCREMENT\b/gi;
      let m: RegExpExecArray | null;
      while ((m = re.exec(createSql)) !== null) {
        autoIncrementCols.add(m[1].toLowerCase());
      }
    } catch {}

    return rows.map((r) => ({
      name: r.name,
      type: r.type || "TEXT",
      nullable: r.notnull === 0,
      defaultValue: r.dflt_value ?? undefined,
      isPrimaryKey: r.pk > 0,
      isForeignKey: fkCols.has(r.name),
      isAutoIncrement: autoIncrementCols.has(r.name.toLowerCase()),
    }));
  }

  async query(sql: string, params?: unknown[]): Promise<QueryResult> {
    const start = Date.now();

    if (params && params.length > 0) {
      return this._executeSingle(sql, params, start);
    }

    const stmts = splitSQLiteScript(sql);

    if (stmts.length === 0) {
      return { columns: [], rows: [], rowCount: 0, executionTimeMs: 0 };
    }

    if (stmts.length === 1) {
      return this._executeSingle(stmts[0], [], start);
    }

    return this._executeScript(stmts, start);
  }

  private _executeSingle(
    sql: string,
    params: unknown[],
    start: number,
  ): QueryResult {
    const kind = classifySql(sql);
    const bindValues = params as import("node-sqlite3-wasm").BindValues;

    if (kind === "select") {
      const rawRows = this.db!.all(sql, bindValues) as Record<
        string,
        unknown
      >[];
      const columns = rawRows.length > 0 ? Object.keys(rawRows[0]) : [];
      const rows = rawRows.map((row) =>
        Object.fromEntries(columns.map((col, i) => [`__col_${i}`, row[col]])),
      );
      return {
        columns,
        rows,
        rowCount: rows.length,
        executionTimeMs: Date.now() - start,
      };
    }

    try {
      const returningRows = this.db!.all(sql, bindValues) as Record<
        string,
        unknown
      >[];
      if (returningRows.length > 0) {
        const columns = Object.keys(returningRows[0]);
        const rows = returningRows.map((row) =>
          Object.fromEntries(columns.map((col, i) => [`__col_${i}`, row[col]])),
        );
        return {
          columns,
          rows,
          rowCount: rows.length,
          executionTimeMs: Date.now() - start,
        };
      }
    } catch {}

    const info = this.db!.run(sql, bindValues);
    return {
      columns: [],
      rows: [],
      rowCount: info.changes,
      executionTimeMs: Date.now() - start,
      affectedRows: info.changes,
    };
  }

  private _executeScript(stmts: string[], start: number): QueryResult {
    let lastResult: QueryResult = {
      columns: [],
      rows: [],
      rowCount: 0,
      executionTimeMs: 0,
    };
    let totalAffected = 0;

    for (const stmt of stmts) {
      const r = this._executeSingle(stmt, [], start);
      totalAffected += r.affectedRows ?? 0;
      if (r.columns.length > 0) {
        lastResult = r;
      } else if (lastResult.columns.length === 0) {
        lastResult = r;
      }
    }

    lastResult.executionTimeMs = Date.now() - start;
    if (lastResult.columns.length === 0) {
      lastResult.rowCount = totalAffected;
      lastResult.affectedRows = totalAffected;
    }
    return lastResult;
  }

  async getIndexes(
    _database: string,
    _schema: string,
    table: string,
  ): Promise<import("./types").IndexMeta[]> {
    const safeTable = table.replace(/"/g, '""');

    const idxList = this.db!.all(`PRAGMA index_list("${safeTable}")`) as {
      name: string;
      unique: number;
      origin: string;
    }[];

    const result: import("./types").IndexMeta[] = [];
    for (const idx of idxList) {
      const safeName = idx.name.replace(/"/g, '""');
      const cols = this.db!.all(`PRAGMA index_info("${safeName}")`) as {
        seqno: number;
        cid: number;
        name: string;
      }[];
      result.push({
        name: idx.name,
        columns: cols.map((c) => c.name),
        unique: idx.unique === 1,
        primary: idx.origin === "pk",
      });
    }
    return result;
  }

  async getForeignKeys(
    _database: string,
    _schema: string,
    table: string,
  ): Promise<import("./types").ForeignKeyMeta[]> {
    const safeTable = table.replace(/"/g, '""');
    const rows = this.db!.all(`PRAGMA foreign_key_list("${safeTable}")`) as {
      from: string;
      table: string;
      to: string;
      id: number;
    }[];

    return rows.map((r) => ({
      constraintName: `fk_${table}_${r.from}`,
      column: r.from,
      referencedSchema: "main",
      referencedTable: r.table,
      referencedColumn: r.to,
    }));
  }

  async getCreateTableDDL(
    _database: string,
    _schema: string,
    table: string,
  ): Promise<string> {
    const row = this.db!.get(
      `SELECT sql FROM sqlite_master WHERE type IN ('table','view') AND name = ?`,
      [table],
    ) as { sql: string } | null;
    return row?.sql ?? `-- DDL not available for "${table}"`;
  }

  async getRoutineDefinition(
    _database: string,
    _schema: string,
    name: string,
    kind: "function" | "procedure",
  ): Promise<string> {
    return `-- SQLite does not support stored ${kind}s.\n-- Object: ${name}`;
  }

  async runTransaction(
    operations: import("./types").TransactionOperation[],
  ): Promise<void> {
    const db = this.db!;
    db.exec("BEGIN TRANSACTION");
    try {
      for (const op of operations) {
        const bindValues = (op.params ??
          []) as import("node-sqlite3-wasm").BindValues;
        const info = db.run(op.sql, bindValues);
        if (op.checkAffectedRows && info.changes === 0) {
          throw new Error(
            "Row not found — the row may have been modified or deleted by another user",
          );
        }
      }
      db.exec("COMMIT");
    } catch (e) {
      if (db.inTransaction) {
        try {
          db.exec("ROLLBACK");
        } catch {}
      }
      throw e;
    }
  }

  // ─── SQLite type system ───

  mapTypeCategory(nativeType: string): TypeCategory {
    const ct = nativeType.toUpperCase().trim();
    if (ct === "" || ct === "TEXT") return "text";
    if (
      ct === "INTEGER" ||
      ct === "INT" ||
      ct === "BIGINT" ||
      ct === "SMALLINT" ||
      ct === "TINYINT" ||
      ct === "MEDIUMINT"
    )
      return "integer";
    if (ct === "REAL" || ct === "DOUBLE" || ct === "FLOAT") return "float";
    if (ct === "NUMERIC" || ct === "DECIMAL") return "decimal";
    if (ct === "BOOLEAN" || ct === "BOOL") return "boolean";
    if (ct === "BLOB") return "binary";
    if (ct === "DATE") return "date";
    if (ct === "TIME") return "time";
    if (ct === "DATETIME" || ct === "TIMESTAMP") return "datetime";
    if (ct.includes("INT")) return "integer";
    if (ct.includes("CHAR") || ct.includes("TEXT") || ct.includes("CLOB"))
      return "text";
    if (ct.includes("REAL") || ct.includes("FLOA") || ct.includes("DOUB"))
      return "float";
    if (ct.includes("BLOB")) return "binary";
    return "other";
  }

  isBooleanType(nativeType: string): boolean {
    const ct = nativeType.toUpperCase().trim();
    return ct === "BOOLEAN" || ct === "BOOL";
  }

  isDatetimeWithTime(nativeType: string): boolean {
    const ct = nativeType.toUpperCase().trim();
    return ct === "DATETIME" || ct === "TIMESTAMP";
  }

  // ─── SQLite SQL helpers ───

  override quoteIdentifier(name: string): string {
    return `"${name.replace(/"/g, '""')}"`;
  }

  override coerceInputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (value === null || value === undefined || value === "") return value;
    if (value === NULL_SENTINEL) return null;
    if (typeof value !== "string") return value;

    if (column.isBoolean) {
      const lower = value.toLowerCase();
      if (lower === "true" || lower === "1") return 1;
      if (lower === "false" || lower === "0") return 0;
    }

    return value;
  }

  override formatOutputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (value === null || value === undefined) return value;
    if (typeof value === "bigint") return value.toString();
    return value;
  }

  // ─── SQLite filter building ───

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
        const boolVal = strVal === "true" ? 1 : 0;
        const op = operator === "neq" ? "!=" : "=";
        return { sql: `${col} ${op} ?`, params: [boolVal] };
      }
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

    // Default text LIKE
    const v = typeof val === "string" ? val : val[0];
    return { sql: `${col} LIKE ?`, params: [`%${v}%`] };
  }
}
