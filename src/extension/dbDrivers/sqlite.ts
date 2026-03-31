import { Database } from "node-sqlite3-wasm";
import type { ConnectionConfig } from "../connectionManager";
import type {
  ColumnMeta,
  DatabaseInfo,
  IDBDriver,
  QueryResult,
  SchemaInfo,
  TableInfo,
} from "./types";

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

export class SQLiteDriver implements IDBDriver {
  private db: Database | null = null;
  private readonly config: ConnectionConfig;

  constructor(config: ConnectionConfig) {
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

    return rows.map((r) => ({
      name: r.name,
      type: r.type || "TEXT",
      nullable: r.notnull === 0,
      defaultValue: r.dflt_value ?? undefined,
      isPrimaryKey: r.pk > 0,
      isForeignKey: fkCols.has(r.name),
    }));
  }

  async query(sql: string, params?: unknown[]): Promise<QueryResult> {
    const start = Date.now();

    const kind = classifySql(sql);

    if (kind === "select") {
      const bindValues = (params ??
        []) as import("node-sqlite3-wasm").BindValues;
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

    if (params && params.length > 0) {
      const bindValues = params as import("node-sqlite3-wasm").BindValues;
      try {
        const returningRows = this.db!.all(sql, bindValues) as Record<
          string,
          unknown
        >[];
        if (returningRows.length > 0) {
          const columns = Object.keys(returningRows[0]);
          const rows = returningRows.map((row) =>
            Object.fromEntries(
              columns.map((col, i) => [`__col_${i}`, row[col]]),
            ),
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

    this.db!.exec(sql);
    return {
      columns: [],
      rows: [],
      rowCount: 0,
      executionTimeMs: Date.now() - start,
      affectedRows: 0,
    };
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
}
