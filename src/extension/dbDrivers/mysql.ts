import type { Pool } from "mysql2/promise";
import * as mysql from "mysql2/promise";
import type { ConnectionConfig } from "../connectionManager";
import type {
  ColumnMeta,
  DatabaseInfo,
  IDBDriver,
  QueryResult,
  SchemaInfo,
  TableInfo,
} from "./types";

export function splitMySQLScript(sql: string): string[] {
  const stmts: string[] = [];
  let delim = ";";
  let i = 0;
  const n = sql.length;
  let buf = "";

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

    if (sql.startsWith(delim, i)) {
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

export class MySQLDriver implements IDBDriver {
  private pool: Pool | null = null;
  private readonly config: ConnectionConfig;

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
    this.pool = mysql.createPool({
      host: this.config.host,
      port: this.config.port,
      database: this.config.database,
      user: this.config.username,
      password: this.config.password,
      waitForConnections: true,
      connectionLimit: 5,
      connectTimeout: 10000,
      multipleStatements: true,
      idleTimeout: 30000,
      dateStrings: true,
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
      `SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY
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
      fieldList.forEach((f: any, i: number) => {
        if (
          (f.type === 1 && f.length === 1) ||
          (f.type === 16 && f.length === 1)
        ) {
          boolCols.add(i);
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
        const [rawRows, fields] = await conn.query<any[]>({
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
}
