import { Database } from "node-sqlite3-wasm";
import type { ConnectionConfig } from "../connectionManager";
import { BaseDBDriver } from "./BaseDBDriver";
import type {
  ColumnMeta,
  ColumnTypeMeta,
  DatabaseInfo,
  FilterConditionResult,
  FilterOperator,
  GeneratedKind,
  PersistedEditCheckOptions,
  PersistedEditCheckResult,
  QueryResult,
  SchemaInfo,
  TableInfo,
  TypeCategory,
  ValueSemantics,
} from "./types";

type SqlStatementKind = "select" | "dml";
interface SQLiteTableXInfoRow {
  name: string;
  type: string;
  notnull: number;
  dflt_value: string | null;
  pk: number;
  hidden: number;
}
interface SQLiteGeneratedColumnDetail {
  expression?: string;
  generatedKind?: GeneratedKind;
}
interface SQLiteTableMetadata {
  rows: SQLiteTableXInfoRow[];
  foreignKeyColumns: Set<string>;
  autoIncrementColumns: Set<string>;
  generatedColumns: Map<string, SQLiteGeneratedColumnDetail>;
}
const SQLITE_SELECT_STARTERS = new Set([
  "SELECT",
  "PRAGMA",
  "EXPLAIN",
  "VALUES",
]);
const SQLITE_DML_STARTERS = new Set(["INSERT", "UPDATE", "DELETE", "REPLACE"]);
const SQLITE_TIME_LITERAL_RE = /^\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?$/;
const SQLITE_DATETIME_LITERAL_RE =
  /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?: ?(?:Z|[+-]\d{2}:\d{2}))?$/i;
function approximateNumericFilterTolerance(rawValue: string): number {
  const fraction = /\.(\d+)/.exec(rawValue)?.[1].length ?? 0;
  const precision = Math.min(Math.max(fraction + 2, 6), 12);
  return 10 ** -precision;
}
function sqliteDeclaredTypeBase(typeName: string): string {
  return typeName.toUpperCase().trim().split("(")[0].trim();
}
function classifySql(sql: string): "select" | "dml" {
  const start = skipSqlTrivia(sql, 0);
  const leadingKeyword = readSqlKeyword(sql, start);
  if (!leadingKeyword) return "dml";
  if (leadingKeyword.keyword === "WITH") {
    return classifyWithStatement(sql, leadingKeyword.next);
  }
  return SQLITE_SELECT_STARTERS.has(leadingKeyword.keyword) ? "select" : "dml";
}
function splitSQLiteScript(sql: string): string[] {
  const stmts: string[] = [];
  let buf = "";
  let i = 0;
  const n = sql.length;
  while (i < n) {
    if (sql[i] === "-" && sql[i + 1] === "-") {
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
      buf += " ";
      continue;
    }
    if (sql[i] === "'" || sql[i] === '"' || sql[i] === "`" || sql[i] === "[") {
      const q = sql[i];
      const closing = q === "[" ? "]" : q;
      buf += sql[i++];
      while (i < n) {
        const c = sql[i];
        buf += c;
        i++;
        if (c === closing) {
          if (closing !== "]" && sql[i] === closing) {
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
function skipSqlTrivia(sql: string, start: number): number {
  let index = start;
  while (index < sql.length) {
    if (/\s/.test(sql[index])) {
      index++;
      continue;
    }
    if (sql[index] === "-" && sql[index + 1] === "-") {
      index += 2;
      while (index < sql.length && sql[index] !== "\n") {
        index++;
      }
      continue;
    }
    if (sql[index] === "/" && sql[index + 1] === "*") {
      index += 2;
      while (index < sql.length) {
        if (sql[index] === "*" && sql[index + 1] === "/") {
          index += 2;
          break;
        }
        index++;
      }
      continue;
    }
    break;
  }
  return index;
}
function readSqlKeyword(
  sql: string,
  start: number,
): {
  keyword: string;
  next: number;
} | null {
  const index = skipSqlTrivia(sql, start);
  const match = /^[A-Za-z_][A-Za-z0-9_]*/.exec(sql.slice(index));
  if (!match) return null;
  return {
    keyword: match[0].toUpperCase(),
    next: index + match[0].length,
  };
}
function nextSqlKeyword(
  sql: string,
  start: number,
): {
  keyword: string;
  next: number;
} | null {
  let index = start;
  while (index < sql.length) {
    index = skipSqlTrivia(sql, index);
    if (index >= sql.length) return null;
    const current = sql[index];
    if (
      current === '"' ||
      current === "'" ||
      current === "`" ||
      current === "["
    ) {
      index = skipQuotedSql(sql, index);
      continue;
    }
    const keyword = readSqlKeyword(sql, index);
    if (keyword) return keyword;
    index++;
  }
  return null;
}
function skipQuotedSql(sql: string, start: number): number {
  const opener = sql[start];
  const closer = opener === "[" ? "]" : opener;
  let index = start + 1;
  while (index < sql.length) {
    const current = sql[index];
    if (current === closer) {
      if (closer !== "]" && sql[index + 1] === closer) {
        index += 2;
        continue;
      }
      return index + 1;
    }
    index++;
  }
  return sql.length;
}
function skipBalancedParens(sql: string, start: number): number {
  if (sql[start] !== "(") return start;
  let depth = 0;
  let index = start;
  while (index < sql.length) {
    const current = sql[index];
    if (
      current === "'" ||
      current === '"' ||
      current === "`" ||
      current === "["
    ) {
      index = skipQuotedSql(sql, index);
      continue;
    }
    if (current === "-" && sql[index + 1] === "-") {
      index = skipSqlTrivia(sql, index);
      continue;
    }
    if (current === "/" && sql[index + 1] === "*") {
      index = skipSqlTrivia(sql, index);
      continue;
    }
    if (current === "(") {
      depth++;
    } else if (current === ")") {
      depth--;
      if (depth === 0) {
        return index + 1;
      }
    }
    index++;
  }
  return sql.length;
}
function skipSqlIdentifier(sql: string, start: number): number {
  const index = skipSqlTrivia(sql, start);
  const current = sql[index];
  if (
    current === '"' ||
    current === "'" ||
    current === "`" ||
    current === "["
  ) {
    return skipQuotedSql(sql, index);
  }
  const match = /^[A-Za-z_][A-Za-z0-9_]*/.exec(sql.slice(index));
  return match ? index + match[0].length : index;
}
function sqliteColumnDefinitionName(definition: string): string | null {
  const trimmed = definition.trim();
  if (!trimmed) {
    return null;
  }
  if (/^(?:constraint|primary|unique|check|foreign)\b/i.test(trimmed)) {
    return null;
  }
  const first = trimmed[0];
  if (first === '"' || first === "`" || first === "[") {
    const closer = first === "[" ? "]" : first;
    let value = "";
    let index = 1;
    while (index < trimmed.length) {
      const current = trimmed[index];
      const next = trimmed[index + 1];
      if (current === closer) {
        if (closer !== "]" && next === closer) {
          value += closer;
          index += 2;
          continue;
        }
        return value;
      }
      value += current;
      index++;
    }
    return null;
  }
  const match = /^[A-Za-z_][A-Za-z0-9_]*/.exec(trimmed);
  return match?.[0] ?? null;
}
function sqliteSplitCreateDefinitions(createSql: string): string[] {
  const openParenIndex = createSql.indexOf("(");
  if (openParenIndex < 0) {
    return [];
  }
  const closeParenIndex = skipBalancedParens(createSql, openParenIndex) - 1;
  if (closeParenIndex <= openParenIndex) {
    return [];
  }
  const body = createSql.slice(openParenIndex + 1, closeParenIndex);
  const definitions: string[] = [];
  let depth = 0;
  let start = 0;
  let index = 0;
  while (index < body.length) {
    const current = body[index];
    if (
      current === '"' ||
      current === "'" ||
      current === "`" ||
      current === "["
    ) {
      index = skipQuotedSql(body, index);
      continue;
    }
    if (current === "(") {
      depth++;
      index++;
      continue;
    }
    if (current === ")") {
      depth = Math.max(0, depth - 1);
      index++;
      continue;
    }
    if (current === "," && depth === 0) {
      const definition = body.slice(start, index).trim();
      if (definition) {
        definitions.push(definition);
      }
      start = index + 1;
    }
    index++;
  }
  const tail = body.slice(start).trim();
  if (tail) {
    definitions.push(tail);
  }
  return definitions;
}
function sqliteGeneratedKindFromHidden(
  hidden: number,
): GeneratedKind | undefined {
  if (hidden === 2) {
    return "virtual";
  }
  if (hidden === 3) {
    return "stored";
  }
  return undefined;
}
function parseSqliteGeneratedColumns(
  createSql: string,
): Map<string, SQLiteGeneratedColumnDetail> {
  const details = new Map<string, SQLiteGeneratedColumnDetail>();
  for (const definition of sqliteSplitCreateDefinitions(createSql)) {
    const columnName = sqliteColumnDefinitionName(definition);
    if (!columnName) {
      continue;
    }
    const asMatch = /\bAS\s*\(/i.exec(definition);
    if (!asMatch) {
      continue;
    }
    const openParenIndex = definition.indexOf("(", asMatch.index);
    if (openParenIndex < 0) {
      continue;
    }
    const closeParenIndex = skipBalancedParens(definition, openParenIndex) - 1;
    if (closeParenIndex <= openParenIndex) {
      continue;
    }
    const expression = definition
      .slice(openParenIndex + 1, closeParenIndex)
      .trim();
    const suffix = definition.slice(closeParenIndex + 1);
    details.set(columnName.toLowerCase(), {
      expression: expression || undefined,
      generatedKind: /\bSTORED\b/i.test(suffix)
        ? "stored"
        : /\bVIRTUAL\b/i.test(suffix)
          ? "virtual"
          : undefined,
    });
  }
  return details;
}
function classifyWithStatement(sql: string, start: number): SqlStatementKind {
  let index = skipSqlTrivia(sql, start);
  const maybeRecursive = readSqlKeyword(sql, index);
  if (maybeRecursive?.keyword === "RECURSIVE") {
    index = maybeRecursive.next;
  }
  while (index < sql.length) {
    index = skipSqlIdentifier(sql, index);
    if (index >= sql.length) return "dml";
    index = skipSqlTrivia(sql, index);
    if (sql[index] === "(") {
      index = skipBalancedParens(sql, index);
      index = skipSqlTrivia(sql, index);
    }
    const asKeyword = readSqlKeyword(sql, index);
    if (!asKeyword || asKeyword.keyword !== "AS") return "dml";
    index = skipSqlTrivia(sql, asKeyword.next);
    const materializedKeyword = readSqlKeyword(sql, index);
    if (materializedKeyword?.keyword === "NOT") {
      const nextKeyword = readSqlKeyword(sql, materializedKeyword.next);
      if (nextKeyword?.keyword === "MATERIALIZED") {
        index = skipSqlTrivia(sql, nextKeyword.next);
      }
    } else if (materializedKeyword?.keyword === "MATERIALIZED") {
      index = skipSqlTrivia(sql, materializedKeyword.next);
    }
    if (sql[index] !== "(") return "dml";
    index = skipBalancedParens(sql, index);
    index = skipSqlTrivia(sql, index);
    if (sql[index] === ",") {
      index++;
      continue;
    }
    break;
  }
  const mainKeyword = readSqlKeyword(sql, index);
  if (!mainKeyword) return "dml";
  if (SQLITE_SELECT_STARTERS.has(mainKeyword.keyword)) return "select";
  return SQLITE_DML_STARTERS.has(mainKeyword.keyword) ? "dml" : "dml";
}
function isUnsafeSQLiteScript(sql: string): boolean {
  let keyword = nextSqlKeyword(sql, 0);
  while (keyword) {
    if (keyword.keyword === "CREATE") {
      let nextKeyword = nextSqlKeyword(sql, keyword.next);
      if (
        nextKeyword?.keyword === "TEMP" ||
        nextKeyword?.keyword === "TEMPORARY"
      ) {
        nextKeyword = nextSqlKeyword(sql, nextKeyword.next);
      }
      if (nextKeyword?.keyword === "TRIGGER") {
        return true;
      }
    }
    keyword = nextSqlKeyword(sql, keyword.next);
  }
  return false;
}
function canSQLiteStatementReturnRows(sql: string): boolean {
  let keyword = nextSqlKeyword(sql, 0);
  while (keyword) {
    if (keyword.keyword === "RETURNING") {
      return true;
    }
    keyword = nextSqlKeyword(sql, keyword.next);
  }
  return false;
}
function normalizeSqliteTimeLiteral(value: string): string | null {
  const trimmed = value.trim();
  if (!SQLITE_TIME_LITERAL_RE.test(trimmed)) return null;
  const parts = trimmed.split(":");
  const hours = Number(parts[0]);
  const minutes = Number(parts[1]);
  const secondsPart = parts[2] ?? "00";
  const seconds = Number(secondsPart.split(".")[0]);
  if (hours > 23 || minutes > 59 || seconds > 59) return null;
  return parts.length === 2 ? `${trimmed}:00` : trimmed;
}
function normalizeSqliteDatetimeLiteral(value: string): string | null {
  const trimmed = value.trim().replace(/ ([+-]\d{2}:\d{2})$/, "$1");
  if (!SQLITE_DATETIME_LITERAL_RE.test(trimmed)) return null;
  const normalized = trimmed.replace("T", " ");
  const match =
    /^(\d{4}-\d{2}-\d{2}) (\d{2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?(?: ?(?:Z|[+-]\d{2}:\d{2}))?$/i.exec(
      normalized,
    );
  if (!match) return null;
  const hours = Number(match[2]);
  const minutes = Number(match[3]);
  const seconds = Number(match[4] ?? "00");
  if (hours > 23 || minutes > 59 || seconds > 59) return null;
  return normalized;
}
function parseSqliteBlobDisplayValue(val: string): Uint8Array | null {
  try {
    const parsed: unknown = JSON.parse(val.trim());
    if (Array.isArray(parsed)) {
      if (
        parsed.length > 0 &&
        parsed.every(
          (v) =>
            typeof v === "number" && Number.isInteger(v) && v >= 0 && v <= 255,
        )
      ) {
        return new Uint8Array(parsed as number[]);
      }
      return null;
    }
    if (parsed !== null && typeof parsed === "object") {
      const entries = Object.entries(parsed as Record<string, unknown>);
      if (
        entries.length > 0 &&
        entries.every(
          ([k, v]) =>
            /^\d+$/.test(k) &&
            typeof v === "number" &&
            Number.isInteger(v) &&
            v >= 0 &&
            v <= 255,
        )
      ) {
        const maxIdx = Math.max(...entries.map(([k]) => Number(k)));
        const bytes = new Uint8Array(maxIdx + 1);
        for (const [k, v] of entries) {
          bytes[Number(k)] = v as number;
        }
        return bytes;
      }
    }
    return null;
  } catch {
    return null;
  }
}
function invalidSqliteTemporalFilterError(
  columnName: string,
  typeName: "time" | "datetime",
): Error {
  return new Error(
    `[RapiDB Filter] Column ${columnName} expects a valid ${typeName} value.`,
  );
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
  private requireDb(): Database {
    if (!this.db?.isOpen) {
      throw new Error("[RapiDB] SQLite connection is not open");
    }
    return this.db;
  }
  private readTableMetadata(table: string): SQLiteTableMetadata {
    const db = this.requireDb();
    const safeName = table.replace(/"/g, '""');
    const rows = (
      db.all(
        `PRAGMA table_xinfo("${safeName}")`,
      ) as unknown as SQLiteTableXInfoRow[]
    ).filter((row) => row.hidden !== 1);
    const fkRows = db.all(`PRAGMA foreign_key_list("${safeName}")`) as {
      from: string;
    }[];
    const autoIncrementColumns = new Set<string>();
    let createSql = "";
    try {
      const master = db.all(
        `SELECT sql FROM sqlite_master WHERE type='table' AND name=?`,
        [table],
      ) as {
        sql: string;
      }[];
      createSql = master[0]?.sql ?? "";
      const re =
        /[`"[]?(\w+)[`"\]]?\s+INTEGER\b[^,)]*\bPRIMARY\s+KEY\b[^,)]*\bAUTOINCREMENT\b/gi;
      let match: RegExpExecArray | null;
      match = re.exec(createSql);
      while (match !== null) {
        autoIncrementColumns.add(match[1].toLowerCase());
        match = re.exec(createSql);
      }
    } catch {}
    const generatedColumns = parseSqliteGeneratedColumns(createSql);
    for (const row of rows) {
      const generatedKind = sqliteGeneratedKindFromHidden(row.hidden);
      if (!generatedKind) {
        continue;
      }
      const key = row.name.toLowerCase();
      const detail = generatedColumns.get(key) ?? {};
      generatedColumns.set(key, {
        ...detail,
        generatedKind: detail.generatedKind ?? generatedKind,
      });
    }
    return {
      rows,
      foreignKeyColumns: new Set(fkRows.map((row) => row.from)),
      autoIncrementColumns,
      generatedColumns,
    };
  }
  private toColumnMeta(
    row: SQLiteTableXInfoRow,
    metadata: SQLiteTableMetadata,
  ): ColumnMeta {
    const generatedDetail = metadata.generatedColumns.get(
      row.name.toLowerCase(),
    );
    const isComputed = generatedDetail !== undefined;
    const defaultValue = !isComputed
      ? (row.dflt_value ?? undefined)
      : undefined;
    return {
      name: row.name,
      type: row.type || "TEXT",
      nullable: row.notnull === 0,
      defaultValue,
      defaultKind:
        defaultValue === undefined
          ? undefined
          : this.inferDefaultKind(defaultValue),
      isComputed,
      computedExpression: generatedDetail?.expression,
      generatedKind: generatedDetail?.generatedKind,
      isPersisted:
        generatedDetail?.generatedKind === undefined
          ? undefined
          : generatedDetail.generatedKind === "stored",
      isPrimaryKey: row.pk > 0,
      primaryKeyOrdinal: row.pk > 0 ? row.pk : undefined,
      isForeignKey: metadata.foreignKeyColumns.has(row.name),
      isAutoIncrement: metadata.autoIncrementColumns.has(
        row.name.toLowerCase(),
      ),
    };
  }
  async disconnect(): Promise<void> {
    this.db?.close();
    this.db = null;
  }
  isConnected(): boolean {
    return this.db?.isOpen ?? false;
  }
  async listDatabases(): Promise<DatabaseInfo[]> {
    return [{ name: "main", schemas: [] }];
  }
  async listSchemas(_database: string): Promise<SchemaInfo[]> {
    return [{ name: "main" }];
  }
  async listObjects(_database: string, _schema: string): Promise<TableInfo[]> {
    const rows = this.requireDb().all(`SELECT name, type
       FROM sqlite_master
       WHERE type IN ('table','view')
         AND name NOT LIKE 'sqlite_%'
       ORDER BY type DESC, name`) as {
      name: string;
      type: string;
    }[];
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
    const metadata = this.readTableMetadata(table);
    return metadata.rows.map((row) => this.toColumnMeta(row, metadata));
  }
  override async describeColumns(
    _database: string,
    _schema: string,
    table: string,
  ): Promise<ColumnTypeMeta[]> {
    const metadata = this.readTableMetadata(table);
    return metadata.rows.map((row) => {
      const column = this.enrichColumn(this.toColumnMeta(row, metadata));
      const declaredType = sqliteDeclaredTypeBase(row.type);
      const isExplicitTemporal =
        declaredType === "TIME" ||
        declaredType === "DATETIME" ||
        declaredType === "TIMESTAMP";
      return {
        ...column,
        filterOperators: isExplicitTemporal
          ? ["eq", "neq", "between", "like", "is_null", "is_not_null"]
          : column.filterOperators,
      };
    });
  }
  async query(sql: string, params?: unknown[]): Promise<QueryResult> {
    const start = Date.now();
    if (params && params.length > 0) {
      return this._executeSingle(sql, params, start);
    }
    if (isUnsafeSQLiteScript(sql)) {
      this.requireDb().exec(sql);
      return {
        columns: [],
        rows: [],
        rowCount: 0,
        executionTimeMs: Date.now() - start,
      };
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
    const db = this.requireDb();
    if (kind === "select") {
      const rawRows = db.all(sql, bindValues) as Record<string, unknown>[];
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
    if (canSQLiteStatementReturnRows(sql)) {
      const returningRows = db.all(sql, bindValues) as Record<
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
      return {
        columns: [],
        rows: [],
        rowCount: 0,
        executionTimeMs: Date.now() - start,
      };
    }
    const info = db.run(sql, bindValues);
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
    const db = this.requireDb();
    const idxList = db.all(`PRAGMA index_list("${safeTable}")`) as {
      name: string;
      unique: number;
      origin: string;
    }[];
    const result: import("./types").IndexMeta[] = [];
    for (const idx of idxList) {
      const safeName = idx.name.replace(/"/g, '""');
      const cols = db.all(`PRAGMA index_info("${safeName}")`) as {
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
    const rows = this.requireDb().all(
      `PRAGMA foreign_key_list("${safeTable}")`,
    ) as {
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
    const row = this.requireDb().get(
      `SELECT sql FROM sqlite_master WHERE type IN ('table','view') AND name = ?`,
      [table],
    ) as {
      sql: string;
    } | null;
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
    const db = this.requireDb();
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
  mapTypeCategory(nativeType: string): TypeCategory {
    const ct = nativeType.toUpperCase().trim();
    const base = sqliteDeclaredTypeBase(nativeType);
    if (base === "" || base === "TEXT") return "text";
    if (base === "JSON") return "json";
    if (base === "UUID") return "uuid";
    if (
      base === "INTEGER" ||
      base === "INT" ||
      base === "BIGINT" ||
      base === "SMALLINT" ||
      base === "TINYINT" ||
      base === "MEDIUMINT"
    )
      return "integer";
    if (base === "REAL" || base === "DOUBLE" || base === "FLOAT")
      return "float";
    if (base === "NUMERIC" || base === "DECIMAL") return "decimal";
    if (base === "BOOLEAN" || base === "BOOL") return "boolean";
    if (base === "BLOB") return "binary";
    if (base === "DATE") return "date";
    if (base === "TIME") return "time";
    if (base === "DATETIME" || base === "TIMESTAMP") return "datetime";
    if (ct.includes("INT")) return "integer";
    if (ct.includes("CHAR") || ct.includes("TEXT") || ct.includes("CLOB"))
      return "text";
    if (ct.includes("REAL") || ct.includes("FLOA") || ct.includes("DOUB"))
      return "float";
    if (ct.includes("BLOB")) return "binary";
    return "other";
  }
  protected getValueSemantics(
    nativeType: string,
    _category: TypeCategory,
  ): ValueSemantics {
    const base = sqliteDeclaredTypeBase(nativeType);
    return base === "BOOLEAN" || base === "BOOL" ? "boolean" : "plain";
  }
  isDatetimeWithTime(nativeType: string): boolean {
    const base = sqliteDeclaredTypeBase(nativeType);
    return base === "DATETIME" || base === "TIMESTAMP";
  }
  override coerceInputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (typeof value === "string" && this.hasBooleanSemantics(column)) {
      const normalized = this.parseBooleanInput(value);
      if (normalized !== null) {
        return normalized ? 1 : 0;
      }
    }
    if (typeof value === "string" && column.category === "binary") {
      const parsedBlob = parseSqliteBlobDisplayValue(value);
      if (parsedBlob !== null) {
        return parsedBlob;
      }
    }
    return super.coerceInputValue(value, column);
  }
  override formatOutputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (this.hasBooleanSemantics(column)) {
      if (value === 1 || value === "1") return true;
      if (value === 0 || value === "0") return false;
    }
    return super.formatOutputValue(value, column);
  }
  override checkPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null {
    if (column.category === "integer") {
      return this.checkExactNumericPersistedEdit(
        column,
        expectedValue,
        { precision: null, scale: 0 },
        options,
      );
    }
    if (column.category === "decimal") {
      return this.checkExactNumericPersistedEdit(
        column,
        expectedValue,
        { precision: null, scale: null },
        options,
      );
    }
    if (column.category === "float") {
      return this.checkApproximateNumericPersistedEdit(
        column,
        expectedValue,
        15,
        options,
      );
    }
    if (column.category === "boolean") {
      return this.checkBooleanPersistedEdit(column, expectedValue, options);
    }
    if (column.category === "binary") {
      return this.checkBinaryPersistedEdit(column, expectedValue, options);
    }
    if (column.category === "json") {
      return this.checkJsonPersistedEdit(column, expectedValue, options);
    }
    if (column.category === "uuid") {
      return this.checkUuidPersistedEdit(column, expectedValue, options);
    }
    if (
      column.category === "text" ||
      column.category === "date" ||
      column.category === "time" ||
      column.category === "datetime"
    ) {
      return this.checkTextPersistedEdit(column, expectedValue, options);
    }
    return null;
  }
  override normalizeFilterValue(
    column: ColumnTypeMeta,
    operator: FilterOperator,
    value: string | [string, string] | undefined,
  ): string | [string, string] | undefined {
    const normalized = super.normalizeFilterValue(column, operator, value);
    if (normalized === undefined) {
      return normalized;
    }
    if (column.category === "time") {
      if (operator === "between" && Array.isArray(normalized)) {
        const startLiteral = normalizeSqliteTimeLiteral(normalized[0]);
        const endLiteral = normalizeSqliteTimeLiteral(normalized[1]);
        if (!startLiteral || !endLiteral) {
          throw invalidSqliteTemporalFilterError(column.name, "time");
        }
        return [startLiteral, endLiteral];
      }
      if (operator === "in" && typeof normalized === "string") {
        const parts = normalized
          .split(",")
          .map((part) => part.trim())
          .filter(Boolean);
        if (parts.length === 0) {
          throw invalidSqliteTemporalFilterError(column.name, "time");
        }
        const literals = parts.map((part) => {
          const literal = normalizeSqliteTimeLiteral(part);
          if (!literal) {
            throw invalidSqliteTemporalFilterError(column.name, "time");
          }
          return literal;
        });
        return literals.join(", ");
      }
      if (
        ["eq", "neq", "gt", "gte", "lt", "lte"].includes(operator) &&
        typeof normalized === "string"
      ) {
        const literal = normalizeSqliteTimeLiteral(normalized);
        if (literal) {
          return literal;
        }
        if (operator !== "eq" && operator !== "neq") {
          throw invalidSqliteTemporalFilterError(column.name, "time");
        }
        return normalized;
      }
    }
    if (column.category === "datetime") {
      if (operator === "between" && Array.isArray(normalized)) {
        const startLiteral = normalizeSqliteDatetimeLiteral(normalized[0]);
        const endLiteral = normalizeSqliteDatetimeLiteral(normalized[1]);
        if (!startLiteral || !endLiteral) {
          throw invalidSqliteTemporalFilterError(column.name, "datetime");
        }
        return [startLiteral, endLiteral];
      }
      if (operator === "in" && typeof normalized === "string") {
        const parts = normalized
          .split(",")
          .map((part) => part.trim())
          .filter(Boolean);
        if (parts.length === 0) {
          throw invalidSqliteTemporalFilterError(column.name, "datetime");
        }
        const literals = parts.map((part) => {
          const literal = normalizeSqliteDatetimeLiteral(part);
          if (!literal) {
            throw invalidSqliteTemporalFilterError(column.name, "datetime");
          }
          return literal;
        });
        return literals.join(", ");
      }
      if (
        ["eq", "neq", "gt", "gte", "lt", "lte"].includes(operator) &&
        typeof normalized === "string"
      ) {
        const literal = normalizeSqliteDatetimeLiteral(normalized);
        if (literal) {
          return literal;
        }
        if (operator !== "eq" && operator !== "neq") {
          throw invalidSqliteTemporalFilterError(column.name, "datetime");
        }
        return normalized;
      }
    }
    return normalized;
  }
  override buildFilterCondition(
    column: ColumnTypeMeta,
    operator: FilterOperator,
    value: string | [string, string] | undefined,
    _paramIndex: number,
  ): FilterConditionResult | null {
    const col = this.quoteIdentifier(column.name);
    if (operator === "is_null") return { sql: `${col} IS NULL`, params: [] };
    if (operator === "is_not_null")
      return { sql: `${col} IS NOT NULL`, params: [] };
    if (!column.filterable) return null;
    if (value === undefined) return null;
    const val = typeof value === "string" ? value.trim() : value;
    if (column.category === "array") {
      if (operator !== "like" && operator !== "ilike") {
        return null;
      }
      const arrayValue = typeof val === "string" ? val : val[0];
      return { sql: `${col} LIKE ?`, params: [`%${arrayValue}%`] };
    }
    const fallbackTemporalLike = (rawValue: string, negate = false) => ({
      sql: `${col} ${negate ? "NOT LIKE" : "LIKE"} ?`,
      params: [`%${rawValue}%`],
    });
    if (
      this.hasBooleanSemantics(column) &&
      (operator === "eq" || operator === "neq")
    ) {
      const strVal = (typeof val === "string" ? val : val[0]).toLowerCase();
      if (strVal === "true" || strVal === "false") {
        const boolVal = strVal === "true" ? 1 : 0;
        const op = operator === "neq" ? "!=" : "=";
        return { sql: `${col} ${op} ?`, params: [boolVal] };
      }
    }
    if (
      this.isNumericCategory(column.category) &&
      typeof val === "string" &&
      !Number.isNaN(Number(val)) &&
      val !== ""
    ) {
      if (
        column.category === "float" &&
        (operator === "eq" || operator === "neq")
      ) {
        const numericValue = Number(val);
        const tolerance = approximateNumericFilterTolerance(val);
        const toleranceExpr = "MAX(?, ABS(?) * ?)";
        const deltaExpr = `ABS(CAST(${col} AS REAL) - ?)`;
        return {
          sql:
            operator === "neq"
              ? `${deltaExpr} >= ${toleranceExpr}`
              : `${deltaExpr} < ${toleranceExpr}`,
          params: [numericValue, tolerance, numericValue, tolerance],
        };
      }
      const sqlOp = this.sqlOperator(operator);
      if (column.category === "integer" && /^-?\d+$/.test(val)) {
        const big = BigInt(val);
        if (
          big > BigInt(Number.MAX_SAFE_INTEGER) ||
          big < -BigInt(Number.MAX_SAFE_INTEGER)
        ) {
          return { sql: `${col} ${sqlOp} ?`, params: [big] };
        }
      }
      return { sql: `${col} ${sqlOp} ?`, params: [Number(val)] };
    }
    if (column.category === "date") {
      if (operator === "between" && Array.isArray(val)) {
        return {
          sql: `DATE(${col}) BETWEEN DATE(?) AND DATE(?)`,
          params: [val[0], val[1]],
        };
      }
      if (typeof val === "string" && operator === "in") {
        const parts = val
          .split(",")
          .map((part) => part.trim())
          .filter(Boolean);
        return {
          sql: `DATE(${col}) IN (${parts.map(() => "DATE(?)").join(", ")})`,
          params: parts,
        };
      }
      if (
        typeof val === "string" &&
        ["eq", "neq", "gt", "gte", "lt", "lte"].includes(operator)
      ) {
        const sqlOp = operator === "neq" ? "!=" : this.sqlOperator(operator);
        return { sql: `DATE(${col}) ${sqlOp} DATE(?)`, params: [val] };
      }
    }
    if (column.category === "time") {
      if (operator === "between" && Array.isArray(val)) {
        const startLiteral = normalizeSqliteTimeLiteral(val[0]);
        const endLiteral = normalizeSqliteTimeLiteral(val[1]);
        if (!startLiteral || !endLiteral) {
          throw invalidSqliteTemporalFilterError(column.name, "time");
        }
        return {
          sql: `TIME(${col}) BETWEEN TIME(?) AND TIME(?)`,
          params: [startLiteral, endLiteral],
        };
      }
      if (operator === "in" && typeof val === "string") {
        const parts = val
          .split(",")
          .map((part) => part.trim())
          .filter(Boolean);
        return {
          sql: `TIME(${col}) IN (${parts.map(() => "TIME(?)").join(", ")})`,
          params: parts,
        };
      }
      if (
        ["eq", "neq", "gt", "gte", "lt", "lte"].includes(operator) &&
        typeof val === "string"
      ) {
        const literal = normalizeSqliteTimeLiteral(val);
        if (literal) {
          const sqlOp = operator === "neq" ? "!=" : this.sqlOperator(operator);
          return { sql: `TIME(${col}) ${sqlOp} TIME(?)`, params: [literal] };
        }
        if (operator === "eq" || operator === "neq") {
          return fallbackTemporalLike(val, operator === "neq");
        }
      }
    }
    if (column.category === "datetime") {
      if (operator === "between" && Array.isArray(val)) {
        const startLiteral = normalizeSqliteDatetimeLiteral(val[0]);
        const endLiteral = normalizeSqliteDatetimeLiteral(val[1]);
        if (!startLiteral || !endLiteral) {
          throw invalidSqliteTemporalFilterError(column.name, "datetime");
        }
        return {
          sql: `DATETIME(${col}) BETWEEN DATETIME(?) AND DATETIME(?)`,
          params: [startLiteral, endLiteral],
        };
      }
      if (operator === "in" && typeof val === "string") {
        const parts = val
          .split(",")
          .map((part) => part.trim())
          .filter(Boolean);
        return {
          sql: `DATETIME(${col}) IN (${parts.map(() => "DATETIME(?)").join(", ")})`,
          params: parts,
        };
      }
      if (
        ["eq", "neq", "gt", "gte", "lt", "lte"].includes(operator) &&
        typeof val === "string"
      ) {
        const literal = normalizeSqliteDatetimeLiteral(val);
        if (literal) {
          const sqlOp = operator === "neq" ? "!=" : this.sqlOperator(operator);
          return {
            sql: `DATETIME(${col}) ${sqlOp} DATETIME(?)`,
            params: [literal],
          };
        }
        if (operator === "eq" || operator === "neq") {
          return fallbackTemporalLike(val, operator === "neq");
        }
      }
    }
    if (operator === "between" && Array.isArray(val)) {
      return { sql: `${col} BETWEEN ? AND ?`, params: [val[0], val[1]] };
    }
    if (operator === "in" && typeof val === "string") {
      const parts = val.split(",").map((s) => s.trim());
      return {
        sql: `${col} IN (${parts.map(() => "?").join(", ")})`,
        params: parts,
      };
    }
    const v = typeof val === "string" ? val : val[0];
    return { sql: `${col} LIKE ?`, params: [`%${v}%`] };
  }
}
