import type {
  ColumnMeta,
  ColumnTypeMeta,
  DatabaseInfo,
  FilterConditionResult,
  FilterOperator,
  ForeignKeyMeta,
  IDBDriver,
  IndexMeta,
  PaginationResult,
  QueryResult,
  SchemaInfo,
  TableInfo,
  TransactionOperation,
  TypeCategory,
} from "./types";
import { filterOperatorsForCategory, NULL_SENTINEL } from "./types";

// ─── Shared datetime formatting helper ───

const pad2 = (n: number) => String(n).padStart(2, "0");

export function formatDatetimeForDisplay(val: unknown): string | null {
  if (val instanceof Date) {
    if (Number.isNaN(val.getTime())) return null;
    const ms = val.getUTCMilliseconds();
    const frac = ms > 0 ? `.${String(ms).padStart(3, "0")}` : "";
    return (
      `${val.getUTCFullYear()}-${pad2(val.getUTCMonth() + 1)}-${pad2(val.getUTCDate())} ` +
      `${pad2(val.getUTCHours())}:${pad2(val.getUTCMinutes())}:${pad2(val.getUTCSeconds())}${frac}`
    );
  }
  if (typeof val === "string") {
    const m =
      /^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})(\.\d+)?([+-]\d{2}(:\d{2})?|Z)?$/.exec(
        val,
      );
    if (m) {
      const [, date, time, rawFrac, tz] = m;
      let fracStr = "";
      if (rawFrac && rawFrac.length > 1) {
        const digits = rawFrac.slice(1).slice(0, 3).padEnd(3, "0");
        const msNum = parseInt(digits, 10);
        if (msNum > 0) {
          fracStr = `.${String(msNum).padStart(3, "0").replace(/0+$/, "")}`;
        }
      }
      return `${date} ${time}${fracStr}${tz ?? ""}`;
    }
  }
  return null;
}

export function isoToLocalDateStr(iso: string): string | null {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
}

export function hexFromBuffer(val: Buffer): string {
  return val.length === 0 ? "" : `\\x${val.toString("hex")}`;
}

export function parseHexToBuffer(value: string): Buffer {
  const stripped =
    value.startsWith("\\x") ||
    value.startsWith("\\X") ||
    value.startsWith("0x") ||
    value.startsWith("0X")
      ? value.slice(2)
      : value;
  if (/^[0-9a-fA-F]*$/.test(stripped)) {
    if (stripped.length % 2 !== 0) {
      throw new Error(
        `Invalid hex value: odd number of hex digits in "${value}". ` +
          "Each byte requires exactly 2 hex digits.",
      );
    }
    return Buffer.from(stripped, "hex");
  }
  throw new Error(`Invalid hex string: "${value}"`);
}

export function isHexLike(value: string): boolean {
  if (
    value.startsWith("\\x") ||
    value.startsWith("\\X") ||
    value.startsWith("0x") ||
    value.startsWith("0X")
  ) {
    return /^[0-9a-fA-F]*$/.test(value.slice(2));
  }
  return /^[0-9a-fA-F]+$/.test(value) && value.length % 2 === 0;
}

// ─── Abstract base driver ───

export abstract class BaseDBDriver implements IDBDriver {
  // ── Abstract methods each driver MUST implement ──
  abstract connect(): Promise<void>;
  abstract disconnect(): Promise<void>;
  abstract isConnected(): boolean;
  abstract listDatabases(): Promise<DatabaseInfo[]>;
  abstract listSchemas(database: string): Promise<SchemaInfo[]>;
  abstract listObjects(database: string, schema: string): Promise<TableInfo[]>;
  abstract describeTable(
    database: string,
    schema: string,
    table: string,
  ): Promise<ColumnMeta[]>;
  abstract getIndexes(
    database: string,
    schema: string,
    table: string,
  ): Promise<IndexMeta[]>;
  abstract getForeignKeys(
    database: string,
    schema: string,
    table: string,
  ): Promise<ForeignKeyMeta[]>;
  abstract getCreateTableDDL(
    database: string,
    schema: string,
    table: string,
  ): Promise<string>;
  abstract getRoutineDefinition(
    database: string,
    schema: string,
    name: string,
    kind: "function" | "procedure",
  ): Promise<string>;
  abstract query(sql: string, params?: unknown[]): Promise<QueryResult>;
  abstract runTransaction(operations: TransactionOperation[]): Promise<void>;

  // ── Abstract: each driver maps its native types to TypeCategory ──
  abstract mapTypeCategory(nativeType: string): TypeCategory;

  // ── Abstract: each driver determines boolean detection ──
  abstract isBooleanType(nativeType: string): boolean;

  // ── Abstract: each driver determines datetime-with-time detection ──
  abstract isDatetimeWithTime(nativeType: string): boolean;

  // ─── describeColumns: wraps describeTable + enriches ───

  async describeColumns(
    database: string,
    schema: string,
    table: string,
  ): Promise<ColumnTypeMeta[]> {
    const cols = await this.describeTable(database, schema, table);
    return cols.map((c) => this.enrichColumn(c));
  }

  protected enrichColumn(col: ColumnMeta): ColumnTypeMeta {
    const category = this.mapTypeCategory(col.type);
    const filterable = this.isFilterable(col.type, category);
    const editable = this.isEditable(col.type, category);
    return {
      ...col,
      category,
      nativeType: col.type,
      filterable,
      editable,
      filterOperators: filterable ? filterOperatorsForCategory(category) : [],
      isBoolean: this.isBooleanType(col.type),
    };
  }

  protected isFilterable(_nativeType: string, category: TypeCategory): boolean {
    return category !== "lob";
  }

  protected isEditable(_nativeType: string, category: TypeCategory): boolean {
    return category !== "lob";
  }

  // ─── SQL helpers ───

  quoteIdentifier(name: string): string {
    return `"${name.replace(/"/g, '""')}"`;
  }

  qualifiedTableName(_database: string, schema: string, table: string): string {
    const parts: string[] = [];
    if (schema) parts.push(this.quoteIdentifier(schema));
    parts.push(this.quoteIdentifier(table));
    return parts.join(".");
  }

  buildPagination(
    offset: number,
    limit: number,
    _paramIndex: number,
  ): PaginationResult {
    return {
      sql: `LIMIT ? OFFSET ?`,
      params: [limit, offset],
    };
  }

  buildOrderByDefault(cols: ColumnTypeMeta[]): string {
    const pkCols = cols.filter((c) => c.isPrimaryKey);
    if (pkCols.length === 0) return "";
    return `ORDER BY ${pkCols.map((c) => this.quoteIdentifier(c.name)).join(", ")}`;
  }

  buildInsertValueExpr(_column: ColumnTypeMeta, _paramIndex: number): string {
    return "?";
  }

  buildSetExpr(column: ColumnTypeMeta, _paramIndex: number): string {
    return `${this.quoteIdentifier(column.name)} = ?`;
  }

  // ─── Type-aware data helpers ───

  coerceInputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (value === null || value === undefined || value === "") return value;
    if (value === NULL_SENTINEL) return null;
    if (typeof value !== "string") return value;

    if (column.isBoolean) {
      const lower = value.toLowerCase();
      if (lower === "true" || lower === "1") return this.coerceBooleanTrue();
      if (lower === "false" || lower === "0") return this.coerceBooleanFalse();
    }

    if (column.category === "binary" && isHexLike(value)) {
      return parseHexToBuffer(value);
    }

    return value;
  }

  protected coerceBooleanTrue(): unknown {
    return true;
  }
  protected coerceBooleanFalse(): unknown {
    return false;
  }

  formatOutputValue(value: unknown, column: ColumnTypeMeta): unknown {
    if (value === null || value === undefined) return value;

    if (Buffer.isBuffer(value)) return hexFromBuffer(value);
    if (typeof value === "bigint") return value.toString();

    if (value instanceof Date) {
      if (column.category === "date") {
        return isoToLocalDateStr(value.toISOString()) ?? value;
      }
      return formatDatetimeForDisplay(value) ?? value;
    }

    if (value !== null && typeof value === "object") {
      return JSON.stringify(value);
    }

    if (this.isDatetimeWithTime(column.nativeType)) {
      const formatted = formatDatetimeForDisplay(value);
      if (formatted !== null) return formatted;
    }

    return value;
  }

  // ─── Filter condition building (default: CAST AS TEXT LIKE) ───

  buildFilterCondition(
    column: ColumnTypeMeta,
    operator: FilterOperator,
    value: string | [string, string] | undefined,
    paramIndex: number,
  ): FilterConditionResult | null {
    if (!column.filterable) return null;
    if (value === undefined) return null;

    const col = this.quoteIdentifier(column.name);
    const val = typeof value === "string" ? value.trim() : value;

    // Null checks
    if (operator === "is_null") return { sql: `${col} IS NULL`, params: [] };
    if (operator === "is_not_null")
      return { sql: `${col} IS NOT NULL`, params: [] };

    // Boolean
    if (column.isBoolean && (operator === "eq" || operator === "neq")) {
      const strVal = (typeof val === "string" ? val : val[0]).toLowerCase();
      if (strVal === "true" || strVal === "false") {
        return this.buildBooleanFilter(
          col,
          operator,
          strVal === "true",
          paramIndex,
        );
      }
    }

    // Numeric exact match
    if (
      this.isNumericCategory(column.category) &&
      typeof val === "string" &&
      !Number.isNaN(Number(val)) &&
      val !== ""
    ) {
      return this.buildNumericFilter(col, column, operator, val, paramIndex);
    }

    // Between
    if (operator === "between" && Array.isArray(val)) {
      return this.buildBetweenFilter(col, column, val, paramIndex);
    }

    // Default: text-based comparison
    return this.buildTextFilter(
      col,
      column,
      operator,
      typeof val === "string" ? val : val[0],
      paramIndex,
    );
  }

  protected isNumericCategory(cat: TypeCategory): boolean {
    return cat === "integer" || cat === "float" || cat === "decimal";
  }

  protected buildBooleanFilter(
    col: string,
    operator: FilterOperator,
    isTrue: boolean,
    _paramIndex: number,
  ): FilterConditionResult {
    const op = operator === "neq" ? "!=" : "=";
    return { sql: `${col} ${op} ?`, params: [isTrue ? 1 : 0] };
  }

  protected buildNumericFilter(
    col: string,
    _column: ColumnTypeMeta,
    operator: FilterOperator,
    val: string,
    _paramIndex: number,
  ): FilterConditionResult {
    const num = Number(val);
    const sqlOp = this.sqlOperator(operator);
    return { sql: `${col} ${sqlOp} ?`, params: [num] };
  }

  protected buildBetweenFilter(
    col: string,
    _column: ColumnTypeMeta,
    val: [string, string],
    _paramIndex: number,
  ): FilterConditionResult {
    return { sql: `${col} BETWEEN ? AND ?`, params: [val[0], val[1]] };
  }

  protected buildTextFilter(
    col: string,
    _column: ColumnTypeMeta,
    operator: FilterOperator,
    val: string,
    _paramIndex: number,
  ): FilterConditionResult {
    const sqlOp = this.sqlOperator(operator);
    if (operator === "like" || operator === "ilike") {
      return { sql: `CAST(${col} AS CHAR) LIKE ?`, params: [`%${val}%`] };
    }
    if (operator === "eq") {
      return { sql: `CAST(${col} AS CHAR) LIKE ?`, params: [`%${val}%`] };
    }
    if (operator === "neq") {
      return { sql: `CAST(${col} AS CHAR) NOT LIKE ?`, params: [`%${val}%`] };
    }
    if (operator === "in") {
      const parts = val.split(",").map((s) => s.trim());
      return {
        sql: `${col} IN (${parts.map(() => "?").join(", ")})`,
        params: parts,
      };
    }
    return { sql: `${col} ${sqlOp} ?`, params: [val] };
  }

  protected sqlOperator(op: FilterOperator): string {
    switch (op) {
      case "eq":
        return "=";
      case "neq":
        return "!=";
      case "gt":
        return ">";
      case "gte":
        return ">=";
      case "lt":
        return "<";
      case "lte":
        return "<=";
      default:
        return "=";
    }
  }
}
