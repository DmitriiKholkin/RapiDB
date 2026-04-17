// ─── Shared regex constants (used by drivers and tableDataService) ───

export const ISO_DATETIME_RE =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?$/;

export const DATE_ONLY_RE = /^\d{4}-\d{2}-\d{2}$/;

export const DATETIME_SQL_RE =
  /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}:\d{2})?$/;

export const NULL_SENTINEL = "\x00__NULL__\x00";

// ─── Type system ───

export type TypeCategory =
  | "text"
  | "integer"
  | "float"
  | "decimal"
  | "boolean"
  | "date"
  | "time"
  | "datetime"
  | "binary"
  | "json"
  | "uuid"
  | "spatial"
  | "interval"
  | "array"
  | "enum"
  | "lob"
  | "other";

export type FilterOperator =
  | "eq"
  | "neq"
  | "gt"
  | "gte"
  | "lt"
  | "lte"
  | "between"
  | "like"
  | "ilike"
  | "in"
  | "is_null"
  | "is_not_null";

export interface FilterExpression {
  column: string;
  operator: FilterOperator;
  value: string | [string, string];
}

// ─── Core metadata interfaces ───

export interface ColumnMeta {
  name: string;
  type: string;
  nullable: boolean;
  defaultValue?: string;
  isPrimaryKey: boolean;
  isForeignKey: boolean;
  isAutoIncrement?: boolean;
}

export interface ColumnTypeMeta extends ColumnMeta {
  category: TypeCategory;
  nativeType: string;
  filterable: boolean;
  editable: boolean;
  filterOperators: FilterOperator[];
  isBoolean: boolean;
}

export interface IndexMeta {
  name: string;
  columns: string[];
  unique: boolean;
  primary: boolean;
}

export interface ForeignKeyMeta {
  column: string;
  referencedSchema: string;
  referencedTable: string;
  referencedColumn: string;
  constraintName: string;
}

export interface TableInfo {
  schema: string;
  name: string;
  type: "table" | "view" | "function" | "procedure";
}

export interface QueryResult {
  columns: string[];
  rows: Record<string, unknown>[];
  rowCount: number;
  executionTimeMs: number;
  affectedRows?: number;
}

export function colKey(index: number): string {
  return `__col_${index}`;
}

export interface DatabaseInfo {
  name: string;
  schemas: SchemaInfo[];
}

export interface SchemaInfo {
  name: string;
}

// ─── Filter condition result (returned by driver.buildFilterCondition) ───

export interface FilterConditionResult {
  sql: string;
  params: unknown[];
}

// ─── Pagination result (returned by driver.buildPagination) ───

export interface PaginationResult {
  sql: string;
  params: unknown[];
}

// ─── Driver interface ───

export interface IDBDriver {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  isConnected(): boolean;
  listDatabases(): Promise<DatabaseInfo[]>;
  listSchemas(database: string): Promise<SchemaInfo[]>;
  listObjects(database: string, schema: string): Promise<TableInfo[]>;
  describeTable(
    database: string,
    schema: string,
    table: string,
  ): Promise<ColumnMeta[]>;
  describeColumns(
    database: string,
    schema: string,
    table: string,
  ): Promise<ColumnTypeMeta[]>;
  getIndexes(
    database: string,
    schema: string,
    table: string,
  ): Promise<IndexMeta[]>;
  getForeignKeys(
    database: string,
    schema: string,
    table: string,
  ): Promise<ForeignKeyMeta[]>;
  getCreateTableDDL(
    database: string,
    schema: string,
    table: string,
  ): Promise<string>;
  getRoutineDefinition(
    database: string,
    schema: string,
    name: string,
    kind: "function" | "procedure",
  ): Promise<string>;
  query(sql: string, params?: unknown[]): Promise<QueryResult>;
  runTransaction(operations: TransactionOperation[]): Promise<void>;

  // ── SQL building helpers ──
  quoteIdentifier(name: string): string;
  qualifiedTableName(database: string, schema: string, table: string): string;
  buildPagination(
    offset: number,
    limit: number,
    paramIndex: number,
  ): PaginationResult;
  buildOrderByDefault(cols: ColumnTypeMeta[]): string;

  // ── Type-aware data helpers ──
  coerceInputValue(value: unknown, column: ColumnTypeMeta): unknown;
  formatOutputValue(value: unknown, column: ColumnTypeMeta): unknown;
  buildFilterCondition(
    column: ColumnTypeMeta,
    operator: FilterOperator,
    value: string | [string, string],
    paramIndex: number,
  ): FilterConditionResult | null;
  buildInsertValueExpr(column: ColumnTypeMeta, paramIndex: number): string;
  buildSetExpr(column: ColumnTypeMeta, paramIndex: number): string;

  // ── Legacy plain-text filter (auto-detect operator) ──
  buildLegacyFilter(
    column: ColumnTypeMeta,
    rawValue: string,
    paramIndex: number,
  ): FilterConditionResult | null;
}

export interface TransactionOperation {
  sql: string;
  params?: unknown[];
  checkAffectedRows?: boolean;
}

// ─── Helpers for filter operators per TypeCategory ───

const NUMERIC_OPS: FilterOperator[] = [
  "eq",
  "neq",
  "gt",
  "gte",
  "lt",
  "lte",
  "between",
  "in",
  "is_null",
  "is_not_null",
];
const TEXT_OPS: FilterOperator[] = [
  "eq",
  "neq",
  "like",
  "ilike",
  "in",
  "is_null",
  "is_not_null",
];
const DATE_OPS: FilterOperator[] = [
  "eq",
  "neq",
  "gt",
  "gte",
  "lt",
  "lte",
  "between",
  "is_null",
  "is_not_null",
];
const BOOL_OPS: FilterOperator[] = ["eq", "neq", "is_null", "is_not_null"];
const NO_OPS: FilterOperator[] = ["is_null", "is_not_null"];

export function filterOperatorsForCategory(
  cat: TypeCategory,
): FilterOperator[] {
  switch (cat) {
    case "integer":
    case "float":
    case "decimal":
      return NUMERIC_OPS;
    case "text":
    case "json":
    case "uuid":
    case "enum":
      return TEXT_OPS;
    case "date":
    case "time":
    case "datetime":
    case "interval":
      return DATE_OPS;
    case "boolean":
      return BOOL_OPS;
    case "binary":
    case "spatial":
    case "lob":
    case "array":
    case "other":
      return NO_OPS;
    default:
      return NO_OPS;
  }
}
