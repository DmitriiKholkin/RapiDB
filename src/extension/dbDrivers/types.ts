import type {
  ColumnMeta,
  ColumnTypeMeta,
  FilterOperator,
  ForeignKeyMeta,
  IndexMeta,
  TypeCategory,
} from "../../shared/tableTypes";

export {
  type ColumnMeta,
  type ColumnTypeMeta,
  type FilterExpression,
  type FilterOperator,
  type ForeignKeyMeta,
  type IndexMeta,
  NULL_SENTINEL,
  type ScalarFilterOperator,
  type TypeCategory,
} from "../../shared/tableTypes";

// ─── Shared regex constants (used by drivers and tableDataService) ───

export const ISO_DATETIME_RE =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?$/;

export const DATE_ONLY_RE = /^\d{4}-\d{2}-\d{2}$/;

export const DATETIME_SQL_RE =
  /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}:\d{2})?$/;

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
  normalizeFilterValue(
    column: ColumnTypeMeta,
    operator: FilterOperator,
    value: string | [string, string] | undefined,
  ): string | [string, string] | undefined;
  buildFilterCondition(
    column: ColumnTypeMeta,
    operator: FilterOperator,
    value: string | [string, string] | undefined,
    paramIndex: number,
  ): FilterConditionResult | null;
  buildInsertValueExpr(column: ColumnTypeMeta, paramIndex: number): string;
  buildSetExpr(column: ColumnTypeMeta, paramIndex: number): string;
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
const FLOAT_OPS: FilterOperator[] = [
  "eq",
  "neq",
  "between",
  "in",
  "is_null",
  "is_not_null",
];
const TEXT_OPS: FilterOperator[] = ["like", "in", "is_null", "is_not_null"];
const DATE_OPS: FilterOperator[] = ["eq", "like", "is_null", "is_not_null"];
const TEMPORAL_OPS: FilterOperator[] = ["like", "is_null", "is_not_null"];
const BOOL_OPS: FilterOperator[] = ["eq", "neq", "is_null", "is_not_null"];
const SEARCH_OPS: FilterOperator[] = ["like", "is_null", "is_not_null"];

export function filterOperatorsForCategory(
  cat: TypeCategory,
): FilterOperator[] {
  switch (cat) {
    case "integer":
    case "decimal":
      return NUMERIC_OPS;
    case "float":
      return FLOAT_OPS;
    case "text":
    case "json":
    case "uuid":
    case "enum":
      return TEXT_OPS;
    case "date":
      return DATE_OPS;
    case "time":
    case "datetime":
    case "interval":
      return TEMPORAL_OPS;
    case "boolean":
      return BOOL_OPS;
    case "binary":
    case "spatial":
    case "array":
    case "other":
      return SEARCH_OPS;
    case "lob":
    default:
      return [];
  }
}
