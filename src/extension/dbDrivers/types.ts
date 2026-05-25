import type {
  DataDbObjectKind,
  DbObjectKind,
  DdlOnlyDbObjectKind,
} from "../../shared/dbObjectKinds";
import { DB_OBJECT_KINDS } from "../../shared/dbObjectKinds";
import type {
  ColumnMeta,
  ColumnTypeMeta,
  FilterExpression,
  FilterOperator,
  ForeignKeyMeta,
  GeneratedKind,
  IdentityGenerationKind,
  IndexMeta,
  QueryColumnMeta,
  TableConstraintMeta,
  TriggerMeta,
  TypeCategory,
} from "../../shared/tableTypes";
import type { QueryEditorPresentation } from "../../shared/webviewContracts";

export {
  type ColumnMeta,
  type ColumnTypeMeta,
  type FilterExpression,
  type FilterOperator,
  type ForeignKeyMeta,
  type GeneratedKind,
  type IdentityGenerationKind,
  type IndexMeta,
  NULL_SENTINEL,
  type QueryColumnMeta,
  type ScalarFilterOperator,
  type TableConstraintMeta,
  type TriggerMeta,
  type TypeCategory,
  type ValueSemantics,
} from "../../shared/tableTypes";
export const ISO_DATETIME_RE =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}(?::?\d{2})?)?$/;
export const DATE_ONLY_RE = /^\d{4}-\d{2}-\d{2}$/;
export const DATETIME_SQL_RE =
  /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}(?::?\d{2})?)?$/;
export interface TableInfo {
  schema: string;
  name: string;
  type: DbObjectKind;
}
export interface QueryResult {
  columns: string[];
  columnMeta?: QueryColumnMeta[];
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
export interface FilterConditionResult {
  sql: string;
  params: unknown[];
}

export type ReadOnlyQueryDecision =
  | { allowed: true }
  | { allowed: false; reason: string };

export type ReadOnlyQueryGuard = (queryText: string) => ReadOnlyQueryDecision;

export interface DriverCapabilities {
  tabularRead: "sql" | "nosql";
  queryMode?: "sql" | "text";
  supportsMutations?: boolean;
  editorPresentation?: QueryEditorPresentation;
  isTableFilterError?: (message: string) => boolean;
  readOnlyQueryGuard?: ReadOnlyQueryGuard;
}

export interface DriverStaticMetadata {
  manifest: DriverEntityManifest;
  capabilities?: DriverCapabilities;
  editorPresentation?: QueryEditorPresentation;
}

export type DriverTableSectionKind =
  | "columns"
  | "constraints"
  | "indexes"
  | "triggers";

export type DriverEntityAvailability = "supported" | "not_applicable";

export type DriverTableSectionOverridesByObjectKind = Readonly<
  Partial<
    Record<
      DataDbObjectKind,
      Partial<Record<DriverTableSectionKind, DriverEntityAvailability>>
    >
  >
>;

export interface DriverEntityManifest {
  dbObjectKinds: readonly DbObjectKind[];
  tableSections: Readonly<
    Record<DriverTableSectionKind, DriverEntityAvailability>
  >;
  tableSectionOverridesByObjectKind?: DriverTableSectionOverridesByObjectKind;
}

export const DEFAULT_DRIVER_ENTITY_MANIFEST: DriverEntityManifest = {
  dbObjectKinds: DB_OBJECT_KINDS,
  tableSections: {
    columns: "supported",
    constraints: "supported",
    indexes: "supported",
    triggers: "supported",
  },
};

export function resolveDriverTableSectionAvailability(
  manifest: DriverEntityManifest,
  objectKind: DataDbObjectKind,
  section: DriverTableSectionKind,
): DriverEntityAvailability {
  return (
    manifest.tableSectionOverridesByObjectKind?.[objectKind]?.[section] ??
    manifest.tableSections[section]
  );
}

export interface DriverSortConfig {
  column: string;
  direction: "asc" | "desc";
}

export interface DriverTablePageRequest {
  database: string;
  schema: string;
  table: string;
  page: number;
  pageSize: number;
  filters: FilterExpression[];
  sort: DriverSortConfig | null;
  skipCount: boolean;
}

export interface DriverTablePageResult {
  columns: ColumnTypeMeta[];
  rows: Record<string, unknown>[];
  totalCount: number;
}

export interface DriverUpdateRowsRequest {
  database: string;
  schema: string;
  table: string;
  updates: Array<{
    primaryKeys: Record<string, unknown>;
    changes: Record<string, unknown>;
  }>;
}

export interface DriverInsertRowRequest {
  database: string;
  schema: string;
  table: string;
  values: Record<string, unknown>;
}

export interface DriverDeleteRowsRequest {
  database: string;
  schema: string;
  table: string;
  primaryKeyValuesList: Record<string, unknown>[];
}

export interface DriverMutationResult {
  affectedRows: number;
}
export interface PaginationResult {
  sql: string;
  params: unknown[];
}
export interface PersistedEditCheckOptions {
  persistedValue: unknown;
}
export interface PersistedEditCheckResult {
  ok: boolean;
  shouldVerify: boolean;
  message?: string;
}
export interface IDBDriver {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  isConnected(): boolean;
  cancelCurrentOperation?(context?: {
    timeoutKind?: "connection" | "dbOperation";
    operationName?: string;
  }): Promise<void> | void;
  recycleConnectionAfterTimeout?(context?: {
    timeoutKind?: "connection" | "dbOperation";
    operationName?: string;
  }): Promise<void> | void;
  getEntityManifest?(): DriverEntityManifest;
  getCapabilities?(): DriverCapabilities;
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
  getConstraints(
    database: string,
    schema: string,
    table: string,
  ): Promise<TableConstraintMeta[]>;
  getTriggers(
    database: string,
    schema: string,
    table: string,
  ): Promise<TriggerMeta[] | null>;
  getConstraintDDL(
    database: string,
    schema: string,
    table: string,
    constraintName: string,
  ): Promise<string>;
  getIndexDDL(
    database: string,
    schema: string,
    table: string,
    indexName: string,
  ): Promise<string>;
  getTriggerDDL(
    database: string,
    schema: string,
    table: string,
    triggerName: string,
  ): Promise<string>;
  getCreateTableDDL(
    database: string,
    schema: string,
    table: string,
  ): Promise<string>;
  getObjectDefinition(
    database: string,
    schema: string,
    name: string,
    kind: DdlOnlyDbObjectKind,
  ): Promise<string | null>;
  getRoutineDefinition(
    database: string,
    schema: string,
    name: string,
    kind: "function" | "procedure",
  ): Promise<string>;
  query(sql: string, params?: unknown[]): Promise<QueryResult>;
  readTablePage?(
    request: DriverTablePageRequest,
  ): Promise<DriverTablePageResult>;
  updateRows?(request: DriverUpdateRowsRequest): Promise<DriverMutationResult>;
  insertRow?(request: DriverInsertRowRequest): Promise<DriverMutationResult>;
  deleteRows?(request: DriverDeleteRowsRequest): Promise<DriverMutationResult>;
  buildMutationPreviewStatement?(
    operation: "insert" | "update" | "delete",
    database: string,
    schema: string,
    table: string,
    data: {
      primaryKeys?: Record<string, unknown>;
      changes?: Record<string, unknown>;
      values?: Record<string, unknown>;
      primaryKeyValuesList?: Array<Record<string, unknown>>;
    },
  ): string;
  buildMutationPreviewStatements?(
    operation: "insert" | "update" | "delete",
    database: string,
    schema: string,
    table: string,
    data: {
      primaryKeys?: Record<string, unknown>;
      changes?: Record<string, unknown>;
      values?: Record<string, unknown>;
      primaryKeyValuesList?: Array<Record<string, unknown>>;
    },
  ): Promise<string[]>;
  runTransaction(operations: TransactionOperation[]): Promise<void>;
  getMutationAtomicityRisk?(
    database: string,
    schema: string,
    table: string,
  ): Promise<string | null>;
  quoteIdentifier(name: string): string;
  qualifiedTableName(database: string, schema: string, table: string): string;
  buildPagination(
    offset: number,
    limit: number,
    paramIndex: number,
  ): PaginationResult;
  buildOrderByDefault(cols: ColumnTypeMeta[]): string;
  coerceInputValue(value: unknown, column: ColumnTypeMeta): unknown;
  formatOutputValue(value: unknown, column: ColumnTypeMeta): unknown;
  checkPersistedEdit(
    column: ColumnTypeMeta,
    expectedValue: unknown,
    options?: PersistedEditCheckOptions,
  ): PersistedEditCheckResult | null;
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
  buildInsertDefaultValuesSql(
    qualifiedTableName: string,
    columns?: readonly ColumnTypeMeta[],
  ): string;
  buildInsertValueExpr(column: ColumnTypeMeta, paramIndex: number): string;
  buildSetExpr(column: ColumnTypeMeta, paramIndex: number): string;
  materializePreviewSql(sql: string, params?: readonly unknown[]): string;
}
export interface TransactionOperation {
  sql: string;
  params?: unknown[];
  checkAffectedRows?: boolean;
}
const TEXT_OPS: FilterOperator[] = ["like"];
const UUID_OPS: FilterOperator[] = ["like", "in"];
const ENUM_OPS: FilterOperator[] = ["like", "in"];
const BOOL_OPS: FilterOperator[] = ["eq", "neq"];
const SEARCH_OPS: FilterOperator[] = ["like"];
const ARRAY_OPS: FilterOperator[] = ["like"];
const NULL_ONLY_OPS: FilterOperator[] = ["is_null", "is_not_null"];
const EXTENDED_OPS: FilterOperator[] = [
  "eq",
  "neq",
  "gt",
  "gte",
  "lt",
  "lte",
  "between",
  "in",
];

export function composeFilterOperators(
  operators: readonly FilterOperator[],
  nullable: boolean,
): FilterOperator[] {
  const scalarOperators = operators.filter(
    (operator) => operator !== "is_null" && operator !== "is_not_null",
  );

  return nullable ? [...scalarOperators, ...NULL_ONLY_OPS] : scalarOperators;
}

export function resolveFilterOperators(
  cat: TypeCategory,
  options: { filterable: boolean; nullable: boolean },
): FilterOperator[] {
  if (!options.filterable) {
    return options.nullable ? [...NULL_ONLY_OPS] : [];
  }

  return composeFilterOperators(
    filterOperatorsForCategory(cat),
    options.nullable,
  );
}

export function filterOperatorsForCategory(
  cat: TypeCategory,
): FilterOperator[] {
  switch (cat) {
    case "integer":
      return EXTENDED_OPS;
    case "decimal":
      return EXTENDED_OPS;
    case "float":
      return EXTENDED_OPS;
    case "uuid":
      return UUID_OPS;
    case "text":
      return TEXT_OPS;
    case "json":
      return TEXT_OPS;
    case "enum":
      return ENUM_OPS;
    case "date":
      return EXTENDED_OPS;
    case "time":
      return EXTENDED_OPS;
    case "datetime":
      return EXTENDED_OPS;
    case "interval":
      return [];
    case "boolean":
      return BOOL_OPS;
    case "binary":
      return [];
    case "spatial":
      return [];
    case "array":
      return ARRAY_OPS;
    case "other":
      return SEARCH_OPS;
    default:
      return [];
  }
}
