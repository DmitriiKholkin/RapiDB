import type {
  DataDbObjectKind,
  DbObjectKind,
  DdlOnlyDbObjectKind,
} from "../../shared/dbObjectKinds";
import { DB_OBJECT_KINDS } from "../../shared/dbObjectKinds";
import type { OperationCancellationContext } from "../../shared/safetyContracts";
import type {
  ColumnMeta,
  ColumnTypeMeta,
  FilterExpression,
  FilterOperator,
  ForeignKeyMeta,
  IndexMeta,
  MongoValueTypeHint,
  QueryColumnMeta,
  QueryMissingCell,
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
  type MongoValueTypeHint,
  NULL_SENTINEL,
  type QueryColumnMeta,
  type QueryMissingCell,
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
  routineIdentity?: string;
}
export interface QueryResult {
  columns: string[];
  columnMeta?: QueryColumnMeta[];
  rows: Record<string, unknown>[];
  responseMetadata?: Record<string, unknown>[];
  missingCells?: QueryMissingCell[];
  rowCount: number;
  executionTimeMs: number;
  affectedRows?: number;
  truncated?: boolean;
}
export interface DatabaseExecutionScope {
  database?: string;
}
export interface QueryExecutionOptions extends DatabaseExecutionScope {
  requestToken?: number;
  readOnly?: boolean;
  signal?: AbortSignal;
  deadline?: number;
  hardCap?: number;
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
export interface MutationSqlExpression {
  sql: string;
  params: unknown[];
  previewColumns?: ColumnTypeMeta[];
}

export type ReadOnlyQueryDecision =
  | { allowed: true }
  | { allowed: false; reason: string };

export type ReadOnlyQueryGuard = (queryText: string) => ReadOnlyQueryDecision;

export interface DriverCapabilities {
  tabularRead: "sql" | "nosql";
  schemaNamespaces?: "none";
  queryMode?: "sql" | "text";
  supportsMutations?: boolean;
  boundedQueryResults?: boolean;
  editorPresentation?: QueryEditorPresentation;
  isTableFilterError?: (message: string) => boolean;
  readOnlyQueryGuard?: ReadOnlyQueryGuard;
  schemaMutationGuard?: (queryText: string) => boolean;
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
  signal?: AbortSignal;
  deadline?: number;
}

export interface DriverTablePageResult {
  columns: ColumnTypeMeta[];
  rows: Record<string, unknown>[];
  mongoIdTypes?: Array<"objectId" | "string" | null>;
  mongoValueTypes?: Array<Record<string, MongoValueTypeHint>>;
  totalCount: number;
  executionTimeMs?: number;
}

export interface DriverTableExportRequest {
  database: string;
  schema: string;
  table: string;
  chunkSize: number;
}

export type DriverTableExportChunk = Pick<
  DriverTablePageResult,
  "columns" | "rows"
>;

export interface DriverUpdateRowsRequest {
  database: string;
  schema: string;
  table: string;
  updates: Array<{
    primaryKeys: Record<string, unknown>;
    changes: Record<string, unknown>;
    originalValues?: Record<string, unknown>;
    originalTypes?: Record<string, MongoValueTypeHint>;
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
  rowOutcomes?: import("../../shared/webviewContracts").DeleteRowOutcome[];
  updateRowOutcomes?: DriverUpdateRowOutcome[];
}
export interface DriverUpdateRowOutcome {
  rowIndex: number;
  status: "applied" | "not_applied" | "unknown";
}
export interface DriverOperationContext {
  signal: AbortSignal;
  deadline: number;
  onDeleteProgress?: (result: DriverMutationResult) => void;
}
export interface PaginationResult {
  sql: string;
  params: unknown[];
}
export interface PersistedEditCheckOptions {
  persistedValue: unknown;
  originalValue?: unknown;
}
export interface PersistedEditCheckResult {
  ok: boolean;
  shouldVerify: boolean;
  message?: string;
}
export interface IDBDriver {
  readonly supportsAtomicUpdateRows?: boolean;
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  isConnected(): boolean;
  getAutomaticReconnectBlockReason?(): string | undefined;
  cancelCurrentOperation?(
    context?: OperationCancellationContext,
  ): Promise<void> | void;
  recycleConnectionAfterTimeout?(
    context?: OperationCancellationContext,
  ): Promise<void> | void;
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
    routineIdentity?: string,
  ): Promise<string>;
  query(
    sql: string,
    params?: unknown[],
    operationContext?: QueryExecutionOptions,
  ): Promise<QueryResult>;
  readTablePage?(
    request: DriverTablePageRequest,
  ): Promise<DriverTablePageResult>;
  clearTableReadCache?(): void;
  exportTableChunks?(
    request: DriverTableExportRequest,
    signal?: AbortSignal,
  ): AsyncIterable<DriverTableExportChunk>;
  validateTableUpdates?(
    updates: readonly Pick<
      DriverUpdateRowsRequest["updates"][number],
      "changes" | "originalValues" | "originalTypes"
    >[],
  ): void;
  updateRows?(
    request: DriverUpdateRowsRequest,
    context?: DriverOperationContext,
  ): Promise<DriverMutationResult>;
  insertRow?(
    request: DriverInsertRowRequest,
    context?: DriverOperationContext,
  ): Promise<DriverMutationResult>;
  deleteRows?(
    request: DriverDeleteRowsRequest,
    context?: DriverOperationContext,
  ): Promise<DriverMutationResult>;
  buildMutationPreviewStatement?(
    operation: "insert" | "update" | "delete",
    database: string,
    schema: string,
    table: string,
    data: {
      primaryKeys?: Record<string, unknown>;
      changes?: Record<string, unknown>;
      originalValues?: Record<string, unknown>;
      originalTypes?: Record<string, MongoValueTypeHint>;
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
      originalValues?: Record<string, unknown>;
      originalTypes?: Record<string, MongoValueTypeHint>;
      values?: Record<string, unknown>;
      primaryKeyValuesList?: Array<Record<string, unknown>>;
    },
  ): Promise<string[]>;
  runTransaction(
    operations: TransactionOperation[],
    context?: TransactionContext,
    scope?: TransactionOptions,
  ): Promise<void>;
  getMutationAtomicityRisk?(
    database: string,
    schema: string,
    table: string,
  ): Promise<string | null>;
  quoteIdentifier(name: string): string;
  qualifiedTableName(database: string, schema: string, table: string): string;
  buildTableDataProjection?(columns: readonly ColumnTypeMeta[]): string;
  buildVerificationValueExpr?(column: ColumnTypeMeta): string;
  formatTableDataValue?(
    value: unknown,
    column: ColumnTypeMeta,
    projectionValues: Record<string, unknown>,
    columnIndex: number,
    columns?: readonly ColumnTypeMeta[],
  ): unknown;
  shouldFallbackTableDataProjection?(error: unknown): boolean;
  buildPagination(
    offset: number,
    limit: number,
    paramIndex: number,
  ): PaginationResult;
  buildOrderByDefault(cols: ColumnTypeMeta[], tableReference?: string): string;
  buildTableDataOrderExpression?(
    columnName: string,
    tableReference: string,
  ): string;
  coerceInputValue(value: unknown, column: ColumnTypeMeta): unknown;
  coerceTableEditValue?(
    value: unknown,
    column: ColumnTypeMeta,
    options?: { preserveNativeType?: boolean },
  ): unknown;
  coercePrimaryKeyValue?(value: unknown, column: ColumnTypeMeta): unknown;
  coerceOriginalValue?(value: unknown, column: ColumnTypeMeta): unknown;
  buildOriginalValueComparison?(
    column: ColumnTypeMeta,
    paramIndex: number,
  ): string;
  buildOriginalValueComparisonWithParams?(
    column: ColumnTypeMeta,
    value: unknown,
    paramIndex: number,
    tableAlias?: string,
  ): MutationSqlExpression | null;
  getUpdateTableAlias?(): string | null;
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
  buildCursorComparison?(
    column: ColumnTypeMeta,
    operator: "eq" | "gt" | "lt",
    rawValue: unknown,
    paramIndex: number,
  ): FilterConditionResult;
  buildInsertDefaultValuesSql(
    qualifiedTableName: string,
    columns?: readonly ColumnTypeMeta[],
  ): string;
  buildInsertValueExpr(column: ColumnTypeMeta, paramIndex: number): string;
  buildSetExpr(column: ColumnTypeMeta, paramIndex: number): string;
  buildMutationValueExpr?(
    column: ColumnTypeMeta,
    value: unknown,
    operation: "insert" | "update",
    originalValue?: unknown,
    paramIndex?: number,
  ): MutationSqlExpression | null;
  materializePreviewSql(sql: string, params?: readonly unknown[]): string;
}
export interface TransactionContext extends DriverOperationContext {}

export interface TransactionVerification {
  rowIndex: number;
  mutation?: "insert" | "update";
  sql: string;
  params: unknown[];
  values: Array<{
    column: ColumnTypeMeta;
    expectedValue: unknown;
    originalValue?: unknown;
  }>;
  identity?: {
    operationIndex: number;
    parameterIndexes: number[];
    allowNull?: boolean;
  };
}

export interface TransactionOptions extends DatabaseExecutionScope {
  verifications?: TransactionVerification[];
}

export interface TransactionOperation {
  sql: string;
  params?: unknown[];
  checkAffectedRows?: boolean;
  expectedAffectedRows?: number;
  captureIdentity?: {
    sql?: string;
    params?: unknown[];
    mysqlInsertId?: {
      sql: string;
      zero?: { columnName: string; keyIndex: number; guardSql: string };
    };
    mysqlKeyGuard?: { sql: string; params: unknown[] };
    oracleOutTypes?: Array<"string" | "buffer">;
    mssqlSelect?: string;
    mssqlInsertTarget?: { database: string; schema: string; table: string };
  };
}

export function assertTransactionAffectedRows(
  operation: TransactionOperation,
  affectedRows: number,
): void {
  if (operation.checkAffectedRows && affectedRows === 0) {
    throw new Error(
      "Row not found — the row may have been modified or deleted by another user",
    );
  }
  if (
    operation.expectedAffectedRows !== undefined &&
    affectedRows !== operation.expectedAffectedRows
  ) {
    throw new Error(
      `Mutation affected ${affectedRows} row(s); expected ${operation.expectedAffectedRows}. One or more rows may have been modified or deleted by another user.`,
    );
  }
}
const TEXT_OPS: FilterOperator[] = ["like"];
const INTERVAL_OPS: FilterOperator[] = ["eq", "neq"];
const UUID_OPS: FilterOperator[] = ["like", "in"];
const ENUM_OPS: FilterOperator[] = ["like", "in"];
const BOOL_OPS: FilterOperator[] = ["eq", "neq"];
const BINARY_OPS: FilterOperator[] = ["eq", "neq"];
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
      return INTERVAL_OPS;
    case "boolean":
      return BOOL_OPS;
    case "binary":
      return BINARY_OPS;
    case "spatial":
      return BINARY_OPS;
    case "array":
      return ARRAY_OPS;
    case "other":
      return SEARCH_OPS;
    default:
      return [];
  }
}
