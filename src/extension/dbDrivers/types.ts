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
  routineIdentity?: string;
}
export interface QueryResult {
  columns: string[];
  columnMeta?: QueryColumnMeta[];
  rows: Record<string, unknown>[];
  rowCount: number;
  executionTimeMs: number;
  affectedRows?: number;
  /** The selected result was drained, but rows beyond the collection budget were discarded. */
  truncated?: boolean;
}
export interface DatabaseExecutionScope {
  /** Omitted/empty means the connection's default database. */
  database?: string;
}
export interface QueryExecutionOptions extends DatabaseExecutionScope {
  requestToken?: number;
  readOnly?: boolean;
  /** Per-query cancellation; must not cancel a later request on the connection. */
  signal?: AbortSignal;
  /** Absolute deadline shared by every command in a query batch. */
  deadline?: number;
  /** Retained-row budget for boundedQueryResults drivers, not a mutation/drain limit. */
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

export type ReadOnlyQueryDecision =
  | { allowed: true }
  | { allowed: false; reason: string };

export type ReadOnlyQueryGuard = (queryText: string) => ReadOnlyQueryDecision;

export interface DriverCapabilities {
  tabularRead: "sql" | "nosql";
  /** No schema namespace: discover objects in a synthetic database-named scope. */
  schemaNamespaces?: "none";
  queryMode?: "sql" | "text";
  supportsMutations?: boolean;
  /** query(..., { hardCap }) bounds collection across every emitted result set. */
  boundedQueryResults?: boolean;
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
  signal?: AbortSignal;
  /** Absolute deadline supplied by the operation timeout wrapper. */
  deadline?: number;
}

export interface DriverTablePageResult {
  columns: ColumnTypeMeta[];
  rows: Record<string, unknown>[];
  /** BSON _id type for each MongoDB table row, aligned with rows. */
  mongoIdTypes?: Array<"objectId" | "string" | null>;
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
  /** Zero-based index in DriverUpdateRowsRequest.updates. */
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
  /** A lost stateful session must not be replaced by an implicit reconnect. */
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
  /** Full unfiltered/default-order export; owns cursor/snapshot cleanup. */
  exportTableChunks?(
    request: DriverTableExportRequest,
    signal?: AbortSignal,
  ): AsyncIterable<DriverTableExportChunk>;
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
  buildPagination(
    offset: number,
    limit: number,
    paramIndex: number,
  ): PaginationResult;
  buildOrderByDefault(cols: ColumnTypeMeta[]): string;
  coerceInputValue(value: unknown, column: ColumnTypeMeta): unknown;
  /** Validate and decode a table row locator without guessing from sampled types. */
  coercePrimaryKeyValue?(value: unknown, column: ColumnTypeMeta): unknown;
  coerceOriginalValue?(value: unknown, column: ColumnTypeMeta): unknown;
  buildOriginalValueComparison?(
    column: ColumnTypeMeta,
    paramIndex: number,
  ): string;
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
  /** Exact comparison of a raw query value in database sort order (no filter tolerance). */
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
  materializePreviewSql(sql: string, params?: readonly unknown[]): string;
}
export interface TransactionContext extends DriverOperationContext {}

/** Serializable so verification also runs inside the SQLite execution worker. */
export interface TransactionVerification {
  rowIndex: number;
  mutation?: "insert" | "update";
  sql: string;
  params: unknown[];
  values: Array<{ column: ColumnTypeMeta; expectedValue: unknown }>;
  /** Replace these positional parameters with the identity captured immediately
   * after this operation, not the last identity of the whole batch. */
  identity?: {
    operationIndex: number;
    parameterIndexes: number[];
    /** SQLite composite primary keys may contain NULL; the read must be NULL-safe. */
    allowNull?: boolean;
  };
}

export interface TransactionOptions extends DatabaseExecutionScope {
  /** Read and validate on the transaction's connection after DML, before COMMIT.
   * Any read/check failure must abort the entire transaction. */
  verifications?: TransactionVerification[];
}

export interface TransactionOperation {
  sql: string;
  params?: unknown[];
  checkAffectedRows?: boolean;
  expectedAffectedRows?: number;
  /** Capture returned __col_N keys, or execute this identity SELECT on the same
   * transaction connection immediately after DML. Serializable across SQLite IPC. */
  captureIdentity?: {
    sql?: string;
    params?: unknown[];
    mysqlInsertId?: {
      sql: string;
      /** A writable AUTO_INCREMENT PK input coerced to zero. The executor must
       * resolve generation using its own session mode for each operation. Literal
       * zero needs an absence/range-lock guard before trusting packet ID zero. */
      zero?: { columnName: string; keyIndex: number; guardSql: string };
    };
    /** For explicit non-auto MySQL keys: prove absence while holding serializable
     * key-range locks, so verification cannot accidentally select a preexisting row. */
    mysqlKeyGuard?: { sql: string; params: unknown[] };
    oracleOutTypes?: Array<"string" | "buffer">;
    /** Executed after @@ROWCOUNT is saved, within the same SQL batch/scope. */
    mssqlSelect?: string;
    /** OUTPUT fabricates rows for INSTEAD OF INSERT. Inspect this target on the
     * transaction connection under locks before trusting returned identities. */
    mssqlInsertTarget?: { database: string; schema: string; table: string };
  };
}

export function assertTransactionAffectedRows(
  operation: TransactionOperation,
  affectedRows: number,
): void {
  if (
    operation.expectedAffectedRows !== undefined &&
    affectedRows !== operation.expectedAffectedRows
  ) {
    throw new Error(
      `Mutation affected ${affectedRows} row(s); expected ${operation.expectedAffectedRows}. One or more rows may have been modified or deleted by another user.`,
    );
  }
  if (operation.checkAffectedRows && affectedRows === 0) {
    throw new Error(
      "Row not found — the row may have been modified or deleted by another user",
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
