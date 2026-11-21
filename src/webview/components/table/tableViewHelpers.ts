import type React from "react";
import type { MssqlSqlVariantRowIdentity } from "../../../shared/mssqlSqlVariantRowIdentity";
import type { MySQLTimestampRowIdentity } from "../../../shared/mysqlTimestampRowIdentity";
import type { OracleTemporalRowIdentity } from "../../../shared/oracleTemporalRowIdentity";
import type { PostgresArrayRowIdentity } from "../../../shared/postgresArrayRowIdentity";
import type { PostgresIntervalRowIdentity } from "../../../shared/postgresIntervalRowIdentity";
import type { SqlDeleteObservation } from "../../../shared/sqlDeleteObservation";
import type { SQLiteRowIdentity } from "../../../shared/sqliteRowIdentity";
import {
  type ColumnTypeMeta as ColumnMeta,
  deriveApplicableFilterDrafts,
  type FilterDraftMap,
  type FilterExpression,
  type FilterOperator,
  formatColumnDetailDescription,
  formatPrimaryKeyRoleLabel,
  isColumnReadOnly,
  type MongoValueTypeHint,
  NULL_SENTINEL,
} from "../../../shared/tableTypes";
import type { ApplyResultPayload } from "../../../shared/webviewContracts";
import type {
  PendingRestoreEntry as BasePendingRestoreEntry,
  EditTarget,
  InsertDraftRow,
  MutationSnapshot,
  PendingEdits,
  Row,
} from "../../types";
import { buildButtonStyle } from "../../utils/buttonStyles";
import { TOOLBAR_H } from "../../utils/layout";

export type PendingRestoreEntry = BasePendingRestoreEntry & {
  originalValues?: Map<string, unknown>;
  originalMissingFields?: Set<string>;
  originalTypes?: Map<string, MongoValueTypeHint>;
  baselinePresenceVersion?: 1;
  baselineMissing?: boolean;
  /** The backend rejected this row. A matching key on a new read is not consent to retarget it. */
  applyConflict?: true;
};
export { TOOLBAR_H };

export const PAGE_SIZES = [25, 100, 500, 1000] as const;
export const DEBOUNCE = 1000;
export const ROW_H = 26;
export const HEADER_H = 28;
export const FILTER_H = 30;
export const PREVIEW_DIALOG_EDITOR_H = "min(42vh, 360px)";
export const INSERT_DEFAULT_SENTINEL = "\x00__RAPIDB_INSERT_DEFAULT__\x00";
const LEGACY_INSERT_DEFAULT_SENTINEL = "__RAPIDB_INSERT_DEFAULT__";
export const MAX_DRAFT_ROWS = 100;
export const SR_ONLY_STYLE: React.CSSProperties = {
  position: "absolute",
  width: 1,
  height: 1,
  padding: 0,
  margin: -1,
  overflow: "hidden",
  clip: "rect(0, 0, 0, 0)",
  whiteSpace: "nowrap",
  border: 0,
};

export interface TableApplyStatus {
  tone: "error" | "warning";
  message: string;
}

export type TableSortState = {
  column: string;
  direction: "asc" | "desc";
} | null;

export interface FetchSnapshot {
  page: number;
  pageSize: number;
  sort: TableSortState;
  filters: FilterExpression[];
}

export interface TableFetchOptions {
  refreshMetadata?: boolean;
  refreshData?: boolean;
  conflictRefresh?: boolean;
}

export interface PendingRestoreState {
  entries: PendingRestoreEntry[];
}

export interface PersistedTableDraft {
  tableKey: string;
  restoreState: PendingRestoreState;
  newRows: InsertDraftRow[];
  mutation?: PersistedTableMutation;
}

export interface PersistedTableMutation {
  kind: "apply" | "delete";
  unknown: boolean;
  operationId?: string;
  applyConflict?: true;
  conflictView?: PersistedTableViewState;
}

export interface PersistedTableViewState {
  tableKey: string;
  page: number;
  pageSize: number;
  sort: TableSortState;
  filters: FilterDraftMap;
}

const TABLE_DRAFT_VERSION = 3;

interface SerializedTableDraft {
  draftVersion: typeof TABLE_DRAFT_VERSION;
  tableKey: string;
  restoreState: {
    entries: Array<{
      originalSignature: string;
      changes: Array<[string, unknown]>;
      originalValues?: Array<[string, unknown] | [string]>;
      originalMissingFields?: string[];
      originalTypes?: Array<[string, MongoValueTypeHint]>;
      baselinePresenceVersion?: 1;
      applyConflict?: true;
    }>;
  };
  newRows: Array<Record<string, { value: unknown } | { valueUndefined: true }>>;
  mutation?: PersistedTableMutation;
}

export interface PendingRestoreResult {
  pendingEdits: PendingEdits;
  unresolved: PendingRestoreEntry[];
}

export function serializeTableDraft(
  tableKey: string,
  restoreState: PendingRestoreState,
  newRows: readonly InsertDraftRow[],
  mutation?: PersistedTableMutation,
): SerializedTableDraft {
  return {
    draftVersion: TABLE_DRAFT_VERSION,
    tableKey,
    restoreState: {
      entries: restoreState.entries.map((entry) => ({
        originalSignature: entry.originalSignature,
        changes: [...entry.changes.entries()],
        ...(entry.originalValues
          ? {
              originalValues: [...entry.originalValues].map(
                ([name, value]): [string, unknown] | [string] =>
                  value === undefined ? [name] : [name, value],
              ),
            }
          : {}),
        ...(entry.originalMissingFields
          ? { originalMissingFields: [...entry.originalMissingFields] }
          : {}),
        ...(entry.originalTypes
          ? {
              originalTypes: [...entry.originalTypes].map(
                ([name, hint]): [string, MongoValueTypeHint] => [
                  name,
                  { ...hint },
                ],
              ),
            }
          : {}),
        ...(entry.baselinePresenceVersion === 1
          ? { baselinePresenceVersion: 1 as const }
          : {}),
        ...(entry.applyConflict && { applyConflict: true as const }),
      })),
    },
    newRows: newRows.map((row) =>
      Object.fromEntries(
        Object.entries(row).map(([name, cell]) => [
          name,
          cell.value === undefined ? { valueUndefined: true } : { ...cell },
        ]),
      ),
    ),
    ...(mutation ? { mutation: { ...mutation } } : {}),
  };
}

export function parsePersistedTableDraft(
  value: unknown,
  expectedTableKey: string,
): PersistedTableDraft | null {
  if (!isRecord(value) || value.tableKey !== expectedTableKey) return null;
  if (
    value.draftVersion !== undefined &&
    value.draftVersion !== 2 &&
    value.draftVersion !== TABLE_DRAFT_VERSION
  )
    return null;
  const migrateLegacyDefault = value.draftVersion === undefined;
  // Past insert drafts used a string to represent explicit NULL. Pending
  // updates already stored real null, and baseline data must stay untouched.
  const migrateInsertNull = value.draftVersion !== TABLE_DRAFT_VERSION;
  const state = value.restoreState;
  const newRows = value.newRows;
  if (
    !isRecord(state) ||
    !Array.isArray(state.entries) ||
    !Array.isArray(newRows) ||
    newRows.length > MAX_DRAFT_ROWS
  ) {
    return null;
  }

  const entries: PendingRestoreEntry[] = [];
  for (const entry of state.entries) {
    if (
      !isRecord(entry) ||
      typeof entry.originalSignature !== "string" ||
      (entry.applyConflict !== undefined && entry.applyConflict !== true) ||
      !Array.isArray(entry.changes)
    ) {
      return null;
    }
    const changes = new Map<string, unknown>();
    for (const change of entry.changes) {
      if (
        !Array.isArray(change) ||
        change.length !== 2 ||
        typeof change[0] !== "string"
      ) {
        return null;
      }
      changes.set(change[0], change[1]);
    }
    let originalValues: Map<string, unknown> | undefined;
    if (entry.originalValues !== undefined) {
      if (!Array.isArray(entry.originalValues)) return null;
      originalValues = new Map();
      for (const original of entry.originalValues) {
        if (
          !Array.isArray(original) ||
          (original.length !== 1 && original.length !== 2) ||
          typeof original[0] !== "string"
        )
          return null;
        originalValues.set(original[0], original[1]);
      }
    }
    let originalMissingFields: Set<string> | undefined;
    if (
      entry.baselinePresenceVersion !== undefined &&
      entry.baselinePresenceVersion !== 1
    )
      return null;
    if (entry.originalMissingFields !== undefined) {
      if (!Array.isArray(entry.originalMissingFields) || !originalValues)
        return null;
      originalMissingFields = new Set();
      for (const name of entry.originalMissingFields) {
        if (
          typeof name !== "string" ||
          !changes.has(name) ||
          !originalValues.has(name) ||
          originalValues.get(name) !== undefined ||
          originalMissingFields.has(name)
        )
          return null;
        originalMissingFields.add(name);
      }
    }
    if (entry.baselinePresenceVersion === 1 && !originalMissingFields)
      return null;
    let originalTypes: Map<string, MongoValueTypeHint> | undefined;
    if (entry.originalTypes !== undefined) {
      if (!Array.isArray(entry.originalTypes) || !originalValues) return null;
      originalTypes = new Map();
      for (const original of entry.originalTypes) {
        if (
          !Array.isArray(original) ||
          original.length !== 2 ||
          typeof original[0] !== "string" ||
          !changes.has(original[0]) ||
          originalTypes.has(original[0]) ||
          !isRecord(original[1]) ||
          typeof original[1].category !== "string" ||
          typeof original[1].nativeType !== "string" ||
          (original[1].bsonSubtype !== undefined &&
            typeof original[1].bsonSubtype !== "number")
        )
          return null;
        originalTypes.set(original[0], {
          ...(original[1] as unknown as MongoValueTypeHint),
        });
      }
    }
    entries.push({
      originalSignature: entry.originalSignature,
      changes,
      originalValues,
      originalMissingFields,
      originalTypes,
      baselinePresenceVersion: entry.baselinePresenceVersion as 1 | undefined,
      baselineMissing: originalValues === undefined,
      ...(entry.applyConflict === true && { applyConflict: true }),
    });
  }

  const parsedRows: InsertDraftRow[] = [];
  for (const row of newRows) {
    if (!isRecord(row)) return null;
    const cells: Array<[string, InsertDraftRow[string]]> = [];
    for (const [name, cell] of Object.entries(row)) {
      if (!isRecord(cell)) return null;
      if (Object.hasOwn(cell, "value")) {
        cells.push([
          name,
          {
            value:
              migrateInsertNull && cell.value === NULL_SENTINEL
                ? null
                : migrateLegacyDefault &&
                    cell.value === LEGACY_INSERT_DEFAULT_SENTINEL
                  ? INSERT_DEFAULT_SENTINEL
                  : cell.value,
          },
        ]);
      } else if (
        Object.hasOwn(cell, "valueUndefined") &&
        cell.valueUndefined === true
      ) {
        cells.push([name, { value: undefined }]);
      } else {
        return null;
      }
    }
    parsedRows.push(Object.fromEntries(cells));
  }

  let mutation: PersistedTableMutation | undefined;
  if (value.mutation !== undefined) {
    const marker = value.mutation;
    if (
      !isRecord(marker) ||
      (marker.kind !== "apply" && marker.kind !== "delete") ||
      typeof marker.unknown !== "boolean" ||
      (marker.applyConflict !== undefined &&
        (marker.applyConflict !== true ||
          marker.kind !== "apply" ||
          marker.unknown)) ||
      (marker.operationId !== undefined &&
        typeof marker.operationId !== "string")
    )
      return null;
    const conflictView = parsePersistedTableViewState(
      marker.conflictView,
      expectedTableKey,
    );
    if (
      marker.conflictView !== undefined &&
      (!conflictView || marker.applyConflict !== true)
    )
      return null;
    mutation = {
      kind: marker.kind,
      unknown: marker.unknown,
      ...(typeof marker.operationId === "string"
        ? { operationId: marker.operationId }
        : {}),
      ...(conflictView && { conflictView }),
      ...(marker.applyConflict === true && { applyConflict: true }),
    };
  }

  return {
    tableKey: expectedTableKey,
    restoreState: { entries },
    newRows: parsedRows,
    ...(mutation ? { mutation } : {}),
  };
}

export function parsePersistedTableViewState(
  value: unknown,
  expectedTableKey: string,
): PersistedTableViewState | null {
  if (!isRecord(value) || value.tableKey !== expectedTableKey) return null;
  const { page, pageSize, sort, filters } = value;
  if (
    typeof page !== "number" ||
    !Number.isSafeInteger(page) ||
    page < 1 ||
    typeof pageSize !== "number" ||
    !(PAGE_SIZES as readonly number[]).includes(pageSize) ||
    !isRecord(filters)
  ) {
    return null;
  }

  let parsedSort: TableSortState = null;
  if (sort !== null) {
    if (
      !isRecord(sort) ||
      typeof sort.column !== "string" ||
      (sort.direction !== "asc" && sort.direction !== "desc")
    ) {
      return null;
    }
    parsedSort = { column: sort.column, direction: sort.direction };
  }

  const operators = new Set<FilterOperator>([
    "eq",
    "neq",
    "gt",
    "gte",
    "lt",
    "lte",
    "between",
    "like",
    "ilike",
    "in",
    "is_null",
    "is_not_null",
  ]);
  const parsedFilters: Array<[string, FilterDraftMap[string]]> = [];
  for (const [column, rawDraft] of Object.entries(filters)) {
    if (!isRecord(rawDraft) || typeof rawDraft.operator !== "string") {
      return null;
    }
    const operator = rawDraft.operator as FilterOperator;
    if (!operators.has(operator)) return null;
    if (operator === "is_null" || operator === "is_not_null") {
      parsedFilters.push([column, { operator }]);
    } else if (operator === "between") {
      if (
        !Array.isArray(rawDraft.value) ||
        rawDraft.value.length !== 2 ||
        !rawDraft.value.every((entry) => typeof entry === "string")
      ) {
        return null;
      }
      parsedFilters.push([
        column,
        {
          operator,
          value: [rawDraft.value[0], rawDraft.value[1]],
        },
      ]);
    } else {
      if (typeof rawDraft.value !== "string") return null;
      parsedFilters.push([column, { operator, value: rawDraft.value }]);
    }
  }

  return {
    tableKey: expectedTableKey,
    page,
    pageSize,
    sort: parsedSort,
    filters: Object.fromEntries(parsedFilters),
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

export function getInitialPageSize(defaultPageSize?: number): number {
  return defaultPageSize !== undefined &&
    (PAGE_SIZES as readonly number[]).includes(defaultPageSize)
    ? defaultPageSize
    : PAGE_SIZES[0];
}

export function tableButtonStyle(
  variant: "primary" | "ghost" | "danger" | "warning" = "ghost",
  disabled = false,
): React.CSSProperties {
  return buildButtonStyle(variant, { disabled, size: "sm" });
}

export function canEditColumn(column?: ColumnMeta): column is ColumnMeta {
  return !!column && !isColumnReadOnly(column);
}

export function canOpenColumn(
  column: ColumnMeta | undefined,
  rowKind: "persisted" | "draft",
): column is ColumnMeta {
  return !!column && (rowKind === "persisted" || canEditColumn(column));
}

export function clonePendingEdits(pendingEdits: PendingEdits): PendingEdits {
  return new Map(
    [...pendingEdits.entries()].map(([rowIdx, columnMap]) => [
      rowIdx,
      new Map(columnMap),
    ]),
  );
}

export function createInsertDraft(
  columns: readonly ColumnMeta[],
): InsertDraftRow {
  return Object.fromEntries(
    columns.map((column) => [
      column.name,
      {
        value: INSERT_DEFAULT_SENTINEL,
      },
    ]),
  );
}

export function keyIconColor(role: ColumnMeta["primaryKeyRole"]): string {
  return role === "sort"
    ? "var(--vscode-textLink-foreground, #2f6f9f)"
    : "var(--vscode-editorWarning-foreground, #8f5b00)";
}

export function buildColumnHeaderTitle(column: ColumnMeta): string {
  return [
    formatColumnDetailDescription(column),
    column.isPrimaryKey
      ? (formatPrimaryKeyRoleLabel(column.primaryKeyRole) ?? "Primary key")
      : undefined,
    column.isForeignKey ? "Foreign key" : undefined,
  ]
    .filter((value): value is string => Boolean(value))
    .join("\n");
}

export function buildActiveFilterDrafts(
  columns: readonly ColumnMeta[],
  drafts: FilterDraftMap,
): FilterDraftMap {
  return deriveApplicableFilterDrafts(columns, drafts);
}

export function getFilterDraftValidationError(
  columns: readonly (Pick<ColumnMeta, "name" | "filterable"> & {
    filterOperators: readonly FilterOperator[];
  })[],
  drafts: FilterDraftMap | null | undefined,
): string | null {
  if (!drafts) return null;

  for (const column of columns) {
    const draft = drafts[column.name];
    if (!draft || !column.filterable) continue;

    if (
      draft.operator === "between" &&
      column.filterOperators.includes("between") &&
      (!draft.value[0].trim() || !draft.value[1].trim())
    ) {
      return `The Between filter for ${JSON.stringify(column.name)} requires both bounds; incomplete criteria are ignored.`;
    }

    if (
      draft.operator === "in" &&
      column.filterOperators.includes("in") &&
      !draft.value.split(",").some((value) => value.trim().length > 0)
    ) {
      return `The In list filter for ${JSON.stringify(column.name)} requires at least one value; incomplete criteria are ignored.`;
    }
  }

  return null;
}

export function buildInsertValues(
  draft: InsertDraftRow,
): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(draft)
      .filter(([, cell]) => cell.value !== INSERT_DEFAULT_SENTINEL)
      .map(([columnName, cell]) => [columnName, cell.value]),
  );
}

export function stablePrimaryKeyPart(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => stablePrimaryKeyPart(item));
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (typeof value === "bigint") {
    return value.toString();
  }

  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, entryValue]) => [key, stablePrimaryKeyPart(entryValue)]),
    );
  }

  return value;
}

// The committed row object owns its identity. Keeping it off the object avoids
// exposing it as a database column and keeps old/new pages distinct in recovery.
const sqlDeleteObservations = new WeakMap<Row, SqlDeleteObservation>();
const noSqlDeleteObservationIds = new WeakMap<Row, string>();

export function registerNoSqlDeleteObservationId(row: Row, id: string): void {
  noSqlDeleteObservationIds.set(row, id);
}

export function readNoSqlDeleteObservationId(row: Row): string | null {
  return noSqlDeleteObservationIds.get(row) ?? null;
}

export function registerSqlDeleteObservation(
  row: Row,
  observation: SqlDeleteObservation,
): void {
  sqlDeleteObservations.set(row, observation);
}

export function readSqlDeleteObservation(row: Row) {
  return sqlDeleteObservations.get(row) ?? null;
}

const sqliteRowIdentities = new WeakMap<Row, SQLiteRowIdentity>();
const postgresArrayRowIdentities = new WeakMap<Row, PostgresArrayRowIdentity>();
const postgresIntervalRowIdentities = new WeakMap<
  Row,
  PostgresIntervalRowIdentity
>();
const mysqlTimestampRowIdentities = new WeakMap<
  Row,
  MySQLTimestampRowIdentity
>();
const mssqlSqlVariantRowIdentities = new WeakMap<
  Row,
  MssqlSqlVariantRowIdentity
>();
const oracleTemporalRowIdentities = new WeakMap<
  Row,
  OracleTemporalRowIdentity
>();

export function registerSQLiteRowIdentity(
  row: Row,
  identity: SQLiteRowIdentity,
): void {
  sqliteRowIdentities.set(row, identity);
}

export function registerPostgresArrayRowIdentity(
  row: Row,
  identity: PostgresArrayRowIdentity,
): void {
  postgresArrayRowIdentities.set(row, identity);
}

export function registerPostgresIntervalRowIdentity(
  row: Row,
  identity: PostgresIntervalRowIdentity,
): void {
  postgresIntervalRowIdentities.set(row, identity);
}

export function registerMySQLTimestampRowIdentity(
  row: Row,
  identity: MySQLTimestampRowIdentity,
): void {
  mysqlTimestampRowIdentities.set(row, identity);
}

export function registerMssqlSqlVariantRowIdentity(
  row: Row,
  identity: MssqlSqlVariantRowIdentity,
): void {
  mssqlSqlVariantRowIdentities.set(row, identity);
}

export function registerOracleTemporalRowIdentity(
  row: Row,
  identity: OracleTemporalRowIdentity,
): void {
  oracleTemporalRowIdentities.set(row, identity);
}

function storedRowKey(row: Row, name: string): unknown {
  const sqliteIdentity = sqliteRowIdentities.get(row);
  if (sqliteIdentity) {
    return Object.hasOwn(sqliteIdentity, name) ? sqliteIdentity[name] : null;
  }
  const postgresIdentity = postgresArrayRowIdentities.get(row);
  if (postgresIdentity && Object.hasOwn(postgresIdentity, name)) {
    return postgresIdentity[name];
  }
  const intervalIdentity = postgresIntervalRowIdentities.get(row);
  if (intervalIdentity && Object.hasOwn(intervalIdentity, name)) {
    return intervalIdentity[name];
  }
  const mysqlIdentity = mysqlTimestampRowIdentities.get(row);
  if (mysqlIdentity && Object.hasOwn(mysqlIdentity, name)) {
    return mysqlIdentity[name];
  }
  const oracleIdentity = oracleTemporalRowIdentities.get(row);
  if (oracleIdentity && Object.hasOwn(oracleIdentity, name)) {
    return oracleIdentity[name];
  }
  const mssqlIdentity = mssqlSqlVariantRowIdentities.get(row);
  return mssqlIdentity && Object.hasOwn(mssqlIdentity, name)
    ? mssqlIdentity[name]
    : row[name];
}

export function rowMutationPrimaryKeys(
  row: Row,
  primaryKeyColumns: readonly string[],
  mongoIdType: "objectId" | "string" | null | undefined,
  mongoRowIdentity: boolean,
): Record<string, unknown> {
  return Object.fromEntries(
    primaryKeyColumns.map((name) => [
      name,
      name === "_id" && mongoRowIdentity
        ? {
            $rapidbMongoId: {
              type: mongoIdType ?? "unsupported",
              value: row[name],
            },
          }
        : storedRowKey(row, name),
    ]),
  );
}

const rowUpdateBlockReasons = new WeakMap<Row, string>();

export function registerRowUpdateBlockReason(row: Row, reason: string): void {
  rowUpdateBlockReasons.set(row, reason);
}

export function readRowUpdateBlockReason(row: Row | undefined): string | null {
  return (row && rowUpdateBlockReasons.get(row)) ?? null;
}

export function rowMutationBlockReason(
  row: Row | undefined,
  primaryKeyColumns: readonly string[],
  mongoIdType?: "objectId" | "string" | null,
  mongoRowIdentity = mongoIdType !== undefined,
): string | null {
  if (!row || primaryKeyColumns.length === 0) {
    return "This row cannot be edited or deleted: a complete primary key is required.";
  }
  for (const name of primaryKeyColumns) {
    const intervalIdentity = postgresIntervalRowIdentities.get(row);
    if (
      intervalIdentity &&
      Object.hasOwn(intervalIdentity, name) &&
      intervalIdentity[name] === null
    ) {
      return `This row cannot be edited or deleted: the exact identity for PostgreSQL interval primary key ${name} is unavailable. Refresh the table and retry.`;
    }
    const oracleIdentity = oracleTemporalRowIdentities.get(row);
    if (
      oracleIdentity &&
      Object.hasOwn(oracleIdentity, name) &&
      oracleIdentity[name] === null
    ) {
      return `This row cannot be edited or deleted: the exact identity for Oracle temporal primary key ${name} is unavailable. Refresh the table or use the Query Editor with an explicit era.`;
    }
    const variantIdentity = mssqlSqlVariantRowIdentities.get(row);
    if (
      variantIdentity &&
      Object.hasOwn(variantIdentity, name) &&
      variantIdentity[name] === null
    ) {
      return `This row cannot be edited or deleted: the exact identity for SQL Server sql_variant primary key ${name} is unavailable. Refresh the table or use the Query Editor with an explicitly typed key.`;
    }
    const key = storedRowKey(row, name);
    if (
      !Object.hasOwn(row, name) ||
      row[name] === null ||
      row[name] === undefined ||
      key === null ||
      key === undefined
    ) {
      return `This row cannot be edited or deleted: primary key ${name} is NULL or missing, so the row cannot be targeted safely.`;
    }
    if (name === "_id" && mongoRowIdentity) {
      const value = row[name];
      if (
        typeof value !== "string" ||
        (mongoIdType !== "string" &&
          (mongoIdType !== "objectId" || !/^[0-9a-f]{24}$/i.test(value)))
      ) {
        return "This row cannot be edited or deleted: its MongoDB _id type is not supported safely.";
      }
    }
  }
  return null;
}

export function rowPrimaryKeySignature(
  row: Row | undefined,
  primaryKeyColumns: readonly string[],
  mongoIdType?: "objectId" | "string" | null,
  mongoRowIdentity = mongoIdType !== undefined,
): string | null {
  if (
    !row ||
    rowMutationBlockReason(
      row,
      primaryKeyColumns,
      mongoIdType,
      mongoRowIdentity,
    )
  ) {
    return null;
  }

  const keyEntries: Array<[string, unknown]> = [];
  for (const columnName of primaryKeyColumns) {
    if (!(columnName in row)) {
      return null;
    }

    keyEntries.push([
      columnName,
      stablePrimaryKeyPart(storedRowKey(row, columnName)),
    ]);
  }

  if (primaryKeyColumns.includes("_id") && mongoIdType !== undefined) {
    keyEntries.push(["$rapidbMongoIdType", mongoIdType]);
  }

  return JSON.stringify(keyEntries);
}

export function buildPendingRestoreState(
  pendingEdits: PendingEdits,
  rows: readonly Row[],
  primaryKeyColumns: readonly string[],
  mongoIdTypes: readonly ("objectId" | "string" | null)[] = [],
  mongoRowIdentity = false,
  preservedEntries: readonly PendingRestoreEntry[] = [],
  rowValueTypes?: readonly Record<string, MongoValueTypeHint>[],
): PendingRestoreState {
  const entries: PendingRestoreEntry[] = [];

  for (const [rowIdx, columnMap] of pendingEdits.entries()) {
    const row = rows[rowIdx];
    const rowHasMongoIdentity =
      mongoRowIdentity || mongoIdTypes[rowIdx] !== undefined;
    const signature = rowPrimaryKeySignature(
      row,
      primaryKeyColumns,
      mongoIdTypes[rowIdx],
      rowHasMongoIdentity,
    );
    if (!signature) {
      entries.push({
        originalSignature: `unresolved-row:${rowIdx}`,
        changes: new Map(columnMap),
        baselineMissing: true,
      });
      continue;
    }

    entries.push({
      originalSignature: signature,
      changes: new Map(columnMap),
      originalValues: new Map(
        [...columnMap.keys()].map((name) => [
          name,
          Object.hasOwn(row, name) ? row[name] : undefined,
        ]),
      ),
      originalMissingFields: new Set(
        [...columnMap.keys()].filter((name) => !Object.hasOwn(row, name)),
      ),
      ...(rowValueTypes?.[rowIdx]
        ? {
            originalTypes: new Map(
              [...columnMap.keys()].map((name) => [
                name,
                rowValueTypeHint(rowValueTypes[rowIdx], name),
              ]),
            ),
          }
        : {}),
      ...(!rowHasMongoIdentity || rowValueTypes?.[rowIdx]
        ? { baselinePresenceVersion: 1 as const }
        : {}),
    });
  }

  return {
    entries: [...entries, ...preservedEntries].map((entry) => ({
      ...entry,
      changes: new Map(entry.changes),
      ...(entry.originalValues
        ? { originalValues: new Map(entry.originalValues) }
        : {}),
      ...(entry.originalMissingFields
        ? { originalMissingFields: new Set(entry.originalMissingFields) }
        : {}),
      ...(entry.originalTypes
        ? { originalTypes: new Map(entry.originalTypes) }
        : {}),
    })),
  };
}

function rowValueTypeHint(
  types: Record<string, MongoValueTypeHint> | undefined,
  name: string,
): MongoValueTypeHint {
  return (
    (types && Object.hasOwn(types, name) ? types[name] : undefined) ?? {
      category: "other",
      nativeType: "missing",
    }
  );
}

// In conflict review, don't collapse different wire values (in particular,
// BSON document field order, missing properties, or distinct number types).
function exactObservedValueEqual(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true;
  if (!left || !right || typeof left !== "object" || typeof right !== "object")
    return false;
  if (left instanceof Date || right instanceof Date)
    return (
      left instanceof Date &&
      right instanceof Date &&
      Object.is(left.getTime(), right.getTime())
    );
  if (Array.isArray(left) !== Array.isArray(right)) return false;
  if (
    Array.isArray(left) &&
    Array.isArray(right) &&
    left.length !== right.length
  )
    return false;
  const leftEntries = Object.entries(left);
  const rightEntries = Object.entries(right);
  return (
    leftEntries.length === rightEntries.length &&
    leftEntries.every(
      ([key, value], index) =>
        key === rightEntries[index][0] &&
        exactObservedValueEqual(value, rightEntries[index][1]),
    )
  );
}

export function restorePendingEditsSafely(
  restoreState: PendingRestoreState | null,
  rows: readonly Row[],
  primaryKeyColumns: readonly string[],
  mongoIdTypes: readonly ("objectId" | "string" | null)[] = [],
  mongoRowIdentity = false,
  rowValueTypes?: readonly Record<string, MongoValueTypeHint>[],
  conflictReview = false,
): PendingRestoreResult {
  if (!restoreState || restoreState.entries.length === 0) {
    return { pendingEdits: new Map(), unresolved: [] };
  }

  const rowsBySignature = new Map<string, number[]>();

  rows.forEach((row, rowIdx) => {
    const signature = rowPrimaryKeySignature(
      row,
      primaryKeyColumns,
      mongoIdTypes[rowIdx],
      mongoRowIdentity || mongoIdTypes[rowIdx] !== undefined,
    );
    if (!signature) {
      return;
    }

    const indexes = rowsBySignature.get(signature) ?? [];
    indexes.push(rowIdx);
    rowsBySignature.set(signature, indexes);
  });

  const restored: PendingEdits = new Map();
  const unresolved: PendingRestoreEntry[] = [];
  const claimedRows = new Set<number>();
  const preserveUnresolved = (entry: PendingRestoreEntry) =>
    unresolved.push(conflictReview ? { ...entry, applyConflict: true } : entry);

  for (const entry of restoreState.entries) {
    const originalMatches = rowsBySignature.get(entry.originalSignature) ?? [];
    if (
      !entry.applyConflict &&
      originalMatches.length === 1 &&
      !claimedRows.has(originalMatches[0])
    ) {
      const rowIdx = originalMatches[0];
      if (
        entry.baselineMissing ||
        (conflictReview && !entry.originalValues) ||
        (entry.originalValues &&
          [...entry.changes.keys()].some(
            (name) =>
              !entry.originalValues?.has(name) ||
              (entry.baselinePresenceVersion !== 1 &&
                entry.originalValues.get(name) === null) ||
              (entry.originalValues.get(name) === undefined &&
                (entry.baselinePresenceVersion !== 1 ||
                  ((mongoRowIdentity || mongoIdTypes[rowIdx] !== undefined) &&
                    !rowValueTypes?.[rowIdx]))) ||
              (entry.originalMissingFields?.has(name)
                ? Object.hasOwn(rows[rowIdx], name)
                : !Object.hasOwn(rows[rowIdx], name) ||
                  (conflictReview
                    ? !exactObservedValueEqual(
                        entry.originalValues.get(name),
                        rows[rowIdx][name],
                      )
                    : JSON.stringify(
                        stablePrimaryKeyPart(entry.originalValues.get(name)),
                      ) !==
                      JSON.stringify(
                        stablePrimaryKeyPart(rows[rowIdx][name]),
                      ))) ||
              ((entry.originalTypes || rowValueTypes?.[rowIdx]) &&
                (!entry.originalTypes?.has(name) ||
                  !rowValueTypes?.[rowIdx] ||
                  JSON.stringify(
                    stablePrimaryKeyPart(entry.originalTypes.get(name)),
                  ) !==
                    JSON.stringify(
                      stablePrimaryKeyPart(
                        rowValueTypeHint(rowValueTypes[rowIdx], name),
                      ),
                    ))),
          ))
      ) {
        preserveUnresolved(entry);
        continue;
      }
      claimedRows.add(rowIdx);
      restored.set(rowIdx, new Map(entry.changes));
    } else {
      preserveUnresolved(entry);
    }
  }

  return { pendingEdits: restored, unresolved };
}

export function getRetainedPendingEdits(
  pendingEdits: PendingEdits,
  updateRowIndexes: readonly number[],
  rowOutcomes?: ApplyResultPayload["rowOutcomes"],
  failedRows?: readonly number[],
): PendingEdits {
  const retainedUpdateIndexes = new Set<number>();

  if (rowOutcomes && rowOutcomes.length > 0) {
    for (const outcome of rowOutcomes) {
      if (outcome.status !== "applied" && outcome.status !== "skipped") {
        retainedUpdateIndexes.add(outcome.rowIndex);
      }
    }
  } else if (failedRows && failedRows.length > 0) {
    for (const rowIndex of failedRows) {
      retainedUpdateIndexes.add(rowIndex);
    }
  }

  if (retainedUpdateIndexes.size === 0) {
    return new Map();
  }

  const nextPending: PendingEdits = new Map();
  for (const updateIndex of retainedUpdateIndexes) {
    const rowIdx = updateRowIndexes[updateIndex];
    if (rowIdx === undefined) {
      continue;
    }

    const rowPending = pendingEdits.get(rowIdx);
    if (rowPending) {
      nextPending.set(rowIdx, new Map(rowPending));
    }
  }

  return nextPending;
}

export function buildUndoRedoSnapshot(
  pendingEdits: PendingEdits,
  newRows: InsertDraftRow[],
  editCell: EditTarget | null,
  unresolvedPendingEdits: readonly PendingRestoreEntry[] = [],
): MutationSnapshot & { unresolvedPendingEdits: PendingRestoreEntry[] } {
  return {
    pendingEdits: clonePendingEdits(pendingEdits),
    unresolvedPendingEdits: unresolvedPendingEdits.map((entry) => ({
      ...entry,
      changes: new Map(entry.changes),
      ...(entry.originalValues
        ? { originalValues: new Map(entry.originalValues) }
        : {}),
      ...(entry.originalMissingFields
        ? { originalMissingFields: new Set(entry.originalMissingFields) }
        : {}),
      ...(entry.originalTypes
        ? { originalTypes: new Map(entry.originalTypes) }
        : {}),
    })),
    newRows: newRows.map((row) => ({ ...row })),
    editCell,
  };
}

export function applyUndoRedoSnapshot(snapshot: MutationSnapshot): {
  pendingEdits: PendingEdits;
  unresolvedPendingEdits: PendingRestoreEntry[];
  newRows: InsertDraftRow[];
  editCell: EditTarget | null;
} {
  return buildUndoRedoSnapshot(
    snapshot.pendingEdits,
    snapshot.newRows,
    snapshot.editCell,
    snapshot.unresolvedPendingEdits,
  );
}
