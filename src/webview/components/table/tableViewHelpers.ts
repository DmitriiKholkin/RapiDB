import type React from "react";
import {
  type ColumnTypeMeta as ColumnMeta,
  deriveApplicableFilterDrafts,
  type FilterDraftMap,
  formatColumnDetailDescription,
  formatPrimaryKeyRoleLabel,
  isServerGeneratedColumn,
  NULL_SENTINEL,
} from "../../../shared/tableTypes";
import type { ApplyResultPayload } from "../../../shared/webviewContracts";
import type {
  EditTarget,
  InsertDraftRow,
  MutationSnapshot,
  PendingEdits,
  PendingRestoreEntry,
  Row,
} from "../../types";
import { buildButtonStyle } from "../../utils/buttonStyles";
import { TOOLBAR_H } from "../../utils/layout";

export type { PendingRestoreEntry } from "../../types";
export { TOOLBAR_H };

export const PAGE_SIZES = [25, 100, 500, 1000] as const;
export const DEBOUNCE = 1000;
export const ROW_H = 26;
export const HEADER_H = 28;
export const FILTER_H = 30;
export const PREVIEW_DIALOG_EDITOR_H = "min(42vh, 360px)";
export const INSERT_DEFAULT_SENTINEL = "__RAPIDB_INSERT_DEFAULT__";
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
}

export interface PendingRestoreState {
  entries: PendingRestoreEntry[];
}

export interface PendingRestoreResult {
  pendingEdits: PendingEdits;
  unresolved: PendingRestoreEntry[];
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
  return !!column && !isServerGeneratedColumn(column);
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

export function buildInsertValues(
  draft: InsertDraftRow,
): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(draft)
      .filter(([, cell]) => cell.value !== INSERT_DEFAULT_SENTINEL)
      .map(([columnName, cell]) => [
        columnName,
        cell.value === NULL_SENTINEL ? null : cell.value,
      ]),
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
    if (
      !Object.hasOwn(row, name) ||
      row[name] === null ||
      row[name] === undefined
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

    keyEntries.push([columnName, stablePrimaryKeyPart(row[columnName])]);
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
      continue;
    }

    entries.push({
      originalSignature: signature,
      changes: new Map(columnMap),
    });
  }

  return {
    entries: [...entries, ...preservedEntries].map((entry) => ({
      ...entry,
      changes: new Map(entry.changes),
    })),
  };
}

export function restorePendingEditsSafely(
  restoreState: PendingRestoreState | null,
  rows: readonly Row[],
  primaryKeyColumns: readonly string[],
  mongoIdTypes: readonly ("objectId" | "string" | null)[] = [],
  mongoRowIdentity = false,
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

  // A proposed PK is not identity evidence, even after a complete snapshot:
  // another client may have created that key since the original read.
  for (const entry of restoreState.entries) {
    const originalMatches = rowsBySignature.get(entry.originalSignature) ?? [];
    if (originalMatches.length === 1 && !claimedRows.has(originalMatches[0])) {
      const rowIdx = originalMatches[0];
      claimedRows.add(rowIdx);
      restored.set(rowIdx, new Map(entry.changes));
    } else {
      unresolved.push(entry);
    }
  }

  return { pendingEdits: restored, unresolved };
}

export function restorePendingEdits(
  restoreState: PendingRestoreState | null,
  rows: readonly Row[],
  primaryKeyColumns: readonly string[],
  mongoIdTypes: readonly ("objectId" | "string" | null)[] = [],
  mongoRowIdentity = false,
): PendingEdits {
  return restorePendingEditsSafely(
    restoreState,
    rows,
    primaryKeyColumns,
    mongoIdTypes,
    mongoRowIdentity,
  ).pendingEdits;
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
