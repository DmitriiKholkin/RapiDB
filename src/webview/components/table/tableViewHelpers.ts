import type React from "react";
import {
  type ColumnTypeMeta as ColumnMeta,
  deriveApplicableFilterDrafts,
  type FilterDraftMap,
  formatColumnDetailDescription,
  formatPrimaryKeyRoleLabel,
} from "../../../shared/tableTypes";
import type { ApplyResultPayload } from "../../../shared/webviewContracts";
import type { InsertDraftRow, PendingEdits, Row } from "../../types";
import { buildButtonStyle } from "../../utils/buttonStyles";

export const PAGE_SIZES = [25, 100, 500, 1000] as const;
export const DEBOUNCE = 400;
export const ROW_H = 26;
export const HEADER_H = 28;
export const FILTER_H = 30;
export const TOOLBAR_H = 36;
export const PREVIEW_DIALOG_EDITOR_H = "min(42vh, 360px)";
export const INSERT_DEFAULT_SENTINEL = "__RAPIDB_INSERT_DEFAULT__";
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

type PendingRestoreState = Map<string, Map<string, unknown>>;

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
  return !!column;
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

export function rowPrimaryKeySignature(
  row: Row | undefined,
  primaryKeyColumns: readonly string[],
): string | null {
  if (!row || primaryKeyColumns.length === 0) {
    return null;
  }

  const keyEntries: Array<[string, unknown]> = [];
  for (const columnName of primaryKeyColumns) {
    if (!(columnName in row)) {
      return null;
    }

    keyEntries.push([columnName, stablePrimaryKeyPart(row[columnName])]);
  }

  return JSON.stringify(keyEntries);
}

export function buildPendingRestoreState(
  pendingEdits: PendingEdits,
  rows: readonly Row[],
  primaryKeyColumns: readonly string[],
): PendingRestoreState {
  const restoreState: PendingRestoreState = new Map();

  for (const [rowIdx, columnMap] of pendingEdits.entries()) {
    const signature = rowPrimaryKeySignature(rows[rowIdx], primaryKeyColumns);
    if (!signature) {
      continue;
    }

    restoreState.set(signature, new Map(columnMap));
  }

  return restoreState;
}

export function restorePendingEdits(
  restoreState: PendingRestoreState | null,
  rows: readonly Row[],
  primaryKeyColumns: readonly string[],
): PendingEdits {
  if (!restoreState || restoreState.size === 0) {
    return new Map();
  }

  const restored: PendingEdits = new Map();

  rows.forEach((row, rowIdx) => {
    const signature = rowPrimaryKeySignature(row, primaryKeyColumns);
    if (!signature) {
      return;
    }

    const columnMap = restoreState.get(signature);
    if (columnMap) {
      restored.set(rowIdx, new Map(columnMap));
    }
  });

  return restored;
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
