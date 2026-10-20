import {
  type ColumnTypeMeta as ColumnMeta,
  NULL_SENTINEL,
} from "../../shared/tableTypes";
import { valueToEditString } from "../components/table/EditInput";
import type { InsertDraftRow, PendingEdits } from "../types";

export interface PendingCellEdit {
  rowIdx: number;
  column: ColumnMeta;
  newVal: string;
  originalVal: unknown;
}

export interface PendingDraftEdit {
  column: ColumnMeta;
  newVal: string;
}

function coerceNewValue(newVal: string): unknown {
  return newVal === NULL_SENTINEL ? null : newVal;
}

type EditClassification =
  | { kind: "revert" }
  | { kind: "write"; coerced: unknown };

function classifyEdit(edit: PendingCellEdit): EditClassification {
  const coerced = coerceNewValue(edit.newVal);
  const reverted = edit.newVal === valueToEditString(edit.originalVal);
  return reverted ? { kind: "revert" } : { kind: "write", coerced };
}

function applySingleCellEdit(
  previousPending: PendingEdits,
  edit: PendingCellEdit,
): PendingEdits {
  const { rowIdx, column } = edit;
  const classification = classifyEdit(edit);
  const existingRow = previousPending.get(rowIdx);

  if (classification.kind === "revert") {
    if (!existingRow?.has(column.name)) {
      return previousPending;
    }
    return removeCellFromRow(previousPending, rowIdx, column.name, existingRow);
  }

  if (existingRow?.get(column.name) === classification.coerced) {
    return previousPending;
  }

  const nextRow = new Map(existingRow ?? []);
  nextRow.set(column.name, classification.coerced);
  const nextPending = new Map(previousPending);
  nextPending.set(rowIdx, nextRow);
  return nextPending;
}

function removeCellFromRow(
  previousPending: PendingEdits,
  rowIdx: number,
  columnName: string,
  existingRow: Map<string, unknown>,
): PendingEdits {
  const nextRow = new Map(existingRow);
  nextRow.delete(columnName);

  const nextPending = new Map(previousPending);
  if (nextRow.size === 0) {
    nextPending.delete(rowIdx);
  } else {
    nextPending.set(rowIdx, nextRow);
  }
  return nextPending;
}

export function createPendingEdit(
  previousPending: PendingEdits,
  rowIdx: number,
  column: ColumnMeta,
  newVal: string,
  originalVal: unknown,
): PendingEdits {
  return applySingleCellEdit(previousPending, {
    rowIdx,
    column,
    newVal,
    originalVal,
  });
}

export function applyCellEditsToPending(
  previousPending: PendingEdits,
  edits: PendingCellEdit[],
): PendingEdits {
  if (edits.length === 0) {
    return previousPending;
  }
  return edits.reduce<PendingEdits>(applySingleCellEdit, previousPending);
}

export function getPendingEditsForCell(
  pendingEdits: PendingEdits,
  rowIdx: number,
  columnName: string,
): unknown | undefined {
  return pendingEdits.get(rowIdx)?.get(columnName);
}

export function clearPendingEdits(
  previousPending: PendingEdits,
  rowIdx: number,
  columnName: string,
): PendingEdits {
  const rowMap = previousPending.get(rowIdx);
  if (!rowMap?.has(columnName)) {
    return previousPending;
  }
  return removeCellFromRow(previousPending, rowIdx, columnName, rowMap);
}

export function clearAllPendingEdits(): PendingEdits {
  return new Map();
}

export function mergePendingEdits(
  base: PendingEdits,
  override: PendingEdits,
): PendingEdits {
  if (override.size === 0) {
    return base;
  }

  const nextPending = new Map(base);
  for (const [rowIdx, columnMap] of override.entries()) {
    const existingRowMap = nextPending.get(rowIdx);
    if (!existingRowMap) {
      nextPending.set(rowIdx, new Map(columnMap));
      continue;
    }

    const mergedRowMap = new Map(existingRowMap);
    for (const [columnName, value] of columnMap.entries()) {
      mergedRowMap.set(columnName, value);
    }
    nextPending.set(rowIdx, mergedRowMap);
  }

  return nextPending;
}

export function applyDraftEditsToInsertRow(
  draft: InsertDraftRow,
  edits: PendingDraftEdit[],
): InsertDraftRow {
  if (edits.length === 0) {
    return draft;
  }

  const next = { ...draft };
  for (const { column, newVal } of edits) {
    const norm = newVal === NULL_SENTINEL ? NULL_SENTINEL : newVal;
    const existingCell = next[column.name];
    next[column.name] = {
      ...(existingCell ?? {}),
      value: norm,
    };
  }
  return next;
}

export function filterEffectiveEdits(
  edits: PendingCellEdit[],
  currentPending: PendingEdits,
): PendingCellEdit[] {
  return edits.filter((edit) => isEditEffective(edit, currentPending));
}

function isEditEffective(
  edit: PendingCellEdit,
  currentPending: PendingEdits,
): boolean {
  const classification = classifyEdit(edit);
  const currentRow = currentPending.get(edit.rowIdx);

  if (classification.kind === "revert") {
    return currentRow?.has(edit.column.name) ?? false;
  }

  return currentRow?.get(edit.column.name) !== classification.coerced;
}
