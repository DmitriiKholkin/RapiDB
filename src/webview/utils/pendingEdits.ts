/**
 * Pure utilities for managing PendingEdits state.
 *
 * Design contract:
 *   - All exported functions are pure: they never mutate the input and
 *     always return a new `PendingEdits` instance for state updates.
 *   - The hot path (single cell update) is implemented in
 *     {@link applySingleCellEdit} and re-used by the higher-level helpers.
 *   - The classification of an edit (revert vs. effective vs. no-op) is
 *     centralized in {@link classifyEdit} so the two predicates
 *     (mutation predicate and effective-edit predicate) cannot drift.
 */
import {
  type ColumnTypeMeta as ColumnMeta,
  NULL_SENTINEL,
} from "../../shared/tableTypes";
import { valueToEditString } from "../components/table/EditInput";
import type { InsertDraftRow, PendingEdits } from "../types";

// ─── Public Types ────────────────────────────────────────────────────────────

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

// ─── Edit Classification ────────────────────────────────────────────────────

/**
 * Coerce the NULL_SENTINEL pseudo-value to an actual JS `null`.
 * Centralised because both the mutation and the predicate path need it.
 */
function coerceNewValue(newVal: string): unknown {
  return newVal === NULL_SENTINEL ? null : newVal;
}

/** Discriminated outcome of comparing a candidate edit against the current state. */
type EditClassification =
  | { kind: "revert" } // cell matches original → drop pending if any
  | { kind: "write"; coerced: unknown }; // cell differs → write the coerced value

/**
 * Decide what should happen to the pending state for a given candidate edit.
 * Single source of truth shared by `applySingleCellEdit` and
 * `isEditEffective` so the two cannot drift.
 */
function classifyEdit(edit: PendingCellEdit): EditClassification {
  const coerced = coerceNewValue(edit.newVal);
  const reverted = edit.newVal === valueToEditString(edit.originalVal);
  return reverted ? { kind: "revert" } : { kind: "write", coerced };
}

// ─── Single-Cell Mutation (the hot path) ─────────────────────────────────────

/**
 * Apply a single cell edit to a `PendingEdits` map. Returns a new map.
 * Behaviour:
 *   - "revert" branch: drops the entry entirely (no-op if not pending).
 *   - "write" branch: inserts/updates the entry, short-circuiting if the
 *     pending state already matches the requested coerced value.
 */
function applySingleCellEdit(
  previousPending: PendingEdits,
  edit: PendingCellEdit,
): PendingEdits {
  const { rowIdx, column } = edit;
  const classification = classifyEdit(edit);
  const existingRow = previousPending.get(rowIdx);

  if (classification.kind === "revert") {
    // Only allocate new maps if the entry actually exists.
    if (!existingRow?.has(column.name)) {
      return previousPending;
    }
    return removeCellFromRow(previousPending, rowIdx, column.name, existingRow);
  }

  // "write" — no-op if the pending state already matches.
  if (existingRow?.get(column.name) === classification.coerced) {
    return previousPending;
  }

  const nextRow = new Map(existingRow ?? []);
  nextRow.set(column.name, classification.coerced);
  const nextPending = new Map(previousPending);
  nextPending.set(rowIdx, nextRow);
  return nextPending;
}

/**
 * Remove a single cell from a row, deleting the row entirely when it
 * becomes empty. Always returns a new `PendingEdits` map.
 */
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

// ─── Public API: PendingEdits Map Operations ────────────────────────────────

/**
 * Create a new PendingEdits map with a single cell edit applied.
 * Returns a new map (immutable update).
 */
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

/**
 * Apply a batch of cell edits to a PendingEdits map.
 * Returns a new map (immutable update).
 */
export function applyCellEditsToPending(
  previousPending: PendingEdits,
  edits: PendingCellEdit[],
): PendingEdits {
  if (edits.length === 0) {
    return previousPending;
  }
  return edits.reduce<PendingEdits>(applySingleCellEdit, previousPending);
}

/**
 * Get pending edits for a specific cell.
 * Returns the coerced value if pending, or `undefined` if not.
 */
export function getPendingEditsForCell(
  pendingEdits: PendingEdits,
  rowIdx: number,
  columnName: string,
): unknown | undefined {
  return pendingEdits.get(rowIdx)?.get(columnName);
}

/**
 * Clear all pending edits for a specific cell.
 * Returns the same map (no-op) if the cell isn't pending, otherwise a new map.
 */
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

/**
 * Clear all pending edits.
 * Returns a new empty map.
 */
export function clearAllPendingEdits(): PendingEdits {
  return new Map();
}

/**
 * Merge two PendingEdits maps, with `override` taking precedence.
 * Returns a new map (immutable update).
 */
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
      // Defensive copy so callers can mutate the result without leaking
      // references into `override`.
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

// ─── Public API: Draft Row Operations ────────────────────────────────────────

/**
 * Apply a batch of draft cell edits to an InsertDraftRow.
 * Returns a new row (immutable update).
 */
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
      // Preserve any unrelated fields if the cell was already populated.
      ...(existingCell ?? {}),
      value: norm,
    };
  }
  return next;
}

// ─── Public API: Effective Edit Filtering ────────────────────────────────────

/**
 * Determine which edits are effective (different from current pending state).
 * An edit is effective if it would change the pending state.
 */
export function filterEffectiveEdits(
  edits: PendingCellEdit[],
  currentPending: PendingEdits,
): PendingCellEdit[] {
  return edits.filter((edit) => isEditEffective(edit, currentPending));
}

/**
 * Single-edit predicate. Exposed for callers that want to short-circuit
 * before allocating a full filtered array.
 */
function isEditEffective(
  edit: PendingCellEdit,
  currentPending: PendingEdits,
): boolean {
  const classification = classifyEdit(edit);
  const currentRow = currentPending.get(edit.rowIdx);

  if (classification.kind === "revert") {
    // Edit matches the original — effective only if the cell is currently
    // pending (we'd be removing a stale edit).
    return currentRow?.has(edit.column.name) ?? false;
  }

  // Edit differs from the original — effective unless already pending
  // with the exact same coerced value.
  return currentRow?.get(edit.column.name) !== classification.coerced;
}
