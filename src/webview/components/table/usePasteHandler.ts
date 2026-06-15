import type { MutableRefObject } from "react";
import { useEffect, useState } from "react";
import type { ColumnTypeMeta as ColumnMeta } from "../../../shared/tableTypes";
import type { InsertDraftRow, Row } from "../../types";
import { isEditableElement } from "../../utils/editableElement";
import { onMessage } from "../../utils/messaging";
import {
  formatNormalizedPasteValue,
  type PasteValidationError,
  parseTsv,
  validatePasteData,
  validatePasteValue,
} from "../../utils/pasteUtils";

// ─── Public Types ──────────────────────────────────────────────────────────

export interface UsePasteHandlerOptions {
  handlePaste: () => void;
  canEditRows: boolean;
  columns: readonly ColumnMeta[];
  rows: Row[];
  selectedColumnOffset: number;
  onBatchCellEdit: (
    edits: Array<{
      rowIdx: number;
      column: ColumnMeta;
      newVal: string;
      originalVal: unknown;
    }>,
  ) => void;
  onBatchDraftCellEdit: (
    edits: Array<{ column: ColumnMeta; newVal: string }>,
  ) => void;
  onMixedBatchEdit: (
    draftEdits: Array<{ column: ColumnMeta; newVal: string }>,
    persistedEdits: Array<{
      rowIdx: number;
      column: ColumnMeta;
      newVal: string;
      originalVal: unknown;
    }>,
  ) => void;
  selectionRangeRef: MutableRefObject<{
    anchorRow: number;
    anchorCol: number;
  } | null>;
  contextMenuCellRef: MutableRefObject<{
    row: number;
    col: number;
  } | null>;
  newRowRef: MutableRefObject<InsertDraftRow | null>;
}

export interface UsePasteHandlerReturn {
  pasteErrors: PasteValidationError[];
  setPasteErrors: React.Dispatch<React.SetStateAction<PasteValidationError[]>>;
}

// ─── Hook ──────────────────────────────────────────────────────────────────

export function usePasteHandler({
  handlePaste,
  canEditRows,
  columns,
  rows,
  selectedColumnOffset,
  onBatchCellEdit,
  onBatchDraftCellEdit,
  onMixedBatchEdit,
  selectionRangeRef,
  contextMenuCellRef,
  newRowRef,
}: UsePasteHandlerOptions): UsePasteHandlerReturn {
  const [pasteErrors, setPasteErrors] = useState<PasteValidationError[]>([]);

  // ── Keyboard / browser paste event ──────────────────────────────────────
  useEffect(() => {
    const handlePasteEvent = (event: ClipboardEvent) => {
      const target = event.target as HTMLElement | null;
      if (
        isEditableElement(target) ||
        isEditableElement(document.activeElement)
      ) {
        return;
      }
      if (!canEditRows || !selectionRangeRef.current) return;
      event.preventDefault();
      event.stopPropagation();
      handlePaste();
    };
    window.addEventListener("paste", handlePasteEvent, true);
    return () => window.removeEventListener("paste", handlePasteEvent, true);
  }, [canEditRows, handlePaste, selectionRangeRef]);

  // ── Receive clipboard text and apply paste ──────────────────────────────
  useEffect(() => {
    const unsubscribe = onMessage<string>("clipboardText", (text) => {
      if (!canEditRows || !selectionRangeRef.current) return;

      const pasteData = parseTsv(text);
      if (pasteData.rows.length === 0) return;

      const ctxCell = contextMenuCellRef.current;
      const startRow = ctxCell
        ? ctxCell.row
        : selectionRangeRef.current.anchorRow;
      const startCol = ctxCell
        ? ctxCell.col - selectedColumnOffset
        : selectionRangeRef.current.anchorCol - selectedColumnOffset;

      contextMenuCellRef.current = null;

      if (startCol < 0) {
        setPasteErrors([
          {
            rowIndex: startRow,
            columnIndex: ctxCell
              ? ctxCell.col
              : selectionRangeRef.current.anchorCol,
            columnName: "",
            value: "",
            message: "Cannot paste into selection column",
          },
        ]);
        return;
      }

      // ── Draft row paste (startRow === -1) ──────────────────────────────
      if (startRow === -1) {
        handleDraftRowPaste(
          pasteData,
          startRow,
          startCol,
          columns,
          rows,
          selectedColumnOffset,
          newRowRef,
          setPasteErrors,
          onBatchCellEdit,
          onBatchDraftCellEdit,
          onMixedBatchEdit,
        );
        return;
      }

      // ── Persisted row paste ────────────────────────────────────────────
      handlePersistedRowPaste(
        pasteData,
        startRow,
        startCol,
        columns,
        rows,
        setPasteErrors,
        onBatchCellEdit,
      );
    });

    return unsubscribe;
  }, [
    canEditRows,
    columns,
    rows,
    selectedColumnOffset,
    onBatchCellEdit,
    onBatchDraftCellEdit,
    onMixedBatchEdit,
    selectionRangeRef,
    contextMenuCellRef,
    newRowRef,
  ]);

  return { pasteErrors, setPasteErrors };
}

// ─── Internal Helpers ─────────────────────────────────────────────────────

interface NormalizedCell {
  targetRow: number;
  column: ColumnMeta;
  value: string;
  normalized: unknown;
}

function handleDraftRowPaste(
  pasteData: ReturnType<typeof parseTsv>,
  startRow: number,
  startCol: number,
  columns: readonly ColumnMeta[],
  rows: Row[],
  selectedColumnOffset: number,
  newRowRef: MutableRefObject<InsertDraftRow | null>,
  setPasteErrors: React.Dispatch<React.SetStateAction<PasteValidationError[]>>,
  onBatchCellEdit: (
    edits: Array<{
      rowIdx: number;
      column: ColumnMeta;
      newVal: string;
      originalVal: unknown;
    }>,
  ) => void,
  onBatchDraftCellEdit: (
    edits: Array<{ column: ColumnMeta; newVal: string }>,
  ) => void,
  onMixedBatchEdit: (
    draftEdits: Array<{ column: ColumnMeta; newVal: string }>,
    persistedEdits: Array<{
      rowIdx: number;
      column: ColumnMeta;
      newVal: string;
      originalVal: unknown;
    }>,
  ) => void,
): void {
  const draft = newRowRef.current;
  if (!draft) return;

  const errors: PasteValidationError[] = [];
  const normalizedCells: NormalizedCell[] = [];

  for (let r = 0; r < pasteData.rows.length; r++) {
    const row = pasteData.rows[r];
    const targetRow = startRow + r;

    for (let c = 0; c < row.length; c++) {
      const value = row[c];
      const targetCol = startCol + c;
      const column = columns[targetCol];
      if (!column) continue;

      const validation = validatePasteValue(value, column);
      if (!validation.valid) {
        errors.push({
          rowIndex: targetRow,
          columnIndex: targetCol + selectedColumnOffset,
          columnName: column.name,
          value,
          message: validation.error ?? "Validation failed",
        });
        continue;
      }

      if (targetRow >= rows.length) {
        errors.push({
          rowIndex: targetRow,
          columnIndex: targetCol + selectedColumnOffset,
          columnName: column.name,
          value,
          message: `Row ${targetRow + 1} does not exist`,
        });
        continue;
      }

      normalizedCells.push({
        targetRow,
        column,
        value,
        normalized: validation.coercedValue,
      });
    }
  }

  if (errors.length > 0) {
    setPasteErrors(errors);
    return;
  }

  setPasteErrors([]);

  const batchEdits: Array<{
    rowIdx: number;
    column: ColumnMeta;
    newVal: string;
    originalVal: unknown;
  }> = [];

  const draftEdits: Array<{
    column: ColumnMeta;
    newVal: string;
  }> = [];

  for (const cell of normalizedCells) {
    const coercedValue = formatNormalizedPasteValue(
      cell.value,
      cell.normalized,
    );

    if (cell.targetRow === -1) {
      draftEdits.push({ column: cell.column, newVal: coercedValue });
    } else {
      const originalValue = rows[cell.targetRow]?.[cell.column.name];
      batchEdits.push({
        rowIdx: cell.targetRow,
        column: cell.column,
        newVal: coercedValue,
        originalVal: originalValue,
      });
    }
  }

  if (draftEdits.length > 0 && batchEdits.length > 0) {
    onMixedBatchEdit(draftEdits, batchEdits);
  } else if (draftEdits.length > 0) {
    onBatchDraftCellEdit(draftEdits);
  } else if (batchEdits.length > 0) {
    onBatchCellEdit(batchEdits);
  }
}

function handlePersistedRowPaste(
  pasteData: ReturnType<typeof parseTsv>,
  startRow: number,
  startCol: number,
  columns: readonly ColumnMeta[],
  rows: Row[],
  setPasteErrors: React.Dispatch<React.SetStateAction<PasteValidationError[]>>,
  onBatchCellEdit: (
    edits: Array<{
      rowIdx: number;
      column: ColumnMeta;
      newVal: string;
      originalVal: unknown;
    }>,
  ) => void,
): void {
  const validationResult = validatePasteData(
    pasteData,
    startRow,
    startCol,
    [...columns],
    rows.length,
  );

  if (validationResult.errors.length > 0) {
    setPasteErrors(validationResult.errors);
    return;
  }

  setPasteErrors([]);

  const edits: Array<{
    rowIdx: number;
    column: ColumnMeta;
    newVal: string;
    originalVal: unknown;
  }> = [];

  for (let r = 0; r < validationResult.rows.length; r++) {
    const normalizedRow = validationResult.rows[r];
    const targetRow = startRow + r;

    for (const cell of normalizedRow) {
      const originalValue = rows[targetRow]?.[cell.column.name];
      const coercedValue = formatNormalizedPasteValue(
        cell.value,
        cell.normalized,
      );

      edits.push({
        rowIdx: targetRow,
        column: cell.column,
        newVal: coercedValue,
        originalVal: originalValue,
      });
    }
  }

  onBatchCellEdit(edits);
}
