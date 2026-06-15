/**
 * Manages the structured cell dialog (JSON/XML/array editor).
 *
 * Encapsulates open/close/update/confirm/setNull logic and delegates
 * the actual cell commit to the parent's commit callbacks.
 */
import { useCallback, useState } from "react";
import type { ColumnTypeMeta as ColumnMeta } from "../../../shared/tableTypes";
import { NULL_SENTINEL } from "../../../shared/tableTypes";
import type {
  StructuredCellDialogState,
  StructuredCellDialogValue,
} from "./structuredCellDialog";
import { serializeStructuredCellDialogDraft } from "./structuredCellDialog";

interface UseStructuredCellDialogOptions {
  commitCellEdit: (
    rowIdx: number,
    column: ColumnMeta,
    newVal: string,
    originalVal: unknown,
  ) => void;
  commitDraftCellEdit: (column: ColumnMeta, newVal: string) => void;
  /** Clear the active edit target (e.g. close inline editor). */
  clearEditCell: () => void;
  /** Clear apply status banner. */
  clearApplyStatus: () => void;
}

export function useStructuredCellDialog({
  commitCellEdit,
  commitDraftCellEdit,
  clearEditCell,
  clearApplyStatus,
}: UseStructuredCellDialogOptions) {
  const [dialog, setDialog] = useState<StructuredCellDialogState | null>(null);

  const open = useCallback(
    (options: {
      rowKind: "persisted" | "draft";
      rowIdx?: number;
      column: ColumnMeta;
      value: StructuredCellDialogValue;
      currentValue: unknown;
      originalValue: unknown;
      readOnly: boolean;
    }) => {
      const {
        rowKind,
        rowIdx,
        column,
        value,
        currentValue,
        originalValue,
        readOnly,
      } = options;

      clearEditCell();
      clearApplyStatus();
      setDialog({
        rowKind,
        rowIdx: rowKind === "persisted" ? (rowIdx ?? null) : null,
        column,
        title: `Cell data: ${column.name}`,
        description: readOnly
          ? `Structured ${value.kind} data in ${column.name}. Apply closes this dialog without sending a mutation.`
          : rowKind === "draft"
            ? `Structured ${value.kind} data in ${column.name}. Apply updates the pending inserted row only.`
            : `Structured ${value.kind} data in ${column.name}. Apply updates the local pending cell edit only.`,
        language: value.language,
        initialText: value.formattedText,
        draftText: value.formattedText,
        originalValue,
        nullable: column.nullable,
        readOnly,
        initialIsNull: currentValue === null,
        isNull: currentValue === null,
      });
    },
    [clearEditCell, clearApplyStatus],
  );

  const updateDraft = useCallback((nextValue: string) => {
    setDialog((current) => {
      if (!current) return current;
      return { ...current, draftText: nextValue, isNull: false };
    });
  }, []);

  const cancel = useCallback(() => setDialog(null), []);

  const commitValue = useCallback(
    (dlg: StructuredCellDialogState, nextValue: string) => {
      if (dlg.rowKind === "persisted" && dlg.rowIdx !== null) {
        commitCellEdit(dlg.rowIdx, dlg.column, nextValue, dlg.originalValue);
        return;
      }
      commitDraftCellEdit(dlg.column, nextValue);
    },
    [commitCellEdit, commitDraftCellEdit],
  );

  const confirm = useCallback(() => {
    if (!dialog) return;
    setDialog(null);

    if (
      dialog.readOnly ||
      (dialog.draftText === dialog.initialText &&
        dialog.isNull === dialog.initialIsNull)
    ) {
      return;
    }

    const nextValue = dialog.isNull
      ? NULL_SENTINEL
      : serializeStructuredCellDialogDraft(dialog.draftText, dialog.column);

    commitValue(dialog, nextValue);
  }, [commitValue, dialog]);

  const setNull = useCallback(() => {
    if (!dialog || dialog.readOnly || !dialog.nullable) return;
    setDialog(null);
    commitValue(dialog, NULL_SENTINEL);
  }, [commitValue, dialog]);

  return {
    dialog,
    open,
    updateDraft,
    cancel,
    confirm,
    setNull,
  };
}
