import {
  type MutableRefObject,
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react";
import {
  type ColumnTypeMeta as ColumnMeta,
  NULL_SENTINEL,
} from "../../../shared/tableTypes";
import type {
  ApplyResultPayload,
  TableMutationPreviewPayload,
} from "../../../shared/webviewContracts";
import type {
  EditTarget,
  InsertDraftRow,
  PendingEdits,
  Row,
} from "../../types";
import { onMessage, postMessage } from "../../utils/messaging";
import { valueToEditString } from "./EditInput";
import type {
  StructuredCellDialogState,
  StructuredCellDialogValue,
} from "./structuredCellDialog";
import { serializeStructuredCellDialogDraft } from "./structuredCellDialog";
import {
  applyUndoRedoSnapshot,
  buildInsertValues,
  buildPendingRestoreState,
  buildUndoRedoSnapshot,
  canEditColumn,
  clonePendingEdits,
  createInsertDraft,
  getRetainedPendingEdits,
  restorePendingEdits,
  type TableApplyStatus,
} from "./tableViewHelpers";
import { useUndoRedoHistory } from "./useUndoRedoHistory";

interface UseTableMutationControllerParams {
  canEditRows: boolean;
  columnsRef: MutableRefObject<ColumnMeta[]>;
  fetchPageRef: MutableRefObject<() => void>;
  pkColsRef: MutableRefObject<string[]>;
  preserveScrollPositionRef: MutableRefObject<() => void>;
  rowsRef: MutableRefObject<Row[]>;
  selected: ReadonlySet<number>;
}

export function useTableMutationController({
  canEditRows,
  columnsRef,
  fetchPageRef,
  pkColsRef,
  preserveScrollPositionRef,
  rowsRef,
  selected,
}: UseTableMutationControllerParams) {
  const [pendingEdits, setPending] = useState<PendingEdits>(new Map());
  const [editCell, setEditCell] = useState<EditTarget | null>(null);
  const [applying, setApplying] = useState(false);
  const [applyStatus, setApplyStatus] = useState<TableApplyStatus | null>(null);
  const [newRow, setNewRow] = useState<InsertDraftRow | null>(null);
  const [inserting, setInserting] = useState(false);
  const [mutErr, setMutErr] = useState<string | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [mutationPreview, setMutationPreview] =
    useState<TableMutationPreviewPayload | null>(null);
  const [structuredCellDialog, setStructuredCellDialog] =
    useState<StructuredCellDialogState | null>(null);

  const applyPendingSnapshotRef = useRef<PendingEdits>(new Map());
  const applyRowIndexesRef = useRef<number[]>([]);
  const pendingRestoreRef = useRef<Map<string, Map<string, unknown>> | null>(
    null,
  );
  const selectedRef = useRef(selected);
  const canEditRowsRef = useRef(canEditRows);
  const mutationPreviewRef = useRef(mutationPreview);

  // Refs for snapshot access inside callbacks that avoid re-creation
  const pendingEditsRef = useRef(pendingEdits);
  pendingEditsRef.current = pendingEdits;
  const newRowRef = useRef(newRow);
  newRowRef.current = newRow;
  const editCellRef = useRef(editCell);
  editCellRef.current = editCell;

  const history = useUndoRedoHistory();

  selectedRef.current = selected;
  canEditRowsRef.current = canEditRows;
  mutationPreviewRef.current = mutationPreview;

  const buildPendingUpdatesPayload = useCallback(
    (source: PendingEdits) => {
      return [...source.entries()].map(([rowIdx, columnMap]) => ({
        primaryKeys: Object.fromEntries(
          pkColsRef.current.map((columnName) => [
            columnName,
            rowsRef.current[rowIdx][columnName],
          ]),
        ),
        changes: Object.fromEntries(columnMap),
      }));
    },
    [pkColsRef, rowsRef],
  );

  const clearApplyRequestState = useCallback(() => {
    setApplying(false);
    applyPendingSnapshotRef.current = new Map();
    applyRowIndexesRef.current = [];
  }, []);

  const handleRowsCommitted = useCallback(
    (rows: readonly Row[], primaryKeyColumns: readonly string[]) => {
      const restoredPending = restorePendingEdits(
        pendingRestoreRef.current,
        rows,
        primaryKeyColumns,
      );

      pendingRestoreRef.current = null;
      setPending(restoredPending);
      setEditCell(null);
    },
    [],
  );

  const resetForTableInit = useCallback(() => {
    clearApplyRequestState();
    history.clear();
    pendingRestoreRef.current = null;
    setPending(new Map());
    setEditCell(null);
    setApplying(false);
    setDeleting(false);
    setInserting(false);
    setMutationPreview(null);
    setStructuredCellDialog(null);
    setNewRow(null);
    setMutErr(null);
    setApplyStatus(null);
  }, [clearApplyRequestState, history]);

  useEffect(() => {
    const unApply = onMessage<ApplyResultPayload>(
      "applyResult",
      ({ success, error, warning, failedRows, rowOutcomes, insertApplied }) => {
        setApplying(false);

        if (success) {
          history.clear();
          setNewRow(null);
          const nextPending = getRetainedPendingEdits(
            applyPendingSnapshotRef.current,
            applyRowIndexesRef.current,
            rowOutcomes,
            failedRows,
          );

          setPending(nextPending);
          setApplyStatus(
            warning
              ? {
                  tone: "warning",
                  message: warning,
                }
              : null,
          );

          const restoreState = buildPendingRestoreState(
            nextPending,
            rowsRef.current,
            pkColsRef.current,
          );
          pendingRestoreRef.current =
            restoreState.size > 0 ? restoreState : null;

          preserveScrollPositionRef.current();
          fetchPageRef.current();
        } else {
          if (insertApplied) {
            setNewRow(null);
            const restoreState = buildPendingRestoreState(
              pendingEdits,
              rowsRef.current,
              pkColsRef.current,
            );
            pendingRestoreRef.current =
              restoreState.size > 0 ? restoreState : null;
            preserveScrollPositionRef.current();
            fetchPageRef.current();
          } else {
            pendingRestoreRef.current = null;
          }

          setApplyStatus({
            tone: "error",
            message: insertApplied
              ? `${error ?? "Apply failed"}. Insert was applied, but update changes were not.`
              : (error ?? "Apply failed — all changes were rolled back"),
          });
        }

        applyPendingSnapshotRef.current = new Map();
        applyRowIndexesRef.current = [];
      },
    );

    const unInsert = onMessage<{ success: boolean; error?: string }>(
      "insertResult",
      ({ success, error }) => {
        setInserting(false);
        if (success) {
          setNewRow(null);
          setEditCell(null);
          setMutErr(null);

          if (applyRowIndexesRef.current.length > 0) {
            const updates = buildPendingUpdatesPayload(
              applyPendingSnapshotRef.current,
            );
            postMessage("applyChanges", { updates });
            return;
          }

          setApplying(false);
          fetchPageRef.current();
        } else {
          setApplying(false);
          setMutErr(error ?? "Insert failed");
        }
      },
    );

    const unDelete = onMessage<{ success: boolean; error?: string }>(
      "deleteResult",
      ({ success, error }) => {
        setDeleting(false);
        if (success) {
          setMutErr(null);
          fetchPageRef.current();
        } else {
          setMutErr(error ?? "Delete failed");
        }
      },
    );

    const unMutationPreview = onMessage<TableMutationPreviewPayload>(
      "tableMutationPreview",
      (payload) => {
        setMutationPreview(payload);
      },
    );

    return () => {
      unApply();
      unInsert();
      unDelete();
      unMutationPreview();
    };
  }, [
    buildPendingUpdatesPayload,
    fetchPageRef,
    history,
    pendingEdits,
    pkColsRef,
    preserveScrollPositionRef,
    rowsRef,
  ]);

  const cancelMutationPreview = useCallback(() => {
    const preview = mutationPreviewRef.current;
    if (!preview) {
      return;
    }

    const { kind, previewToken } = preview;
    setMutationPreview(null);

    if (kind === "applyChanges") {
      clearApplyRequestState();
    } else if (kind === "insertRow") {
      setInserting(false);
      clearApplyRequestState();
    } else {
      setDeleting(false);
    }

    postMessage("cancelMutationPreview", { previewToken });
  }, [clearApplyRequestState]);

  const confirmMutationPreview = useCallback(() => {
    const preview = mutationPreviewRef.current;
    if (!preview) {
      return;
    }

    setMutationPreview(null);
    postMessage("confirmMutationPreview", {
      previewToken: preview.previewToken,
    });
  }, []);

  useEffect(() => {
    if (!mutationPreview) {
      return;
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key !== "Escape") {
        return;
      }

      event.preventDefault();
      cancelMutationPreview();
    };

    window.addEventListener("keydown", handleKeyDown, true);
    return () => {
      window.removeEventListener("keydown", handleKeyDown, true);
    };
  }, [cancelMutationPreview, mutationPreview]);

  const startInsertRow = useCallback(() => {
    history.push(
      buildUndoRedoSnapshot(pendingEditsRef.current, newRowRef.current, null),
    );
    setNewRow(createInsertDraft(columnsRef.current));
    setEditCell(null);
    setMutErr(null);
  }, [columnsRef, history]);

  const applyChanges = useCallback(() => {
    const unsavedRowCount = pendingEdits.size + (newRow ? 1 : 0);
    if (unsavedRowCount === 0 || applying) {
      return;
    }

    applyPendingSnapshotRef.current = clonePendingEdits(pendingEdits);
    applyRowIndexesRef.current = [...pendingEdits.keys()];

    setApplying(true);
    setApplyStatus(null);
    setMutErr(null);

    const updates = buildPendingUpdatesPayload(pendingEdits);
    postMessage("applyChanges", {
      updates,
      ...(newRow ? { insertValues: buildInsertValues(newRow) } : {}),
    });
  }, [applying, buildPendingUpdatesPayload, newRow, pendingEdits]);

  const revertChanges = useCallback(() => {
    history.clear();
    pendingRestoreRef.current = null;
    setPending(new Map());
    setNewRow(null);
    setEditCell(null);
    setMutErr(null);
    setApplyStatus(null);
  }, [history]);

  const commitCellEdit = useCallback(
    (
      rowIdx: number,
      column: ColumnMeta,
      newVal: string,
      originalVal: unknown,
    ) => {
      setEditCell(null);

      if (!canEditRowsRef.current) {
        return;
      }

      const coerced: unknown = newVal === NULL_SENTINEL ? null : newVal;
      const originalValueString = valueToEditString(originalVal);

      // Push current state to history BEFORE making the change
      history.push(
        buildUndoRedoSnapshot(pendingEditsRef.current, newRowRef.current, null),
      );

      if (newVal === originalValueString) {
        setPending((previousPending) => {
          const rowMap = previousPending.get(rowIdx);
          if (!rowMap?.has(column.name)) {
            return previousPending;
          }

          const nextPending = new Map(previousPending);
          const nextRowMap = new Map(rowMap);
          nextRowMap.delete(column.name);
          if (nextRowMap.size === 0) {
            nextPending.delete(rowIdx);
          } else {
            nextPending.set(rowIdx, nextRowMap);
          }

          return nextPending;
        });
        return;
      }

      setPending((previousPending) => {
        const nextPending = new Map(previousPending);
        const nextRowMap = new Map(nextPending.get(rowIdx) ?? []);
        nextRowMap.set(column.name, coerced);
        nextPending.set(rowIdx, nextRowMap);
        return nextPending;
      });
    },
    [history],
  );

  const commitDraftCellEdit = useCallback(
    (column: ColumnMeta, newVal: string) => {
      setEditCell(null);

      // Push current state to history BEFORE making the change
      history.push(
        buildUndoRedoSnapshot(pendingEditsRef.current, newRowRef.current, null),
      );

      setNewRow((currentRow) => {
        if (!currentRow) {
          return currentRow;
        }

        return {
          ...currentRow,
          [column.name]: {
            ...currentRow[column.name],
            value: newVal === NULL_SENTINEL ? NULL_SENTINEL : newVal,
          },
        };
      });
    },
    [history],
  );

  const handleStartEdit = useCallback((rowIdx: number, column: ColumnMeta) => {
    if (!canEditColumn(column)) {
      return;
    }

    setEditCell({ kind: "persisted", rowIdx, col: column.name });
    setApplyStatus(null);
  }, []);

  const handleStartDraftEdit = useCallback((column: ColumnMeta) => {
    setEditCell({ kind: "draft", col: column.name });
    setApplyStatus(null);
  }, []);

  const openStructuredCellDialog = useCallback(
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

      setEditCell(null);
      setApplyStatus(null);
      setStructuredCellDialog({
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
    [],
  );

  const updateStructuredCellDialogDraft = useCallback((nextValue: string) => {
    setStructuredCellDialog((currentDialog) => {
      if (!currentDialog) {
        return currentDialog;
      }

      return {
        ...currentDialog,
        draftText: nextValue,
        isNull: false,
      };
    });
  }, []);

  const cancelStructuredCellDialog = useCallback(() => {
    setStructuredCellDialog(null);
  }, []);

  const commitStructuredCellDialogValue = useCallback(
    (dialog: StructuredCellDialogState, nextValue: string) => {
      if (dialog.rowKind === "persisted" && dialog.rowIdx !== null) {
        commitCellEdit(
          dialog.rowIdx,
          dialog.column,
          nextValue,
          dialog.originalValue,
        );
        return;
      }

      commitDraftCellEdit(dialog.column, nextValue);
    },
    [commitCellEdit, commitDraftCellEdit],
  );

  const confirmStructuredCellDialog = useCallback(() => {
    const dialog = structuredCellDialog;
    if (!dialog) {
      return;
    }

    setStructuredCellDialog(null);

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

    commitStructuredCellDialogValue(dialog, nextValue);
  }, [commitStructuredCellDialogValue, structuredCellDialog]);

  const setStructuredCellDialogNull = useCallback(() => {
    const dialog = structuredCellDialog;
    if (!dialog || dialog.readOnly || !dialog.nullable) {
      return;
    }

    setStructuredCellDialog(null);
    commitStructuredCellDialogValue(dialog, NULL_SENTINEL);
  }, [commitStructuredCellDialogValue, structuredCellDialog]);

  const deleteSelected = useCallback(() => {
    if (
      selectedRef.current.size === 0 ||
      pkColsRef.current.length === 0 ||
      deleting
    ) {
      return;
    }

    const toDelete = [...selectedRef.current].map((index) => {
      const row = rowsRef.current[index];
      return Object.fromEntries(
        pkColsRef.current.map((columnName) => [columnName, row[columnName]]),
      );
    });

    setDeleting(true);
    postMessage("deleteRows", { primaryKeysList: toDelete });
  }, [deleting, pkColsRef, rowsRef]);

  const undoAction = useCallback(() => {
    if (applying || inserting || deleting) return;

    const currentSnapshot = buildUndoRedoSnapshot(
      pendingEditsRef.current,
      newRowRef.current,
      editCellRef.current,
    );
    const previousSnapshot = history.undo(currentSnapshot);
    if (!previousSnapshot) return;

    const restored = applyUndoRedoSnapshot(previousSnapshot);
    setPending(restored.pendingEdits);
    setNewRow(restored.newRow);
    setEditCell(restored.editCell);
  }, [applying, inserting, deleting, history]);

  const redoAction = useCallback(() => {
    if (applying || inserting || deleting) return;

    const currentSnapshot = buildUndoRedoSnapshot(
      pendingEditsRef.current,
      newRowRef.current,
      editCellRef.current,
    );
    const nextSnapshot = history.redo(currentSnapshot);
    if (!nextSnapshot) return;

    const restored = applyUndoRedoSnapshot(nextSnapshot);
    setPending(restored.pendingEdits);
    setNewRow(restored.newRow);
    setEditCell(restored.editCell);
  }, [applying, inserting, deleting, history]);

  useEffect(() => {
    if (!canEditRows) return;

    const handleKeyDown = (event: KeyboardEvent) => {
      const tag = (event.target as HTMLElement)?.tagName;
      if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;
      if ((event.target as HTMLElement)?.closest?.(".monaco-editor")) return;

      const isMac = navigator.platform.toUpperCase().includes("MAC");
      const mod = isMac ? event.metaKey : event.ctrlKey;
      if (!mod) return;

      if (event.code === "KeyZ" && !event.shiftKey) {
        event.preventDefault();
        undoAction();
      } else if (event.code === "KeyZ" && event.shiftKey) {
        event.preventDefault();
        redoAction();
      } else if (event.key === "y") {
        event.preventDefault();
        redoAction();
      }
    };

    window.addEventListener("keydown", handleKeyDown, true);
    return () => window.removeEventListener("keydown", handleKeyDown, true);
  }, [canEditRows, undoAction, redoAction]);

  return {
    applying,
    applyStatus,
    commitCellEdit,
    commitDraftCellEdit,
    confirmMutationPreview,
    deleteSelected,
    deleting,
    dismissApplyStatus: () => setApplyStatus(null),
    dismissMutationError: () => setMutErr(null),
    editCell,
    structuredCellDialog,
    handleRowsCommitted,
    handleStartDraftEdit,
    handleStartEdit,
    inserting,
    mutErr,
    mutationPreview,
    newRow,
    pendingEdits,
    openStructuredCellDialog,
    resetForTableInit,
    revertChanges,
    cancelStructuredCellDialog,
    confirmStructuredCellDialog,
    setEditCell,
    setStructuredCellDialogNull,
    startInsertRow,
    applyChanges,
    cancelMutationPreview,
    updateStructuredCellDialogDraft,
    undoAction,
    redoAction,
    canUndo: history.canUndo,
    canRedo: history.canRedo,
  };
}
