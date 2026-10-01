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
  DeleteResultPayload,
  TableMutationPreviewPayload,
} from "../../../shared/webviewContracts";
import type {
  EditTarget,
  InsertDraftRow,
  PendingEdits,
  Row,
} from "../../types";
import { onMessage, postMessage } from "../../utils/messaging";
import type { PasteValidationError } from "../../utils/pasteUtils";
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
  INSERT_DEFAULT_SENTINEL,
  MAX_DRAFT_ROWS,
  restorePendingEdits,
  rowMutationBlockReason,
  type TableApplyStatus,
} from "./tableViewHelpers";
import { useUndoRedoHistory } from "./useUndoRedoHistory";

interface UseTableMutationControllerParams {
  mongoRowIdentity?: boolean;
  canEditRows: boolean;
  loadingRef: MutableRefObject<boolean>;
  metadataBlockedRef?: MutableRefObject<boolean>;
  committedColumnNamesRef?: MutableRefObject<ReadonlySet<string>>;
  columnsRef: MutableRefObject<ColumnMeta[]>;
  fetchPageRef: MutableRefObject<() => void>;
  pkColsRef: MutableRefObject<string[]>;
  preserveScrollPositionRef: MutableRefObject<() => void>;
  rowsRef: MutableRefObject<Row[]>;
  mongoIdTypesRef: MutableRefObject<Array<"objectId" | "string" | null>>;
  selected: ReadonlySet<number>;
}

export function useTableMutationController({
  mongoRowIdentity = false,
  canEditRows,
  loadingRef,
  metadataBlockedRef,
  committedColumnNamesRef,
  columnsRef,
  fetchPageRef,
  pkColsRef,
  preserveScrollPositionRef,
  rowsRef,
  mongoIdTypesRef,
  selected,
}: UseTableMutationControllerParams) {
  const [pendingEdits, setPending] = useState<PendingEdits>(new Map());
  const [editCell, setEditCell] = useState<EditTarget | null>(null);
  const [applying, setApplying] = useState(false);
  const [applyStatus, setApplyStatus] = useState<TableApplyStatus | null>(null);
  const [newRows, setNewRows] = useState<InsertDraftRow[]>([]);
  const [mutErr, setMutErr] = useState<string | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [deleteReconciliationPending, setDeleteReconciliationPending] =
    useState(false);
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
  const operationSequenceRef = useRef(0);
  const activeOperationIdRef = useRef<string | null>(null);
  const deleteRefreshRef = useRef(false);
  const deleteOutcomeUnknownRef = useRef(false);
  const isBusy = useCallback(
    () =>
      loadingRef.current ||
      activeOperationIdRef.current !== null ||
      deleteRefreshRef.current,
    [loadingRef],
  );

  // Refs for snapshot access inside callbacks that avoid re-creation
  const pendingEditsRef = useRef(pendingEdits);
  pendingEditsRef.current = pendingEdits;
  const newRowsRef = useRef(newRows);
  newRowsRef.current = newRows;
  const editCellRef = useRef(editCell);
  editCellRef.current = editCell;

  const history = useUndoRedoHistory();

  selectedRef.current = selected;
  canEditRowsRef.current = canEditRows;
  mutationPreviewRef.current = mutationPreview;

  const getRowMutationBlockReason = useCallback(
    (rowIdx: number) =>
      rowMutationBlockReason(
        rowsRef.current[rowIdx],
        pkColsRef.current,
        mongoIdTypesRef.current[rowIdx],
        mongoRowIdentity,
      ),
    [rowsRef, pkColsRef, mongoIdTypesRef, mongoRowIdentity],
  );
  const blockUnsafeRows = useCallback(
    (rowIndexes: Iterable<number>) => {
      for (const rowIdx of rowIndexes) {
        const reason = getRowMutationBlockReason(rowIdx);
        if (reason) {
          setMutErr(`Row ${rowIdx + 1}: ${reason}`);
          return true;
        }
      }
      return false;
    },
    [getRowMutationBlockReason],
  );

  const buildPendingUpdatesPayload = useCallback(
    (source: PendingEdits) => {
      return [...source.entries()].map(([rowIdx, columnMap]) => ({
        primaryKeys: Object.fromEntries(
          pkColsRef.current.map((columnName) => [
            columnName,
            columnName === "_id" && mongoRowIdentity
              ? {
                  $rapidbMongoId: {
                    type: mongoIdTypesRef.current[rowIdx] ?? "unsupported",
                    value: rowsRef.current[rowIdx][columnName],
                  },
                }
              : rowsRef.current[rowIdx][columnName],
          ]),
        ),
        changes: Object.fromEntries(columnMap),
        originalValues: Object.fromEntries(
          [...columnMap.keys()].map((columnName) => [
            columnName,
            rowsRef.current[rowIdx][columnName],
          ]),
        ),
      }));
    },
    [pkColsRef, rowsRef, mongoIdTypesRef, mongoRowIdentity],
  );

  const clearApplyRequestState = useCallback(() => {
    setApplying(false);
    applyPendingSnapshotRef.current = new Map();
    applyRowIndexesRef.current = [];
  }, []);

  const handleRowsCommitted = useCallback(
    (
      rows: readonly Row[],
      primaryKeyColumns: readonly string[],
      previousMongoIdTypes: readonly ("objectId" | "string" | null)[],
      nextMongoIdTypes: readonly ("objectId" | "string" | null)[],
    ) => {
      // Delete reconciliation moves surviving rows. Remap both history stacks
      // by identity so undo cannot target a different row at the old index.
      if (deleteRefreshRef.current) {
        history.remap((snapshot) => ({
          ...snapshot,
          pendingEdits: restorePendingEdits(
            buildPendingRestoreState(
              snapshot.pendingEdits,
              rowsRef.current,
              primaryKeyColumns,
              previousMongoIdTypes,
              mongoRowIdentity,
            ),
            rows,
            primaryKeyColumns,
            nextMongoIdTypes,
            mongoRowIdentity,
          ),
          editCell: null,
        }));
        deleteRefreshRef.current = false;
        setDeleteReconciliationPending(false);
      } else {
        history.clear();
      }
      const restoreSource =
        pendingRestoreRef.current ??
        buildPendingRestoreState(
          pendingEditsRef.current,
          rowsRef.current,
          primaryKeyColumns,
          previousMongoIdTypes,
          mongoRowIdentity,
        );
      const restoredPending = restorePendingEdits(
        restoreSource,
        rows,
        primaryKeyColumns,
        nextMongoIdTypes,
        mongoRowIdentity,
      );

      pendingRestoreRef.current = null;
      setPending(restoredPending);
      setEditCell(null);
    },
    [history, rowsRef, mongoRowIdentity],
  );

  const resetForTableInit = useCallback(() => {
    clearApplyRequestState();
    history.clear();
    pendingRestoreRef.current = null;
    setPending(new Map());
    setEditCell(null);
    setApplying(false);
    setDeleting(false);
    setMutationPreview(null);
    setStructuredCellDialog(null);
    setNewRows([]);
    setMutErr(null);
    setApplyStatus(null);
    activeOperationIdRef.current = null;
    deleteRefreshRef.current = false;
    setDeleteReconciliationPending(false);
    deleteOutcomeUnknownRef.current = false;
  }, [clearApplyRequestState, history]);

  useEffect(() => {
    const unApply = onMessage<ApplyResultPayload>(
      "applyResult",
      ({
        operationId,
        success,
        error,
        warning,
        failedRows,
        rowOutcomes,
        insertApplied,
      }) => {
        if (!operationId || operationId !== activeOperationIdRef.current)
          return;
        activeOperationIdRef.current = null;
        setApplying(false);

        if (success) {
          history.clear();
          setNewRows([]);
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
            mongoIdTypesRef.current,
            mongoRowIdentity,
          );
          pendingRestoreRef.current =
            restoreState.size > 0 ? restoreState : null;

          preserveScrollPositionRef.current();
          fetchPageRef.current();
        } else {
          if (insertApplied) {
            setNewRows([]);
            const restoreState = buildPendingRestoreState(
              pendingEdits,
              rowsRef.current,
              pkColsRef.current,
              mongoIdTypesRef.current,
              mongoRowIdentity,
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
              : (error ??
                "Apply failed. Refresh and verify the data before retrying."),
          });
        }

        applyPendingSnapshotRef.current = new Map();
        applyRowIndexesRef.current = [];
      },
    );

    const unDelete = onMessage<DeleteResultPayload>(
      "deleteResult",
      ({
        operationId,
        success,
        error,
        changesPossible,
        outcomeUnknown,
        affectedRows,
      }) => {
        if (!operationId || operationId !== activeOperationIdRef.current)
          return;
        activeOperationIdRef.current = null;
        setDeleting(false);
        deleteOutcomeUnknownRef.current = Boolean(outcomeUnknown);
        setMutErr(
          success
            ? null
            : (error ??
                `Delete may have partially changed the data (${affectedRows ?? 0} row(s) confirmed deleted). Refresh and verify before retrying.`),
        );
        if (outcomeUnknown) {
          setMutErr(
            `${error ?? "Delete outcome is unknown."} The first refresh may precede a late write; refresh again and verify before retrying.`,
          );
        }
        if (changesPossible !== false) {
          deleteRefreshRef.current = true;
          setDeleteReconciliationPending(true);
          pendingRestoreRef.current = buildPendingRestoreState(
            pendingEditsRef.current,
            rowsRef.current,
            pkColsRef.current,
            mongoIdTypesRef.current,
            mongoRowIdentity,
          );
          preserveScrollPositionRef.current();
          fetchPageRef.current();
        }
      },
    );

    const unMutationPreview = onMessage<TableMutationPreviewPayload>(
      "tableMutationPreview",
      (payload) => {
        if (payload.operationId !== activeOperationIdRef.current) return;
        setMutationPreview(payload);
      },
    );

    return () => {
      unApply();
      unDelete();
      unMutationPreview();
    };
  }, [
    fetchPageRef,
    history,
    mongoIdTypesRef,
    mongoRowIdentity,
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
    } else {
      setDeleting(false);
    }

    postMessage("cancelMutationPreview", {
      operationId: preview.operationId,
      previewToken,
    });
    activeOperationIdRef.current = null;
  }, [clearApplyRequestState]);

  const confirmMutationPreview = useCallback(() => {
    const preview = mutationPreviewRef.current;
    if (!preview) {
      return;
    }

    setMutationPreview(null);
    postMessage("confirmMutationPreview", {
      operationId: preview.operationId,
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
    if (metadataBlockedRef?.current) return;
    if (isBusy()) return;
    if (applying || deleting) return;
    if (newRowsRef.current.length >= MAX_DRAFT_ROWS) return;
    history.push(
      buildUndoRedoSnapshot(pendingEditsRef.current, newRowsRef.current, null),
    );
    setNewRows((prev) => [createInsertDraft(columnsRef.current), ...prev]);
    setEditCell(null);
    setMutErr(null);
  }, [applying, columnsRef, deleting, history, isBusy, metadataBlockedRef]);

  const applyChanges = useCallback(() => {
    if (metadataBlockedRef?.current) return;
    if (isBusy()) return;
    const unsavedRowCount = pendingEdits.size + newRows.length;
    if (unsavedRowCount === 0 || applying) {
      return;
    }
    if (blockUnsafeRows(pendingEdits.keys())) return;

    applyPendingSnapshotRef.current = clonePendingEdits(pendingEdits);
    applyRowIndexesRef.current = [...pendingEdits.keys()];

    setApplying(true);
    setApplyStatus(null);
    setMutErr(null);
    const operationId = `table-mutation:${++operationSequenceRef.current}`;
    activeOperationIdRef.current = operationId;

    const updates = buildPendingUpdatesPayload(pendingEdits);
    // An empty values object inserts a row using database defaults.
    const insertValues = newRows.map(buildInsertValues);
    postMessage("applyChanges", {
      operationId,
      updates,
      ...(insertValues.length > 0 ? { insertValues } : {}),
    });
  }, [
    applying,
    buildPendingUpdatesPayload,
    newRows,
    pendingEdits,
    isBusy,
    metadataBlockedRef,
    blockUnsafeRows,
  ]);

  const revertChanges = useCallback(() => {
    if (loadingRef.current || activeOperationIdRef.current !== null) return;
    history.clear();
    pendingRestoreRef.current = null;
    setPending(new Map());
    setNewRows([]);
    setEditCell(null);
    setStructuredCellDialog(null);
    setMutErr(null);
    setApplyStatus(null);
  }, [history, loadingRef]);

  // Validate the entire persisted target set against the live read baseline.
  // Metadata may have changed while a clipboard request was in flight.
  const validatePersistedBatch = useCallback(
    (edits: Array<{ rowIdx: number; column: ColumnMeta; newVal: string }>) => {
      const errors: PasteValidationError[] = [];
      for (const { rowIdx, column, newVal } of edits) {
        if (
          committedColumnNamesRef &&
          !committedColumnNamesRef.current.has(column.name)
        ) {
          errors.push({
            rowIndex: rowIdx,
            columnIndex: columnsRef.current.findIndex(
              (c) => c.name === column.name,
            ),
            columnName: column.name,
            value: newVal,
            message: `Column "${column.name}" has not been read for these rows. Apply or revert pending changes, then refresh before pasting.`,
          });
        }
      }
      return errors;
    },
    [committedColumnNamesRef, columnsRef],
  );

  const commitBatchCellEdits = useCallback(
    (
      edits: Array<{
        rowIdx: number;
        column: ColumnMeta;
        newVal: string;
        originalVal: unknown;
      }>,
    ) => {
      if (isBusy() || !canEditRowsRef.current) {
        return;
      }
      const errors = validatePersistedBatch(edits);
      if (errors.length > 0) return errors;
      if (blockUnsafeRows(edits.map((edit) => edit.rowIdx))) return;
      setEditCell(null);

      const currentPending = pendingEditsRef.current;

      const effectiveEdits: Array<{
        rowIdx: number;
        column: ColumnMeta;
        newVal: string;
        originalVal: unknown;
      }> = [];

      for (const edit of edits) {
        const { rowIdx, column, newVal, originalVal } = edit;
        const coerced: unknown = newVal === NULL_SENTINEL ? null : newVal;
        const originalValueString = valueToEditString(originalVal);

        if (newVal === originalValueString) {
          if (currentPending.get(rowIdx)?.has(column.name)) {
            effectiveEdits.push(edit);
          }
        } else {
          if (currentPending.get(rowIdx)?.get(column.name) !== coerced) {
            effectiveEdits.push(edit);
          }
        }
      }

      if (effectiveEdits.length === 0) return;

      history.push(
        buildUndoRedoSnapshot(currentPending, newRowsRef.current, null),
      );

      setPending((previousPending) => {
        const nextPending = new Map(previousPending);

        for (const { rowIdx, column, newVal, originalVal } of effectiveEdits) {
          const coerced: unknown = newVal === NULL_SENTINEL ? null : newVal;
          const originalValueString = valueToEditString(originalVal);

          if (newVal === originalValueString) {
            const rowMap = nextPending.get(rowIdx);
            if (rowMap?.has(column.name)) {
              const nextRowMap = new Map(rowMap);
              nextRowMap.delete(column.name);
              if (nextRowMap.size === 0) {
                nextPending.delete(rowIdx);
              } else {
                nextPending.set(rowIdx, nextRowMap);
              }
            }
          } else {
            const existing = nextPending.get(rowIdx)?.get(column.name);
            if (existing !== coerced) {
              const nextRowMap = new Map(nextPending.get(rowIdx) ?? []);
              nextRowMap.set(column.name, coerced);
              nextPending.set(rowIdx, nextRowMap);
            }
          }
        }

        return nextPending;
      });
    },
    [history, isBusy, validatePersistedBatch, blockUnsafeRows],
  );

  const commitCellEdit = useCallback(
    (
      rowIdx: number,
      column: ColumnMeta,
      newVal: string,
      originalVal: unknown,
    ) => {
      if (
        committedColumnNamesRef &&
        !committedColumnNamesRef.current.has(column.name)
      )
        return;
      setEditCell(null);

      if (isBusy() || !canEditRowsRef.current) {
        return;
      }
      if (blockUnsafeRows([rowIdx])) return;

      const coerced: unknown = newVal === NULL_SENTINEL ? null : newVal;
      const originalValueString = valueToEditString(originalVal);
      const currentPending = pendingEditsRef.current;

      if (newVal === originalValueString) {
        if (!currentPending.get(rowIdx)?.has(column.name)) {
          return;
        }
      } else {
        if (currentPending.get(rowIdx)?.get(column.name) === coerced) {
          return;
        }
      }

      history.push(
        buildUndoRedoSnapshot(currentPending, newRowsRef.current, null),
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
        const existing = nextPending.get(rowIdx)?.get(column.name);
        if (existing === coerced) return previousPending;
        const nextRowMap = new Map(nextPending.get(rowIdx) ?? []);
        nextRowMap.set(column.name, coerced);
        nextPending.set(rowIdx, nextRowMap);
        return nextPending;
      });
    },
    [history, isBusy, committedColumnNamesRef, blockUnsafeRows],
  );

  const commitDraftCellEdit = useCallback(
    (rowIdx: number, column: ColumnMeta, newVal: string) => {
      if (isBusy()) return;
      setEditCell(null);

      const currentRows = newRowsRef.current;
      if (rowIdx < 0 || rowIdx >= currentRows.length) return;

      const norm = newVal === NULL_SENTINEL ? NULL_SENTINEL : newVal;

      if (currentRows[rowIdx][column.name]?.value === norm) return;

      history.push(
        buildUndoRedoSnapshot(pendingEditsRef.current, currentRows, null),
      );

      setNewRows((prev) =>
        prev.map((draft, i) =>
          i === rowIdx
            ? {
                ...draft,
                [column.name]: { ...draft[column.name], value: norm },
              }
            : draft,
        ),
      );
    },
    [history, isBusy],
  );

  const commitBatchDraftCellEdits = useCallback(
    (rowIdx: number, edits: Array<{ column: ColumnMeta; newVal: string }>) => {
      if (isBusy()) return;
      setEditCell(null);

      const currentRows = newRowsRef.current;
      if (rowIdx < 0 || rowIdx >= currentRows.length) return;
      const currentRow = currentRows[rowIdx];

      const normEdits: Array<{ columnName: string; value: string }> = [];

      for (const { column, newVal } of edits) {
        const norm = newVal === NULL_SENTINEL ? NULL_SENTINEL : newVal;
        const prev = currentRow[column.name]?.value;
        if (prev === norm) continue;
        normEdits.push({ columnName: column.name, value: norm });
      }

      if (normEdits.length === 0) return;

      history.push(
        buildUndoRedoSnapshot(pendingEditsRef.current, currentRows, null),
      );

      setNewRows((prev) =>
        prev.map((draft, i) => {
          if (i !== rowIdx) return draft;
          const next = { ...draft };
          for (const e of normEdits) {
            next[e.columnName] = { ...next[e.columnName], value: e.value };
          }
          return next;
        }),
      );
    },
    [history, isBusy],
  );

  const commitMixedBatchEdits = useCallback(
    (
      draftEdits: Array<{ rowIdx: number; column: ColumnMeta; newVal: string }>,
      persistedEdits: Array<{
        rowIdx: number;
        column: ColumnMeta;
        newVal: string;
        originalVal: unknown;
      }>,
    ) => {
      if (isBusy()) return;
      const errors = validatePersistedBatch(persistedEdits);
      if (errors.length > 0) return errors;
      if (blockUnsafeRows(persistedEdits.map((edit) => edit.rowIdx))) return;
      setEditCell(null);

      const currentRows = newRowsRef.current;
      const currentPending = pendingEditsRef.current;

      const draftEditsByRow = new Map<
        number,
        Array<{ columnName: string; value: string }>
      >();
      for (const { rowIdx, column, newVal } of draftEdits) {
        if (rowIdx < 0 || rowIdx >= currentRows.length) continue;
        const norm = newVal === NULL_SENTINEL ? NULL_SENTINEL : newVal;
        const prev = currentRows[rowIdx][column.name]?.value;
        if (prev === norm) continue;
        let edits = draftEditsByRow.get(rowIdx);
        if (!edits) {
          edits = [];
          draftEditsByRow.set(rowIdx, edits);
        }
        edits.push({ columnName: column.name, value: norm as string });
      }

      const effectivePersistedEdits: typeof persistedEdits = [];
      for (const edit of persistedEdits) {
        const { rowIdx, column, newVal, originalVal } = edit;
        const coerced: unknown = newVal === NULL_SENTINEL ? null : newVal;
        const originalValueString = valueToEditString(originalVal);

        if (newVal === originalValueString) {
          if (currentPending.get(rowIdx)?.has(column.name)) {
            effectivePersistedEdits.push(edit);
          }
        } else {
          if (currentPending.get(rowIdx)?.get(column.name) !== coerced) {
            effectivePersistedEdits.push(edit);
          }
        }
      }

      if (draftEditsByRow.size === 0 && effectivePersistedEdits.length === 0) {
        return;
      }

      history.push(buildUndoRedoSnapshot(currentPending, currentRows, null));

      if (draftEditsByRow.size > 0) {
        setNewRows((prev) =>
          prev.map((draft, i) => {
            const rowEdits = draftEditsByRow.get(i);
            if (!rowEdits) return draft;
            const next = { ...draft };
            for (const e of rowEdits) {
              next[e.columnName] = { ...next[e.columnName], value: e.value };
            }
            return next;
          }),
        );
      }

      if (effectivePersistedEdits.length > 0) {
        setPending((previousPending) => {
          const nextPending = new Map(previousPending);

          for (const {
            rowIdx,
            column,
            newVal,
            originalVal,
          } of effectivePersistedEdits) {
            const coerced: unknown = newVal === NULL_SENTINEL ? null : newVal;
            const originalValueString = valueToEditString(originalVal);

            if (newVal === originalValueString) {
              const rowMap = nextPending.get(rowIdx);
              if (rowMap?.has(column.name)) {
                const nextRowMap = new Map(rowMap);
                nextRowMap.delete(column.name);
                if (nextRowMap.size === 0) {
                  nextPending.delete(rowIdx);
                } else {
                  nextPending.set(rowIdx, nextRowMap);
                }
              }
            } else {
              const existing = nextPending.get(rowIdx)?.get(column.name);
              if (existing !== coerced) {
                const nextRowMap = new Map(nextPending.get(rowIdx) ?? []);
                nextRowMap.set(column.name, coerced);
                nextPending.set(rowIdx, nextRowMap);
              }
            }
          }

          return nextPending;
        });
      }
    },
    [history, isBusy, validatePersistedBatch, blockUnsafeRows],
  );

  const handleStartEdit = useCallback(
    (rowIdx: number, column: ColumnMeta) => {
      if (
        committedColumnNamesRef &&
        !committedColumnNamesRef.current.has(column.name)
      )
        return;
      if (isBusy()) return;
      if (canEditRowsRef.current && blockUnsafeRows([rowIdx])) return;
      if (!canEditColumn(column)) {
        return;
      }

      setEditCell({ kind: "persisted", rowIdx, col: column.name });
      setApplyStatus(null);
    },
    [isBusy, committedColumnNamesRef, blockUnsafeRows],
  );

  const handleStartDraftEdit = useCallback(
    (rowIdx: number, column: ColumnMeta) => {
      if (isBusy()) return;
      setEditCell({ kind: "draft", rowIdx, col: column.name });
      setApplyStatus(null);
    },
    [isBusy],
  );

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
      if (isBusy()) return;
      const {
        rowKind,
        rowIdx,
        column,
        value,
        currentValue,
        originalValue,
        readOnly: requestedReadOnly,
      } = options;
      const rowBlockReason =
        rowKind === "persisted" && rowIdx !== undefined
          ? getRowMutationBlockReason(rowIdx)
          : null;
      const readOnly =
        requestedReadOnly ||
        (rowKind === "persisted" &&
          (rowIdx === undefined ||
            Boolean(rowBlockReason) ||
            Boolean(
              committedColumnNamesRef &&
                !committedColumnNamesRef.current.has(column.name),
            )));
      const dataDescription =
        value.kind === "text" ? "Text data" : `Structured ${value.kind} data`;

      setEditCell(null);
      setApplyStatus(null);
      setStructuredCellDialog({
        rowKind,
        rowIdx: rowIdx ?? null,
        column,
        title: `Cell data: ${column.name}`,
        description: readOnly
          ? `${dataDescription} in ${column.name}. ${rowBlockReason ?? "Apply closes this dialog without sending a mutation."}`
          : rowKind === "draft"
            ? `${dataDescription} in ${column.name}. Apply updates the pending inserted row only.`
            : `${dataDescription} in ${column.name}. Apply updates the local pending cell edit only.`,
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
    [isBusy, committedColumnNamesRef, getRowMutationBlockReason],
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
      if (dialog.rowIdx !== null) {
        if (dialog.rowKind === "persisted") {
          commitCellEdit(
            dialog.rowIdx,
            dialog.column,
            nextValue,
            dialog.originalValue,
          );
        } else {
          commitDraftCellEdit(dialog.rowIdx, dialog.column, nextValue);
        }
        return;
      }
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

  const setStructuredCellDialogDefault = useCallback(() => {
    const dialog = structuredCellDialog;
    if (!dialog || dialog.readOnly || dialog.rowKind !== "draft") {
      return;
    }

    setStructuredCellDialog(null);
    commitStructuredCellDialogValue(dialog, INSERT_DEFAULT_SENTINEL);
  }, [commitStructuredCellDialogValue, structuredCellDialog]);

  const deleteSelected = useCallback(() => {
    if (metadataBlockedRef?.current) return;
    if (isBusy()) return;
    if (
      selectedRef.current.size === 0 ||
      pkColsRef.current.length === 0 ||
      deleting ||
      applying
    ) {
      return;
    }

    if (blockUnsafeRows(selectedRef.current)) return;
    const toDelete = [...selectedRef.current].map((index) => {
      const row = rowsRef.current[index];
      return Object.fromEntries(
        pkColsRef.current.map((columnName) => [
          columnName,
          columnName === "_id" && mongoRowIdentity
            ? {
                $rapidbMongoId: {
                  type: mongoIdTypesRef.current[index] ?? "unsupported",
                  value: row[columnName],
                },
              }
            : row[columnName],
        ]),
      );
    });

    setDeleting(true);
    const operationId = `table-mutation:${++operationSequenceRef.current}`;
    activeOperationIdRef.current = operationId;
    postMessage("deleteRows", { operationId, primaryKeysList: toDelete });
  }, [
    applying,
    deleting,
    pkColsRef,
    rowsRef,
    mongoIdTypesRef,
    mongoRowIdentity,
    isBusy,
    metadataBlockedRef,
    blockUnsafeRows,
  ]);

  const undoAction = useCallback(() => {
    if (isBusy()) return;
    if (applying || deleting) return;

    const currentSnapshot = buildUndoRedoSnapshot(
      pendingEditsRef.current,
      newRowsRef.current,
      editCellRef.current,
    );
    const previousSnapshot = history.undo(currentSnapshot);
    if (!previousSnapshot) return;

    const restored = applyUndoRedoSnapshot(previousSnapshot);
    setPending(restored.pendingEdits);
    setNewRows(restored.newRows);
    setEditCell(restored.editCell);
  }, [applying, deleting, history, isBusy]);

  const redoAction = useCallback(() => {
    if (isBusy()) return;
    if (applying || deleting) return;

    const currentSnapshot = buildUndoRedoSnapshot(
      pendingEditsRef.current,
      newRowsRef.current,
      editCellRef.current,
    );
    const nextSnapshot = history.redo(currentSnapshot);
    if (!nextSnapshot) return;

    const restored = applyUndoRedoSnapshot(nextSnapshot);
    setPending(restored.pendingEdits);
    setNewRows(restored.newRows);
    setEditCell(restored.editCell);
  }, [applying, deleting, history, isBusy]);

  useEffect(() => {
    if (!canEditRows) return;

    const handleKeyDown = (event: KeyboardEvent) => {
      const tag = (event.target as HTMLElement)?.tagName;
      if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;
      if ((event.target as HTMLElement)?.closest?.(".monaco-editor")) return;

      const isMac = navigator.platform.toUpperCase().includes("MAC");
      const mod = isMac ? event.metaKey : event.ctrlKey;
      if (!mod || event.altKey) return;

      if (event.code === "KeyZ" && !event.shiftKey) {
        event.preventDefault();
        undoAction();
      } else if (event.code === "KeyZ" && event.shiftKey) {
        event.preventDefault();
        redoAction();
      } else if (event.code === "KeyY" && !event.shiftKey) {
        event.preventDefault();
        redoAction();
      }
    };

    window.addEventListener("keydown", handleKeyDown, true);
    return () => window.removeEventListener("keydown", handleKeyDown, true);
  }, [canEditRows, undoAction, redoAction]);

  return {
    getRowMutationBlockReason,
    getMetadataRefreshState: () => ({
      reconciliationPending: deleteRefreshRef.current,
      busy:
        activeOperationIdRef.current !== null ||
        editCellRef.current !== null ||
        structuredCellDialog !== null,
      hasWork:
        pendingEditsRef.current.size > 0 ||
        newRowsRef.current.length > 0 ||
        history.canUndo ||
        history.canRedo,
    }),
    applying,
    applyStatus,
    commitBatchCellEdits,
    commitBatchDraftCellEdits,
    commitMixedBatchEdits,
    commitCellEdit,
    commitDraftCellEdit,
    confirmMutationPreview,
    deleteSelected,
    deleting,
    deleteReconciliationPending,
    dismissApplyStatus: () => setApplyStatus(null),
    dismissMutationError: () => setMutErr(null),
    editCell,
    structuredCellDialog,
    handleRowsCommitted,
    retryDeleteRefresh: () => {
      if (!deleteRefreshRef.current && !deleteOutcomeUnknownRef.current)
        return false;
      // Reconciliation itself blocks writes, but must not block its read retry.
      if (loadingRef.current || activeOperationIdRef.current !== null)
        return true;
      deleteRefreshRef.current = true;
      setDeleteReconciliationPending(true);
      pendingRestoreRef.current = buildPendingRestoreState(
        pendingEditsRef.current,
        rowsRef.current,
        pkColsRef.current,
        mongoIdTypesRef.current,
        mongoRowIdentity,
      );
      preserveScrollPositionRef.current();
      fetchPageRef.current();
      return true;
    },
    handleReadFailed: () => {
      // The old rows remain committed, so future restores must use current edits.
      pendingRestoreRef.current = null;
      if (deleteRefreshRef.current) {
        setMutErr((current) =>
          `${current ?? ""} The delete refresh failed. Refresh and verify the data before making further changes.`.trim(),
        );
      }
    },
    handleStartDraftEdit,
    handleStartEdit,
    mutErr,
    mutationPreview,
    newRows,
    pendingEdits,
    openStructuredCellDialog,
    resetForTableInit,
    revertChanges,
    cancelStructuredCellDialog,
    confirmStructuredCellDialog,
    setEditCell,
    setStructuredCellDialogNull,
    setStructuredCellDialogDefault,
    startInsertRow,
    applyChanges,
    cancelMutationPreview,
    updateStructuredCellDialogDraft,
    undoAction,
    redoAction,
    canUndo: history.canUndo,
    canRedo: history.canRedo,
    blockNavigationWithUnsavedChanges: () =>
      setApplyStatus({
        tone: "warning",
        message:
          "Apply or revert pending changes before loading different rows.",
      }),
  };
}
