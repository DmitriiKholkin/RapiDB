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
import {
  buildInsertValues,
  buildPendingRestoreState,
  canEditColumn,
  clonePendingEdits,
  createInsertDraft,
  getRetainedPendingEdits,
  restorePendingEdits,
  type TableApplyStatus,
} from "./tableViewHelpers";

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

  const applyPendingSnapshotRef = useRef<PendingEdits>(new Map());
  const applyRowIndexesRef = useRef<number[]>([]);
  const pendingRestoreRef = useRef<Map<string, Map<string, unknown>> | null>(
    null,
  );
  const selectedRef = useRef(selected);
  const canEditRowsRef = useRef(canEditRows);
  const mutationPreviewRef = useRef(mutationPreview);

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
    pendingRestoreRef.current = null;
    setPending(new Map());
    setEditCell(null);
    setApplying(false);
    setDeleting(false);
    setInserting(false);
    setMutationPreview(null);
    setNewRow(null);
    setMutErr(null);
    setApplyStatus(null);
  }, [clearApplyRequestState]);

  useEffect(() => {
    const unApply = onMessage<ApplyResultPayload>(
      "applyResult",
      ({ success, error, warning, failedRows, rowOutcomes, insertApplied }) => {
        setApplying(false);

        if (success) {
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
    setNewRow(createInsertDraft(columnsRef.current));
    setEditCell(null);
    setMutErr(null);
  }, [columnsRef]);

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
    pendingRestoreRef.current = null;
    setPending(new Map());
    setNewRow(null);
    setEditCell(null);
    setMutErr(null);
    setApplyStatus(null);
  }, []);

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
    [],
  );

  const commitDraftCellEdit = useCallback(
    (column: ColumnMeta, newVal: string) => {
      setEditCell(null);

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
    [],
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
    handleRowsCommitted,
    handleStartDraftEdit,
    handleStartEdit,
    inserting,
    mutErr,
    mutationPreview,
    newRow,
    pendingEdits,
    resetForTableInit,
    revertChanges,
    setEditCell,
    startInsertRow,
    applyChanges,
    cancelMutationPreview,
  };
}
