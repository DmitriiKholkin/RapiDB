import {
  type MutableRefObject,
  type SetStateAction,
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react";
import {
  type ColumnTypeMeta as ColumnMeta,
  type MongoValueTypeHint,
  NULL_SENTINEL,
} from "../../../shared/tableTypes";
import type {
  ApplyResultPayload,
  DeleteResultPayload,
  TableInitPayload,
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
import { readWebviewState, updateWebviewState } from "../../utils/vscodeState";
import { valueToEditString } from "./EditInput";
import type {
  StructuredCellDialogState,
  StructuredCellDialogValue,
} from "./structuredCellDialog";
import {
  getStructuredCellDialogValue,
  serializeStructuredCellDialogDraft,
} from "./structuredCellDialog";
import {
  applyUndoRedoSnapshot,
  buildInsertValues,
  buildPendingRestoreState,
  buildUndoRedoSnapshot,
  canEditColumn,
  canOpenColumn,
  clonePendingEdits,
  createInsertDraft,
  getRetainedPendingEdits,
  INSERT_DEFAULT_SENTINEL,
  MAX_DRAFT_ROWS,
  type PendingRestoreEntry,
  type PendingRestoreState,
  type PersistedTableDraft,
  type PersistedTableMutation,
  restorePendingEditsSafely,
  rowMutationBlockReason,
  rowPrimaryKeySignature,
  serializeTableDraft,
  type TableApplyStatus,
} from "./tableViewHelpers";
import { useUndoRedoHistory } from "./useUndoRedoHistory";

interface UseTableMutationControllerParams {
  pasteStructureEpochRef?: MutableRefObject<number>;
  initialDraft?: PersistedTableDraft | null;
  tableDraftKey?: string;
  mongoRowIdentity?: boolean;
  canEditRows: boolean;
  canInsertRows?: boolean;
  loadingRef: MutableRefObject<boolean>;
  metadataBlockedRef?: MutableRefObject<boolean>;
  committedColumnNamesRef?: MutableRefObject<ReadonlySet<string>>;
  columnsRef: MutableRefObject<ColumnMeta[]>;
  fetchPageRef: MutableRefObject<() => void>;
  pkColsRef: MutableRefObject<string[]>;
  preserveScrollPositionRef: MutableRefObject<() => void>;
  rowsRef: MutableRefObject<Row[]>;
  mongoIdTypesRef: MutableRefObject<Array<"objectId" | "string" | null>>;
  rowValueTypesRef: MutableRefObject<Array<Record<string, MongoValueTypeHint>>>;
  selected: ReadonlySet<number>;
}

interface ReconciliationState {
  kind: "apply" | "delete";
  unknown: boolean;
  verified: boolean;
  requiresExplicitRead?: boolean;
}

const RESTORED_MUTATION_WARNING =
  "A previous table write may have been applied. Refresh and verify before retrying. The initial read may precede a late write; use Refresh again before retrying or reverting.";
const UNKNOWN_INSERT_WARNING =
  "Insert outcome is unknown. These insert drafts cannot be reapplied. Refresh and verify the data, then use Revert All to discard the drafts. Create new drafts only for rows confirmed missing. The first refresh may precede a late write; refresh again and verify before creating new drafts.";

function persistedEditMatchesOriginal(
  newVal: string,
  originalVal: unknown,
): boolean {
  if (newVal === NULL_SENTINEL) {
    return originalVal === null;
  }
  if (originalVal == null) {
    return false;
  }
  return newVal === valueToEditString(originalVal);
}

function hasUnknownInsertDrafts(
  recovery: ReconciliationState | null,
  drafts: readonly InsertDraftRow[],
): boolean {
  return recovery?.kind === "apply" && recovery.unknown && drafts.length > 0;
}

export function useTableMutationController({
  pasteStructureEpochRef: externalPasteStructureEpochRef,
  initialDraft,
  tableDraftKey,
  mongoRowIdentity = false,
  canEditRows,
  canInsertRows = canEditRows,
  loadingRef,
  metadataBlockedRef,
  committedColumnNamesRef,
  columnsRef,
  fetchPageRef,
  pkColsRef,
  preserveScrollPositionRef,
  rowsRef,
  mongoIdTypesRef,
  rowValueTypesRef,
  selected,
}: UseTableMutationControllerParams) {
  const localPasteStructureEpochRef = useRef(0);
  const pasteStructureEpochRef =
    externalPasteStructureEpochRef ?? localPasteStructureEpochRef;
  const [pendingEdits, setPendingState] = useState<PendingEdits>(new Map());
  const [unresolvedPendingEdits, setUnresolvedPendingEdits] = useState<
    PendingRestoreEntry[]
  >([]);
  const [editCell, setEditCell] = useState<EditTarget | null>(null);
  const [applying, setApplying] = useState(false);
  const [applyStatus, setApplyStatus] = useState<TableApplyStatus | null>(
    initialDraft?.mutation?.kind === "apply"
      ? {
          tone: "error",
          message:
            initialDraft.mutation.unknown && initialDraft.newRows.length > 0
              ? `${RESTORED_MUTATION_WARNING} ${UNKNOWN_INSERT_WARNING}`
              : RESTORED_MUTATION_WARNING,
        }
      : null,
  );
  const [newRows, setNewRowsState] = useState<InsertDraftRow[]>(
    initialDraft?.newRows ?? [],
  );
  const [mutErr, setMutErr] = useState<string | null>(
    initialDraft?.mutation?.kind === "delete"
      ? RESTORED_MUTATION_WARNING
      : null,
  );
  const [deleting, setDeleting] = useState(false);
  const [reconciliation, setReconciliationState] =
    useState<ReconciliationState | null>(
      initialDraft?.mutation
        ? {
            kind: initialDraft.mutation.kind,
            unknown: initialDraft.mutation.unknown,
            verified: false,
            requiresExplicitRead: true,
          }
        : null,
    );
  const [persistedMutation, setPersistedMutation] = useState(
    initialDraft?.mutation,
  );
  const [mutationPreview, setMutationPreview] =
    useState<TableMutationPreviewPayload | null>(null);
  const [structuredCellDialog, setStructuredCellDialog] =
    useState<StructuredCellDialogState | null>(null);

  const pendingEditsRef = useRef(pendingEdits);
  const newRowsRef = useRef(newRows);
  const setPending = useCallback((update: SetStateAction<PendingEdits>) => {
    const next =
      typeof update === "function" ? update(pendingEditsRef.current) : update;
    pendingEditsRef.current = next;
    setPendingState(next);
  }, []);
  const setNewRows = useCallback((update: SetStateAction<InsertDraftRow[]>) => {
    const next =
      typeof update === "function" ? update(newRowsRef.current) : update;
    newRowsRef.current = next;
    setNewRowsState(next);
  }, []);

  const applyPendingSnapshotRef = useRef<PendingEdits>(new Map());
  const applyRowIndexesRef = useRef<number[]>([]);
  const deleteRequestSignaturesRef = useRef<Array<string | null>>([]);
  const pendingRestoreRef = useRef<PendingRestoreState | null>(
    initialDraft?.restoreState ?? null,
  );
  const initialDraftPendingRef = useRef(
    initialDraft !== null && initialDraft !== undefined,
  );
  const didHydrateDraftRef = useRef(false);
  const [awaitingDraftRestore, setAwaitingDraftRestore] = useState(
    () => (initialDraft?.restoreState?.entries.length ?? 0) > 0,
  );
  const unresolvedPendingEditsRef = useRef<PendingRestoreEntry[]>([]);
  const selectedRef = useRef(selected);
  const canEditRowsRef = useRef(canEditRows);
  const mutationPreviewRef = useRef(mutationPreview);
  const structuredCellDialogRef = useRef(structuredCellDialog);
  const operationSequenceRef = useRef(0);
  const operationEpochRef = useRef(
    `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`,
  );
  const activeOperationIdRef = useRef<string | null>(null);
  const reconciliationRef = useRef<ReconciliationState | null>(reconciliation);
  const setReconciliation = useCallback((state: ReconciliationState | null) => {
    reconciliationRef.current = state;
    setReconciliationState(state);
    setPersistedMutation(
      state ? { kind: state.kind, unknown: state.unknown } : undefined,
    );
  }, []);
  const dismissApplyStatus = useCallback(() => {
    const recovery = reconciliationRef.current;
    if (recovery?.kind === "apply" && recovery.unknown) return;
    setApplyStatus(null);
  }, []);
  const setLocalMutationError = useCallback((message: string | null) => {
    const recovery = reconciliationRef.current;
    if (recovery?.kind === "delete" && recovery.unknown) {
      if (message !== null) {
        setMutErr((current) => `${current ?? ""} ${message}`.trim());
      }
      return;
    }
    setMutErr(message);
  }, []);
  const isBusy = useCallback(
    () =>
      loadingRef.current ||
      activeOperationIdRef.current !== null ||
      (reconciliationRef.current !== null &&
        !reconciliationRef.current.verified) ||
      hasUnknownInsertDrafts(reconciliationRef.current, newRowsRef.current),
    [loadingRef],
  );

  unresolvedPendingEditsRef.current = unresolvedPendingEdits;
  const editCellRef = useRef(editCell);
  editCellRef.current = editCell;

  const history = useUndoRedoHistory();

  selectedRef.current = selected;
  canEditRowsRef.current = canEditRows;
  mutationPreviewRef.current = mutationPreview;
  structuredCellDialogRef.current = structuredCellDialog;

  const persistDraft = useCallback(
    (
      mutation?: PersistedTableMutation,
      edits = pendingEditsRef.current,
      inserts = newRowsRef.current,
      unresolved = unresolvedPendingEditsRef.current,
    ) => {
      if (!tableDraftKey) return;
      const restoreState = buildPendingRestoreState(
        edits,
        rowsRef.current,
        pkColsRef.current,
        mongoIdTypesRef.current,
        mongoRowIdentity,
        unresolved,
        rowValueTypesRef.current,
      );
      const hasDraft =
        restoreState.entries.length > 0 ||
        inserts.length > 0 ||
        mutation !== undefined;
      updateWebviewState((state) => {
        const next = { ...state };
        if (hasDraft) {
          next.tableDraft = serializeTableDraft(
            tableDraftKey,
            restoreState,
            inserts,
            mutation,
          );
        } else {
          delete next.tableDraft;
        }
        return next;
      });
    },
    [
      mongoRowIdentity,
      rowsRef,
      pkColsRef,
      mongoIdTypesRef,
      rowValueTypesRef,
      tableDraftKey,
    ],
  );

  useEffect(() => {
    if (!didHydrateDraftRef.current) return;
    persistDraft(
      persistedMutation,
      pendingEdits,
      newRows,
      unresolvedPendingEdits,
    );
  }, [
    persistDraft,
    persistedMutation,
    pendingEdits,
    newRows,
    unresolvedPendingEdits,
  ]);

  const hasUnsavedChanges =
    awaitingDraftRestore ||
    pendingEdits.size > 0 ||
    unresolvedPendingEdits.length > 0 ||
    newRows.length > 0;
  const [hostAssumesRestoredDraft] = useState(
    () =>
      readWebviewState<Record<string, unknown>>({}).tableDraft !== undefined,
  );
  const reportedUnsavedChangesRef = useRef<boolean | null>(
    hostAssumesRestoredDraft ? null : false,
  );
  useEffect(() => {
    if (reportedUnsavedChangesRef.current === hasUnsavedChanges) return;
    reportedUnsavedChangesRef.current = hasUnsavedChanges;
    postMessage("unsavedChangesState", { hasUnsavedChanges });
  }, [hasUnsavedChanges]);

  useEffect(
    () =>
      onMessage<TableInitPayload>("tableInit", ({ connectionSessionKey }) => {
        if (connectionSessionKey === undefined) return;
        updateWebviewState((state) => {
          const initialState = state.initialState;
          if (
            !initialState ||
            typeof initialState !== "object" ||
            (initialState as Record<string, unknown>).connectionSessionKey ===
              connectionSessionKey
          )
            return state;
          return {
            ...state,
            initialState: { ...initialState, connectionSessionKey },
          };
        });
      }),
    [],
  );

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
          setLocalMutationError(`Row ${rowIdx + 1}: ${reason}`);
          return true;
        }
      }
      return false;
    },
    [getRowMutationBlockReason, setLocalMutationError],
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
            Object.hasOwn(rowsRef.current[rowIdx], columnName)
              ? rowsRef.current[rowIdx][columnName]
              : undefined,
          ]),
        ),
        ...(mongoRowIdentity || rowValueTypesRef.current[rowIdx]
          ? {
              originalTypes: Object.fromEntries(
                [...columnMap.keys()].map((columnName) => [
                  columnName,
                  (rowValueTypesRef.current[rowIdx] &&
                  Object.hasOwn(rowValueTypesRef.current[rowIdx], columnName)
                    ? rowValueTypesRef.current[rowIdx][columnName]
                    : undefined) ?? {
                    category: "other",
                    nativeType: "missing",
                  },
                ]),
              ),
            }
          : {}),
      }));
    },
    [pkColsRef, rowsRef, mongoIdTypesRef, rowValueTypesRef, mongoRowIdentity],
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
      previousRowValueTypes?: readonly Record<string, MongoValueTypeHint>[],
      nextRowValueTypes?: readonly Record<string, MongoValueTypeHint>[],
    ) => {
      if (
        reconciliationRef.current?.kind !== "apply" &&
        (reconciliationRef.current?.kind === "delete" ||
          history.hasUnresolvedEdits())
      ) {
        history.remap((snapshot) => {
          const restored = restorePendingEditsSafely(
            buildPendingRestoreState(
              snapshot.pendingEdits,
              rowsRef.current,
              primaryKeyColumns,
              previousMongoIdTypes,
              mongoRowIdentity,
              snapshot.unresolvedPendingEdits,
              previousRowValueTypes,
            ),
            rows,
            primaryKeyColumns,
            nextMongoIdTypes,
            mongoRowIdentity,
            nextRowValueTypes,
          );
          return {
            ...snapshot,
            pendingEdits: restored.pendingEdits,
            unresolvedPendingEdits: restored.unresolved,
            editCell: null,
          };
        });
      } else {
        history.clear();
      }
      const recovery = reconciliationRef.current;
      if (recovery && !recovery.requiresExplicitRead) {
        setReconciliation(
          recovery.unknown ? { ...recovery, verified: true } : null,
        );
      }
      const restoreSource =
        pendingRestoreRef.current ??
        buildPendingRestoreState(
          pendingEditsRef.current,
          rowsRef.current,
          primaryKeyColumns,
          previousMongoIdTypes,
          mongoRowIdentity,
          unresolvedPendingEditsRef.current,
          previousRowValueTypes,
        );
      const restoreResult = restorePendingEditsSafely(
        restoreSource,
        rows,
        primaryKeyColumns,
        nextMongoIdTypes,
        mongoRowIdentity,
        nextRowValueTypes,
      );

      pendingRestoreRef.current = null;
      didHydrateDraftRef.current = true;
      setAwaitingDraftRestore(false);
      unresolvedPendingEditsRef.current = restoreResult.unresolved;
      setPending(restoreResult.pendingEdits);
      setUnresolvedPendingEdits(restoreResult.unresolved);
      setEditCell(null);
    },
    [history, rowsRef, mongoRowIdentity, setReconciliation, setPending],
  );

  const resetForTableInit = useCallback(() => {
    const preserveInitialDraft = initialDraftPendingRef.current;
    initialDraftPendingRef.current = false;
    clearApplyRequestState();
    history.clear();
    if (!preserveInitialDraft) {
      pendingRestoreRef.current = null;
      setAwaitingDraftRestore(false);
    }
    setPending(new Map());
    unresolvedPendingEditsRef.current = [];
    setUnresolvedPendingEdits([]);
    setEditCell(null);
    setApplying(false);
    setDeleting(false);
    setMutationPreview(null);
    setStructuredCellDialog(null);
    if (!preserveInitialDraft) {
      pasteStructureEpochRef.current++;
      setNewRows([]);
    }
    if (!preserveInitialDraft) {
      setMutErr(null);
      setApplyStatus(null);
    }
    activeOperationIdRef.current = null;
    deleteRequestSignaturesRef.current = [];
    if (!preserveInitialDraft) setReconciliation(null);
  }, [
    clearApplyRequestState,
    history,
    setReconciliation,
    pasteStructureEpochRef,
    setPending,
    setNewRows,
  ]);

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
        changesPossible,
        outcomeUnknown,
      }) => {
        if (!operationId || operationId !== activeOperationIdRef.current)
          return;
        activeOperationIdRef.current = null;
        setApplying(false);

        if (success) {
          setReconciliation(null);
          history.clear();
          pasteStructureEpochRef.current++;
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
            unresolvedPendingEditsRef.current,
            rowValueTypesRef.current,
          );
          pendingRestoreRef.current =
            restoreState.entries.length > 0 ? restoreState : null;

          preserveScrollPositionRef.current();
          fetchPageRef.current();
        } else {
          const shouldReconcile =
            Boolean(insertApplied) ||
            changesPossible === true ||
            outcomeUnknown === true;
          const hasDriverRowOutcomes = rowOutcomes?.some(
            ({ status }) =>
              status === "applied" ||
              status === "not_applied" ||
              status === "unknown",
          );
          const failedPending = hasDriverRowOutcomes
            ? getRetainedPendingEdits(
                applyPendingSnapshotRef.current,
                applyRowIndexesRef.current,
                rowOutcomes,
                failedRows,
              )
            : pendingEditsRef.current;
          if (hasDriverRowOutcomes) {
            setPending(failedPending);
          }
          if (shouldReconcile) {
            if (insertApplied) {
              pasteStructureEpochRef.current++;
              setNewRows([]);
            }
            const restoreState = buildPendingRestoreState(
              failedPending,
              rowsRef.current,
              pkColsRef.current,
              mongoIdTypesRef.current,
              mongoRowIdentity,
              unresolvedPendingEditsRef.current,
              rowValueTypesRef.current,
            );
            pendingRestoreRef.current =
              restoreState.entries.length > 0 ? restoreState : null;
            setReconciliation({
              kind: "apply",
              unknown: outcomeUnknown === true,
              verified: false,
            });
            preserveScrollPositionRef.current();
            fetchPageRef.current();
          } else {
            pendingRestoreRef.current = null;
            setReconciliation(null);
          }

          const failureMessage = error ?? "Apply failed";
          const needsUnknownOutcomeWarning =
            outcomeUnknown &&
            !/(?:outcome.{0,24}unknown|may have been applied|refresh and verify)/i.test(
              failureMessage,
            );
          const message = insertApplied
            ? `${failureMessage}. Insert was applied, but update changes were not.`
            : needsUnknownOutcomeWarning
              ? `${failureMessage} The write outcome is unknown. Refresh and verify before retrying.`
              : failureMessage;
          setApplyStatus({
            tone: "error",
            message:
              outcomeUnknown && newRowsRef.current.length > 0 && !insertApplied
                ? `${message} ${UNKNOWN_INSERT_WARNING}`
                : outcomeUnknown
                  ? `${message} The first refresh may precede a late write; refresh again and verify before retrying.`
                  : message,
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
        rowOutcomes,
      }) => {
        if (!operationId || operationId !== activeOperationIdRef.current)
          return;
        activeOperationIdRef.current = null;
        setDeleting(false);
        const deletedSignatures = new Set<string>();
        for (const outcome of rowOutcomes ?? []) {
          const signature =
            deleteRequestSignaturesRef.current[outcome.rowIndex];
          if (outcome.status === "deleted" && signature) {
            deletedSignatures.add(signature);
          }
        }
        deleteRequestSignaturesRef.current = [];
        if (deletedSignatures.size > 0) {
          const retainPending = (pending: PendingEdits): PendingEdits =>
            new Map(
              [...pending].filter(([rowIdx]) => {
                const signature = rowPrimaryKeySignature(
                  rowsRef.current[rowIdx],
                  pkColsRef.current,
                  mongoIdTypesRef.current[rowIdx],
                  mongoRowIdentity,
                );
                return !signature || !deletedSignatures.has(signature);
              }),
            );
          const retainEntries = (entries: PendingRestoreEntry[]) =>
            entries.filter(
              (entry) => !deletedSignatures.has(entry.originalSignature),
            );
          const nextPending = retainPending(pendingEditsRef.current);
          pendingEditsRef.current = nextPending;
          setPending(nextPending);
          const nextUnresolved = retainEntries(
            unresolvedPendingEditsRef.current,
          );
          unresolvedPendingEditsRef.current = nextUnresolved;
          setUnresolvedPendingEdits(nextUnresolved);
          if (pendingRestoreRef.current) {
            pendingRestoreRef.current = {
              entries: retainEntries(pendingRestoreRef.current.entries),
            };
          }
          history.remap((snapshot) => ({
            ...snapshot,
            pendingEdits: retainPending(snapshot.pendingEdits),
            unresolvedPendingEdits: retainEntries(
              snapshot.unresolvedPendingEdits ?? [],
            ),
            editCell: null,
          }));
        }
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
          setReconciliation({
            kind: "delete",
            unknown: outcomeUnknown === true,
            verified: false,
          });
          pendingRestoreRef.current = buildPendingRestoreState(
            pendingEditsRef.current,
            rowsRef.current,
            pkColsRef.current,
            mongoIdTypesRef.current,
            mongoRowIdentity,
            unresolvedPendingEditsRef.current,
            rowValueTypesRef.current,
          );
          preserveScrollPositionRef.current();
          fetchPageRef.current();
        } else {
          setReconciliation(null);
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
    rowValueTypesRef,
    mongoRowIdentity,
    pkColsRef,
    preserveScrollPositionRef,
    rowsRef,
    setReconciliation,
    pasteStructureEpochRef,
    setNewRows,
    setPending,
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
      deleteRequestSignaturesRef.current = [];
    }

    postMessage("cancelMutationPreview", {
      operationId: preview.operationId,
      previewToken,
    });
    activeOperationIdRef.current = null;
    setReconciliation(null);
  }, [clearApplyRequestState, setReconciliation]);

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
    if (unresolvedPendingEditsRef.current.length > 0) return;
    if (isBusy()) return;
    if (applying || deleting) return;
    if (newRowsRef.current.length >= MAX_DRAFT_ROWS) return;
    history.push(
      buildUndoRedoSnapshot(
        pendingEditsRef.current,
        newRowsRef.current,
        null,
        unresolvedPendingEditsRef.current,
      ),
    );
    pasteStructureEpochRef.current++;
    setNewRows((prev) => [createInsertDraft(columnsRef.current), ...prev]);
    setEditCell(null);
    setLocalMutationError(null);
  }, [
    applying,
    columnsRef,
    deleting,
    history,
    isBusy,
    metadataBlockedRef,
    setLocalMutationError,
    pasteStructureEpochRef,
    setNewRows,
  ]);

  const applyChanges = useCallback(() => {
    if (metadataBlockedRef?.current) return;
    if (isBusy()) return;
    if (unresolvedPendingEditsRef.current.length > 0) return;
    const currentPending = pendingEditsRef.current;
    const currentRows = newRowsRef.current;
    const unsavedRowCount = currentPending.size + currentRows.length;
    if (unsavedRowCount === 0 || applying) {
      return;
    }
    if (blockUnsafeRows(currentPending.keys())) return;

    applyPendingSnapshotRef.current = clonePendingEdits(currentPending);
    applyRowIndexesRef.current = [...currentPending.keys()];

    setApplying(true);
    setReconciliation(null);
    setApplyStatus(null);
    setMutErr(null);
    const operationId = `table-mutation:${operationEpochRef.current}:${++operationSequenceRef.current}`;
    activeOperationIdRef.current = operationId;

    const updates = buildPendingUpdatesPayload(currentPending);
    const insertValues = currentRows.map(buildInsertValues);
    const marker: PersistedTableMutation = {
      kind: "apply",
      unknown: true,
      operationId,
    };
    setPersistedMutation(marker);
    persistDraft(marker);
    postMessage("applyChanges", {
      operationId,
      updates,
      ...(insertValues.length > 0 ? { insertValues } : {}),
    });
  }, [
    applying,
    buildPendingUpdatesPayload,
    isBusy,
    metadataBlockedRef,
    blockUnsafeRows,
    setReconciliation,
    persistDraft,
  ]);

  const revertChanges = useCallback(() => {
    if (loadingRef.current || activeOperationIdRef.current !== null) return;
    history.clear();
    pendingRestoreRef.current = null;
    setAwaitingDraftRestore(false);
    setPending(new Map());
    unresolvedPendingEditsRef.current = [];
    setUnresolvedPendingEdits([]);
    pasteStructureEpochRef.current++;
    setNewRows([]);
    setEditCell(null);
    setStructuredCellDialog(null);
    if (reconciliationRef.current?.verified) setReconciliation(null);
    setLocalMutationError(null);
    dismissApplyStatus();
  }, [
    dismissApplyStatus,
    history,
    loadingRef,
    setLocalMutationError,
    setReconciliation,
    pasteStructureEpochRef,
    setPending,
    setNewRows,
  ]);

  const discardUnresolvedPendingEdits = useCallback(() => {
    if (loadingRef.current || activeOperationIdRef.current !== null) return;
    const discardedSignatures = new Set(
      unresolvedPendingEditsRef.current.map((entry) => entry.originalSignature),
    );
    if (discardedSignatures.size === 0) return;
    unresolvedPendingEditsRef.current = [];
    setUnresolvedPendingEdits([]);
    if (pendingRestoreRef.current) {
      pendingRestoreRef.current = {
        entries: pendingRestoreRef.current.entries.filter(
          (entry) => !discardedSignatures.has(entry.originalSignature),
        ),
      };
    }
    history.remap((snapshot) => ({
      ...snapshot,
      unresolvedPendingEdits: (snapshot.unresolvedPendingEdits ?? []).filter(
        (entry) => !discardedSignatures.has(entry.originalSignature),
      ),
      editCell: null,
    }));
  }, [history, loadingRef]);

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

        if (persistedEditMatchesOriginal(newVal, originalVal)) {
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
        buildUndoRedoSnapshot(
          currentPending,
          newRowsRef.current,
          null,
          unresolvedPendingEditsRef.current,
        ),
      );

      setPending((previousPending) => {
        const nextPending = new Map(previousPending);

        for (const { rowIdx, column, newVal, originalVal } of effectiveEdits) {
          const coerced: unknown = newVal === NULL_SENTINEL ? null : newVal;

          if (persistedEditMatchesOriginal(newVal, originalVal)) {
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
    [history, isBusy, validatePersistedBatch, blockUnsafeRows, setPending],
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

      if (isBusy() || !canEditRowsRef.current || !canEditColumn(column)) {
        return;
      }
      if (blockUnsafeRows([rowIdx])) return;

      const coerced: unknown = newVal === NULL_SENTINEL ? null : newVal;
      const currentPending = pendingEditsRef.current;

      if (persistedEditMatchesOriginal(newVal, originalVal)) {
        if (!currentPending.get(rowIdx)?.has(column.name)) {
          return;
        }
      } else {
        if (currentPending.get(rowIdx)?.get(column.name) === coerced) {
          return;
        }
      }

      history.push(
        buildUndoRedoSnapshot(
          currentPending,
          newRowsRef.current,
          null,
          unresolvedPendingEditsRef.current,
        ),
      );

      if (persistedEditMatchesOriginal(newVal, originalVal)) {
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
    [history, isBusy, committedColumnNamesRef, blockUnsafeRows, setPending],
  );

  const commitDraftCellEdit = useCallback(
    (rowIdx: number, column: ColumnMeta, newVal: string) => {
      if (isBusy() || !canEditColumn(column)) return;
      setEditCell(null);

      const currentRows = newRowsRef.current;
      if (rowIdx < 0 || rowIdx >= currentRows.length) return;

      const norm = newVal === NULL_SENTINEL ? NULL_SENTINEL : newVal;

      if (
        Object.hasOwn(currentRows[rowIdx], column.name) &&
        currentRows[rowIdx][column.name]?.value === norm
      )
        return;

      history.push(
        buildUndoRedoSnapshot(
          pendingEditsRef.current,
          currentRows,
          null,
          unresolvedPendingEditsRef.current,
        ),
      );

      setNewRows((prev) =>
        prev.map((draft, i) =>
          i === rowIdx
            ? {
                ...draft,
                [column.name]: {
                  ...(Object.hasOwn(draft, column.name)
                    ? draft[column.name]
                    : undefined),
                  value: norm,
                },
              }
            : draft,
        ),
      );
    },
    [history, isBusy, setNewRows],
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
        const prev = Object.hasOwn(currentRow, column.name)
          ? currentRow[column.name]?.value
          : undefined;
        if (prev === norm) continue;
        normEdits.push({ columnName: column.name, value: norm });
      }

      if (normEdits.length === 0) return;

      history.push(
        buildUndoRedoSnapshot(
          pendingEditsRef.current,
          currentRows,
          null,
          unresolvedPendingEditsRef.current,
        ),
      );

      setNewRows((prev) =>
        prev.map((draft, i) => {
          if (i !== rowIdx) return draft;
          const next = { ...draft };
          for (const e of normEdits) {
            Object.defineProperty(next, e.columnName, {
              value: {
                ...(Object.hasOwn(next, e.columnName)
                  ? next[e.columnName]
                  : undefined),
                value: e.value,
              },
              enumerable: true,
              configurable: true,
              writable: true,
            });
          }
          return next;
        }),
      );
    },
    [history, isBusy, setNewRows],
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
        const prev = Object.hasOwn(currentRows[rowIdx], column.name)
          ? currentRows[rowIdx][column.name]?.value
          : undefined;
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

        if (persistedEditMatchesOriginal(newVal, originalVal)) {
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

      history.push(
        buildUndoRedoSnapshot(
          currentPending,
          currentRows,
          null,
          unresolvedPendingEditsRef.current,
        ),
      );

      if (draftEditsByRow.size > 0) {
        setNewRows((prev) =>
          prev.map((draft, i) => {
            const rowEdits = draftEditsByRow.get(i);
            if (!rowEdits) return draft;
            const next = { ...draft };
            for (const e of rowEdits) {
              Object.defineProperty(next, e.columnName, {
                value: {
                  ...(Object.hasOwn(next, e.columnName)
                    ? next[e.columnName]
                    : undefined),
                  value: e.value,
                },
                enumerable: true,
                configurable: true,
                writable: true,
              });
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

            if (persistedEditMatchesOriginal(newVal, originalVal)) {
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
    [
      history,
      isBusy,
      validatePersistedBatch,
      blockUnsafeRows,
      setPending,
      setNewRows,
    ],
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
      if (!canOpenColumn(column, "persisted")) {
        return;
      }

      setEditCell({ kind: "persisted", rowIdx, col: column.name });
      dismissApplyStatus();
    },
    [isBusy, committedColumnNamesRef, blockUnsafeRows, dismissApplyStatus],
  );

  const handleStartDraftEdit = useCallback(
    (rowIdx: number, column: ColumnMeta) => {
      if (isBusy()) return;
      if (!canOpenColumn(column, "draft")) return;
      setEditCell({ kind: "draft", rowIdx, col: column.name });
      dismissApplyStatus();
    },
    [isBusy, dismissApplyStatus],
  );

  const getRowCellColumn = useCallback(
    (rowIdx: number, column: ColumnMeta): ColumnMeta => {
      const rowTypes = rowValueTypesRef.current[rowIdx];
      const rowType =
        rowTypes && Object.hasOwn(rowTypes, column.name)
          ? rowTypes[column.name]
          : undefined;
      return rowType &&
        !["null", "undefined", "missing"].includes(rowType.nativeType)
        ? {
            ...column,
            category: rowType.category,
            nativeType: rowType.nativeType,
          }
        : column;
    },
    [rowValueTypesRef],
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
      if (!canOpenColumn(column, rowKind)) return;
      const cellColumn =
        rowKind === "persisted" && rowIdx !== undefined
          ? getRowCellColumn(rowIdx, column)
          : column;
      const cellValue =
        cellColumn === column
          ? value
          : (getStructuredCellDialogValue(currentValue, cellColumn) ?? value);
      const rowBlockReason =
        rowKind === "persisted" && rowIdx !== undefined
          ? getRowMutationBlockReason(rowIdx)
          : null;
      const readOnly =
        requestedReadOnly ||
        !canEditColumn(column) ||
        (rowKind === "persisted" &&
          (!canEditRowsRef.current ||
            rowIdx === undefined ||
            Boolean(rowBlockReason) ||
            Boolean(
              committedColumnNamesRef &&
                !committedColumnNamesRef.current.has(column.name),
            )));
      const dataDescription =
        cellValue.kind === "text"
          ? "Text data"
          : `Structured ${cellValue.kind} data`;

      setEditCell(null);
      dismissApplyStatus();
      setStructuredCellDialog({
        rowKind,
        rowIdx: rowIdx ?? null,
        column: cellColumn,
        title: `Cell data: ${column.name}`,
        description: readOnly
          ? `${dataDescription} in ${column.name}. ${rowBlockReason ?? "Apply closes this dialog without sending a mutation."}`
          : rowKind === "draft"
            ? `${dataDescription} in ${column.name}. Apply updates the pending inserted row only.`
            : `${dataDescription} in ${column.name}. Apply updates the local pending cell edit only.`,
        language: cellValue.language,
        initialText: cellValue.formattedText,
        draftText: cellValue.formattedText,
        originalValue,
        nullable: column.nullable,
        readOnly,
        initialIsNull: currentValue === null,
        isNull: currentValue === null,
      });
    },
    [
      isBusy,
      committedColumnNamesRef,
      getRowMutationBlockReason,
      getRowCellColumn,
      dismissApplyStatus,
    ],
  );

  const updateStructuredCellDialogDraft = useCallback((nextValue: string) => {
    setStructuredCellDialog((currentDialog) => {
      if (!currentDialog || currentDialog.readOnly) {
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
    const selectedIndexes = [...selectedRef.current];
    deleteRequestSignaturesRef.current = selectedIndexes.map((index) =>
      rowPrimaryKeySignature(
        rowsRef.current[index],
        pkColsRef.current,
        mongoIdTypesRef.current[index],
        mongoRowIdentity,
      ),
    );
    const toDelete = selectedIndexes.map((index) => {
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
    setReconciliation(null);
    const operationId = `table-mutation:${operationEpochRef.current}:${++operationSequenceRef.current}`;
    activeOperationIdRef.current = operationId;
    const marker: PersistedTableMutation = {
      kind: "delete",
      unknown: true,
      operationId,
    };
    setPersistedMutation(marker);
    persistDraft(marker);
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
    setReconciliation,
    persistDraft,
  ]);

  const undoAction = useCallback(() => {
    if (isBusy()) return;
    if (unresolvedPendingEditsRef.current.length > 0) return;
    if (applying || deleting) return;

    const currentSnapshot = buildUndoRedoSnapshot(
      pendingEditsRef.current,
      newRowsRef.current,
      editCellRef.current,
      unresolvedPendingEditsRef.current,
    );
    const previousSnapshot = history.undo(currentSnapshot);
    if (!previousSnapshot) return;

    const restored = applyUndoRedoSnapshot(previousSnapshot);
    setPending(restored.pendingEdits);
    unresolvedPendingEditsRef.current = restored.unresolvedPendingEdits;
    setUnresolvedPendingEdits(restored.unresolvedPendingEdits);
    pasteStructureEpochRef.current++;
    setNewRows(restored.newRows);
    setEditCell(restored.editCell);
    setStructuredCellDialog(null);
  }, [
    applying,
    deleting,
    history,
    isBusy,
    pasteStructureEpochRef,
    setPending,
    setNewRows,
  ]);

  const redoAction = useCallback(() => {
    if (isBusy()) return;
    if (unresolvedPendingEditsRef.current.length > 0) return;
    if (applying || deleting) return;

    const currentSnapshot = buildUndoRedoSnapshot(
      pendingEditsRef.current,
      newRowsRef.current,
      editCellRef.current,
      unresolvedPendingEditsRef.current,
    );
    const nextSnapshot = history.redo(currentSnapshot);
    if (!nextSnapshot) return;

    const restored = applyUndoRedoSnapshot(nextSnapshot);
    setPending(restored.pendingEdits);
    unresolvedPendingEditsRef.current = restored.unresolvedPendingEdits;
    setUnresolvedPendingEdits(restored.unresolvedPendingEdits);
    pasteStructureEpochRef.current++;
    setNewRows(restored.newRows);
    setEditCell(restored.editCell);
    setStructuredCellDialog(null);
  }, [
    applying,
    deleting,
    history,
    isBusy,
    pasteStructureEpochRef,
    setPending,
    setNewRows,
  ]);

  useEffect(() => {
    if (!canEditRows && !canInsertRows) return;

    const handleKeyDown = (event: KeyboardEvent) => {
      if (
        structuredCellDialogRef.current ||
        mutationPreviewRef.current ||
        document.querySelector('[aria-modal="true"]')
      )
        return;
      const tag = (event.target as HTMLElement)?.tagName;
      if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;
      if ((event.target as HTMLElement)?.closest?.(".monaco-editor")) return;

      const isMac = navigator.platform.toUpperCase().includes("MAC");
      const mod = isMac ? event.metaKey : event.ctrlKey;
      if (!mod || event.altKey) return;

      const key = event.key.toLowerCase();
      // Non-Latin layouts (e.g. Cyrillic) report a localized key; fall back to the physical key.
      const letter = /^[a-z]$/.test(key)
        ? key
        : /^\P{ASCII}$/u.test(event.key) && event.code.startsWith("Key")
          ? event.code.slice(3).toLowerCase()
          : "";

      if (letter === "z" && !event.shiftKey) {
        event.preventDefault();
        undoAction();
      } else if (letter === "z" && event.shiftKey) {
        event.preventDefault();
        redoAction();
      } else if (letter === "y" && !event.shiftKey) {
        event.preventDefault();
        redoAction();
      }
    };

    window.addEventListener("keydown", handleKeyDown, true);
    return () => window.removeEventListener("keydown", handleKeyDown, true);
  }, [canEditRows, canInsertRows, undoAction, redoAction]);

  return {
    getRowCellColumn,
    getRowMutationBlockReason,
    isFilterNavigationBlocked: () =>
      activeOperationIdRef.current !== null ||
      editCellRef.current !== null ||
      structuredCellDialog !== null ||
      pendingEditsRef.current.size > 0 ||
      unresolvedPendingEditsRef.current.length > 0 ||
      newRowsRef.current.length > 0 ||
      (reconciliationRef.current !== null &&
        !reconciliationRef.current.verified),
    getMetadataRefreshState: () => ({
      reconciliationPending:
        reconciliationRef.current !== null &&
        !reconciliationRef.current.verified,
      busy:
        activeOperationIdRef.current !== null ||
        editCellRef.current !== null ||
        structuredCellDialog !== null,
      hasWork:
        pendingEditsRef.current.size > 0 ||
        unresolvedPendingEditsRef.current.length > 0 ||
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
    reconciliationPending:
      reconciliation !== null && !reconciliation.verified && loadingRef.current,
    reconciliationRequired:
      (reconciliation !== null && !reconciliation.verified) ||
      hasUnknownInsertDrafts(reconciliation, newRows),
    dismissApplyStatus,
    dismissMutationError: () => setLocalMutationError(null),
    editCell,
    structuredCellDialog,
    handleRowsCommitted,
    retryReconciliation: () => {
      const recovery = reconciliationRef.current;
      if (!recovery || (recovery.verified && !recovery.unknown)) return false;
      if (loadingRef.current || activeOperationIdRef.current !== null)
        return true;
      if (recovery.verified) {
        pendingRestoreRef.current = buildPendingRestoreState(
          pendingEditsRef.current,
          rowsRef.current,
          pkColsRef.current,
          mongoIdTypesRef.current,
          mongoRowIdentity,
          unresolvedPendingEditsRef.current,
          rowValueTypesRef.current,
        );
      }
      setReconciliation({
        ...recovery,
        verified: false,
        requiresExplicitRead: false,
      });
      preserveScrollPositionRef.current();
      fetchPageRef.current();
      return true;
    },
    handleReadFailed: () => {
      const recovery = reconciliationRef.current;
      if (!recovery || recovery.verified) {
        if (didHydrateDraftRef.current) pendingRestoreRef.current = null;
        return;
      }
      const failedReadMessage =
        recovery.kind === "delete"
          ? "The delete refresh failed."
          : "The verification refresh failed.";
      setMutErr((current) =>
        `${current ?? ""} ${failedReadMessage} Refresh and verify the data before making further changes.`.trim(),
      );
    },
    handleStartDraftEdit,
    handleStartEdit,
    mutErr,
    mutationPreview,
    newRows,
    pendingEdits,
    unresolvedPendingEdits,
    openStructuredCellDialog,
    resetForTableInit,
    revertChanges,
    discardUnresolvedPendingEdits,
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
    blockNavigationWithUnsavedChanges: () => {
      if (
        reconciliationRef.current?.kind === "apply" &&
        reconciliationRef.current.unknown
      )
        return;
      setApplyStatus({
        tone: "warning",
        message:
          "Apply or revert pending changes before loading different rows.",
      });
    },
  };
}
