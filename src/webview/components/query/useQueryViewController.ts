import { useCallback, useEffect, useRef, useState } from "react";
import type { QueryDatabaseFields } from "../../../shared/queryDatabaseTarget";
import {
  isRecord,
  parseBookmarkSavedPayload,
  parseQueryRunIdentity,
  parseQueryRunOutcome,
  parseQueryTargetBinding,
  parseQueryTargetChangeResult,
  type QueryBookmarkPayload,
  type QueryEditorLanguage,
  type QueryEditorPresentation,
  type QueryRunIdentity,
  type QuerySchemaPayload,
  type QueryTargetBinding,
  sameQueryRun,
} from "../../../shared/webviewContracts";
import {
  type ConnectionEntry,
  hasQueryResultDetails,
  type QueryResult,
  type SchemaObject,
  useConnectionStore,
  useQueryStore,
  useSchemaStore,
} from "../../store";
import { onMessage, postMessage } from "../../utils/messaging";
import { isArrayOf, isString } from "../../utils/replyShape";
import { readWebviewState, updateWebviewState } from "../../utils/vscodeState";
import type { MonacoEditorHandle } from "../MonacoEditor";
import {
  DEFAULT_EDITOR_H,
  DEFAULT_EDITOR_RATIO,
  DIVIDER_H,
  MIN_EDITOR_H,
  resolveQueryEditorState,
  TOOLBAR_H,
} from "./queryViewHelpers";
import { useQueryRunRecovery } from "./useQueryRunRecovery";

const BLANK_QUERY_VALIDATION_MESSAGE =
  "Select or enter a query before running.";
const MULTIPLE_SELECTIONS_MESSAGE =
  "Multiple selections cannot be run together. Select a single query to run.";
const INVALID_QUERY_RESULT =
  "The host returned an invalid query result. The query may have executed; verify before retrying.";

function hasResultRows(result: Record<string, unknown>): boolean {
  return (
    isArrayOf(result.columns, isString) &&
    isArrayOf(result.rows, isRecord) &&
    (result.columnMeta === undefined ||
      isArrayOf(result.columnMeta, isRecord)) &&
    Number.isFinite(result.rowCount)
  );
}

function isQueryResult(
  payload: Record<string, unknown>,
): payload is Record<string, unknown> & QueryResult {
  return (
    hasResultRows(payload) &&
    Number.isFinite(payload.executionTimeMs) &&
    (payload.rowCountKind === undefined ||
      payload.rowCountKind === "returned") &&
    (payload.affectedRows === undefined ||
      (typeof payload.affectedRows === "number" &&
        Number.isInteger(payload.affectedRows) &&
        payload.affectedRows >= 0)) &&
    (payload.affectedRowsUnknown === undefined ||
      typeof payload.affectedRowsUnknown === "boolean") &&
    (payload.affectedRowsKind === undefined ||
      payload.affectedRowsKind === "processed-writes") &&
    (payload.responseMetadata === undefined ||
      isArrayOf(payload.responseMetadata, isRecord)) &&
    (payload.warnings === undefined || isArrayOf(payload.warnings, isString)) &&
    (payload.error === undefined ||
      (typeof payload.error === "string" && payload.error.trim() !== "")) &&
    (payload.resultSets === undefined ||
      (isArrayOf(payload.resultSets, isRecord) &&
        payload.resultSets.every(
          (set) => typeof set.id === "string" && hasResultRows(set),
        ))) &&
    (payload.missingCells === undefined ||
      (isArrayOf(payload.missingCells, isRecord) &&
        payload.missingCells.every(
          (cell) =>
            Number.isSafeInteger(cell.rowIndex) &&
            Number.isSafeInteger(cell.columnIndex),
        )))
  );
}

interface QueryViewControllerParams extends QueryDatabaseFields {
  panelId: string;
  connectionId: string;
  pinned?: boolean;
  initialTargetBinding?: QueryTargetBinding;
  queryRunStatus?: boolean;
  editorLanguage?: QueryEditorLanguage;
  editorPresentation?: QueryEditorPresentation;
  formatOnOpen?: boolean;
  initialIsBookmarked: boolean;
  initialQueryText: string;
}

function readQueryDraft(
  panelId: string,
  initialQueryText: string,
): {
  text: string;
  restored: boolean;
  activeConnectionId?: string;
} {
  const state = readWebviewState<Record<string, unknown>>({});
  const draft = state.queryDraft;
  if (
    draft !== null &&
    typeof draft === "object" &&
    (draft as { panelId?: unknown }).panelId === panelId &&
    typeof (draft as { text?: unknown }).text === "string"
  ) {
    const activeConnectionId = (draft as { activeConnectionId?: unknown })
      .activeConnectionId;
    return {
      text: (draft as { text: string }).text,
      restored: true,
      ...(typeof activeConnectionId === "string" ? { activeConnectionId } : {}),
    };
  }
  return { text: initialQueryText, restored: false };
}

export function useQueryViewController({
  panelId,
  connectionId,
  pinned = false,
  postgresDatabase,
  mssqlDatabase,
  initialTargetBinding,
  queryRunStatus = false,
  editorLanguage,
  editorPresentation,
  formatOnOpen,
  initialIsBookmarked,
  initialQueryText,
}: QueryViewControllerParams) {
  const [queryDraft] = useState(() =>
    readQueryDraft(panelId, initialQueryText),
  );
  const editorInitialValue = queryDraft.text;
  const initialActiveConnectionId =
    initialTargetBinding?.connectionId ??
    (pinned ? connectionId : (queryDraft.activeConnectionId ?? connectionId));
  const draftInitiallyBookmarked =
    initialIsBookmarked &&
    editorInitialValue.trim() === initialQueryText.trim() &&
    initialActiveConnectionId === connectionId;
  const editorRef = useRef<MonacoEditorHandle>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const [targetBinding, setTargetBinding] = useState(initialTargetBinding);
  const targetBindingRef = useRef(targetBinding);
  // Shortcuts and button handlers must see a selection before React renders it.
  const pendingTargetChangeRef = useRef<{
    requestId: string;
    connectionId: string;
    sessionKey?: string | null;
  } | null>(null);
  const requestedTargetChangeRef = useRef(false);
  const [pendingTargetId, setPendingTargetId] = useState<string | null>(null);
  const [targetChangeError, setTargetChangeError] = useState(false);

  const { status, result, setRunning, setResult, setError, reset } =
    useQueryStore();
  const {
    marker,
    markerRef,
    notice: runNotice,
    prepareRun,
    settle,
    markUnknown,
    acknowledgeUnknown,
  } = useQueryRunRecovery(panelId);
  const {
    connections,
    activeConnectionId,
    setConnections,
    setActiveConnection,
  } = useConnectionStore();
  const { schemaByConnection, setSchema } = useSchemaStore();

  const schemaFetchedRef = useRef<Set<string>>(new Set());
  const schemaErrorsRef = useRef<Record<string, string>>({});
  const bookmarkedRef = useRef(draftInitiallyBookmarked);
  const bookmarkTextRef = useRef(editorInitialValue.trim());
  const bookmarkConnectionRef = useRef(connectionId);
  const bookmarkSequenceRef = useRef(0);
  const pendingBookmarkRef = useRef<{
    requestId: string;
    queryText: string;
    connectionId: string;
  } | null>(null);
  const dragStartY = useRef(0);
  const dragStartH = useRef(DEFAULT_EDITOR_H);
  const didAutoFormat = useRef(queryDraft.restored);
  const didPlaceCursor = useRef(false);
  const blankQueryValidationRef = useRef(false);

  const [editorHeight, setEditorHeight] = useState(DEFAULT_EDITOR_H);
  const [isResizing, setIsResizing] = useState(false);
  const [connectionsLoaded, setConnectionsLoaded] = useState(false);
  const [bookmarked, setBookmarked] = useState(draftInitiallyBookmarked);
  const [bookmarking, setBookmarking] = useState(false);
  const [bookmarkError, setBookmarkError] = useState<string | null>(null);
  const [schemaErrors, setSchemaErrors] = useState<Record<string, string>>({});

  const invalidateBookmark = useCallback(() => {
    pendingBookmarkRef.current = null;
    bookmarkedRef.current = false;
    setBookmarked(false);
    setBookmarking(false);
    setBookmarkError(null);
  }, []);

  const clearBlankQueryValidation = useCallback((queryText: string) => {
    if (!queryText.trim() || !blankQueryValidationRef.current) {
      return;
    }

    const queryState = useQueryStore.getState();
    if (
      queryState.status !== "error" ||
      queryState.result?.error !== BLANK_QUERY_VALIDATION_MESSAGE
    ) {
      blankQueryValidationRef.current = false;
      return;
    }

    blankQueryValidationRef.current = false;
    queryState.reset();
  }, []);

  useEffect(() => {
    const element = containerRef.current;
    if (!element) {
      return;
    }

    const usableHeight = element.clientHeight - TOOLBAR_H - DIVIDER_H;
    if (usableHeight > MIN_EDITOR_H * 2) {
      const nextHeight = Math.round(usableHeight * DEFAULT_EDITOR_RATIO);
      setEditorHeight(nextHeight);
      dragStartH.current = nextHeight;
    }
  }, []);

  const resolvedConnectionId = pinned
    ? connectionId
    : activeConnectionId ||
      (connectionsLoaded ? "" : initialActiveConnectionId);
  useEffect(() => {
    if (bookmarkConnectionRef.current !== resolvedConnectionId) {
      bookmarkConnectionRef.current = resolvedConnectionId;
      invalidateBookmark();
    }
  }, [invalidateBookmark, resolvedConnectionId]);
  const activeConnection = connections.find(
    (connection) => connection.id === resolvedConnectionId,
  );
  const pinnedTargetUnavailable =
    pinned && connectionsLoaded && !activeConnection;
  const currentTargetAvailable = !!activeConnection?.targetSessionKey;
  const targetReviewRequired =
    !!targetBinding &&
    (!targetBinding.sessionKey ||
      (connectionsLoaded &&
        activeConnection?.targetSessionKey !== targetBinding.sessionKey));
  const schema: SchemaObject[] = targetReviewRequired
    ? []
    : (schemaByConnection[resolvedConnectionId] ?? []);
  const editorState = resolveQueryEditorState({
    activeConnection,
    editorLanguage,
    editorPresentation,
    formatOnOpen,
    initialConnectionId: connectionId,
    resolvedConnectionId,
  });

  useEffect(() => {
    setActiveConnection(initialActiveConnectionId);
    postMessage("getConnections");
  }, [initialActiveConnectionId, setActiveConnection]);

  const matchesCurrentRunTarget = useCallback(
    (run: QueryRunIdentity) => {
      const binding = targetBindingRef.current;
      return (
        run.postgresDatabase === postgresDatabase &&
        run.mssqlDatabase === mssqlDatabase &&
        run.connectionId ===
          (binding?.connectionId ??
            (pinned
              ? connectionId
              : useConnectionStore.getState().activeConnectionId)) &&
        run.targetSessionKey === (binding?.sessionKey ?? null)
      );
    },
    [connectionId, mssqlDatabase, pinned, postgresDatabase],
  );

  useEffect(() => {
    const current = markerRef.current;
    if (current?.phase !== "pending") return;
    // HTML can still name the old binding on a document-only reload. Wait
    // for the host snapshot rather than discarding a run from a newer binding.
    if (
      current.run.postgresDatabase !== postgresDatabase ||
      current.run.mssqlDatabase !== mssqlDatabase ||
      (pinned && current.run.connectionId !== connectionId)
    ) {
      markUnknown();
      return;
    }
    setRunning({
      operationId: current.run.operationId,
      connectionId: current.run.connectionId,
    });
  }, [
    connectionId,
    markUnknown,
    markerRef,
    mssqlDatabase,
    pinned,
    postgresDatabase,
    setRunning,
  ]);

  useEffect(() => {
    if (!resolvedConnectionId) {
      return;
    }

    if (schemaFetchedRef.current.has(resolvedConnectionId)) {
      return;
    }

    const cachedSchema =
      useSchemaStore.getState().schemaByConnection[resolvedConnectionId];
    if (
      cachedSchema !== undefined &&
      schemaErrorsRef.current[resolvedConnectionId] === undefined
    ) {
      schemaFetchedRef.current.add(resolvedConnectionId);
      return;
    }

    schemaFetchedRef.current.add(resolvedConnectionId);
    postMessage("getSchema", { connectionId: resolvedConnectionId });
  }, [resolvedConnectionId]);

  const handleConnectionChange = useCallback(
    (nextConnectionId: string) => {
      if (targetBindingRef.current) {
        if (!nextConnectionId) return;
        const sessionKey = useConnectionStore
          .getState()
          .connections.find(
            ({ id }) => id === nextConnectionId,
          )?.targetSessionKey;
        const requestId = crypto.randomUUID();
        requestedTargetChangeRef.current = true;
        pendingTargetChangeRef.current = {
          requestId,
          connectionId: nextConnectionId,
          sessionKey,
        };
        setPendingTargetId(nextConnectionId);
        setTargetChangeError(false);
        postMessage("activeConnectionChanged", {
          connectionId: nextConnectionId,
          requestId,
          ...(sessionKey ? { targetSessionKey: sessionKey } : {}),
        });
        return;
      }
      if (pinned) return;
      if (markerRef.current?.run.connectionId !== nextConnectionId)
        markUnknown();
      updateWebviewState((state) => ({
        ...state,
        queryDraft: {
          panelId,
          text: editorRef.current?.getValue() ?? editorInitialValue,
          activeConnectionId: nextConnectionId,
        },
      }));
      if (nextConnectionId !== bookmarkConnectionRef.current) {
        bookmarkConnectionRef.current = nextConnectionId;
        invalidateBookmark();
      }
      blankQueryValidationRef.current = false;
      reset();
      setActiveConnection(nextConnectionId);
      if (!nextConnectionId) return;
      postMessage("activeConnectionChanged", {
        connectionId: nextConnectionId,
      });

      const cachedSchema =
        useSchemaStore.getState().schemaByConnection[nextConnectionId];
      if (
        cachedSchema === undefined ||
        schemaErrorsRef.current[nextConnectionId] !== undefined
      ) {
        schemaFetchedRef.current.delete(nextConnectionId);
      }
    },
    [
      editorInitialValue,
      invalidateBookmark,
      markerRef,
      markUnknown,
      panelId,
      pinned,
      reset,
      setActiveConnection,
    ],
  );

  useEffect(() => {
    const applyTargetBinding = (binding: QueryTargetBinding) => {
      const previousBinding = targetBindingRef.current;
      const checkRunTarget = () => {
        const current = markerRef.current;
        if (
          current?.phase === "pending" &&
          !matchesCurrentRunTarget(current.run)
        ) {
          markUnknown();
        }
      };
      if (
        !previousBinding ||
        (previousBinding.connectionId === binding.connectionId &&
          previousBinding.sessionKey === binding.sessionKey)
      ) {
        checkRunTarget();
        return false;
      }
      targetBindingRef.current = binding;
      setTargetBinding(binding);
      checkRunTarget();
      didAutoFormat.current = true;
      updateWebviewState((state) => ({
        ...state,
        queryDraft: {
          panelId,
          text: editorRef.current?.getValue() ?? editorInitialValue,
          activeConnectionId: binding.connectionId,
          targetBinding: binding,
        },
      }));
      bookmarkConnectionRef.current = binding.connectionId;
      invalidateBookmark();
      blankQueryValidationRef.current = false;
      if (markerRef.current?.phase !== "pending") reset();
      setActiveConnection(binding.connectionId);
      schemaFetchedRef.current.delete(binding.connectionId);
      setSchema(binding.connectionId, []);
      return true;
    };
    const unsubscribeTarget = onMessage<unknown>(
      "queryTargetBinding",
      (payload) => {
        // Unsolicited snapshots also reach newly loaded documents. Once this
        // document selects a target, only its matching reply can update it.
        if (requestedTargetChangeRef.current) return;
        const binding = parseQueryTargetBinding(payload);
        if (binding) applyTargetBinding(binding);
      },
    );
    const unsubscribeTargetChange = onMessage<unknown>(
      "queryTargetChangeResult",
      (input) => {
        const reply = parseQueryTargetChangeResult(input);
        const pending = pendingTargetChangeRef.current;
        if (
          !reply ||
          !pending ||
          reply.requestId !== pending.requestId ||
          (reply.accepted &&
            (reply.binding.connectionId !== pending.connectionId ||
              reply.binding.sessionKey !== pending.sessionKey))
        ) {
          return;
        }
        const bindingChanged = applyTargetBinding(reply.binding);
        if (!reply.accepted && bindingChanged) {
          // An earlier choice may have committed in the host before its reply
          // was superseded here. Refresh the schema for that effective target.
          postMessage("getSchema", {
            connectionId: reply.binding.connectionId,
          });
        }
        pendingTargetChangeRef.current = null;
        setPendingTargetId(null);
        setTargetChangeError(!reply.accepted);
      },
    );
    const unsubscribeResult = onMessage<unknown>(
      "queryResult",
      // Check attribution before treating a malformed reply as terminal.
      (payload) => {
        const { activeOperation, status } = useQueryStore.getState();
        const current = markerRef.current;
        const operation =
          current?.run ??
          (status === "running" && !queryRunStatus ? activeOperation : null);
        if (
          !operation ||
          current?.phase === "unknown" ||
          !isRecord(payload) ||
          payload.operationId !== operation.operationId ||
          payload.connectionId !== operation.connectionId
        ) {
          return;
        }
        if (current && !matchesCurrentRunTarget(current.run)) return;
        const receivedRun = parseQueryRunIdentity(payload.run);
        const outcome = parseQueryRunOutcome(payload.runOutcome);
        if (
          queryRunStatus &&
          current &&
          (!receivedRun || !sameQueryRun(receivedRun, current.run) || !outcome)
        )
          return;
        blankQueryValidationRef.current = false;
        try {
          if (!isQueryResult(payload)) throw new Error(INVALID_QUERY_RESULT);
          if (payload.error && !hasQueryResultDetails(payload))
            setError(payload.error);
          else setResult(payload);
        } catch {
          // With status support, a separate probe can still recover the summary.
          if (queryRunStatus && current) return;
          setError(INVALID_QUERY_RESULT);
          markUnknown();
          return;
        }
        if (current) settle(outcome ?? "completed");
      },
    );

    const unsubscribeConnections = onMessage<ConnectionEntry[]>(
      "connections",
      (payload) => {
        const binding = targetBindingRef.current;
        const current = markerRef.current;
        if (
          current?.phase === "pending" &&
          !payload.some(
            ({ id, targetSessionKey }) =>
              id === current.run.connectionId &&
              (current.run.targetSessionKey === null ||
                targetSessionKey === current.run.targetSessionKey),
          )
        )
          markUnknown();
        if (
          binding &&
          (!binding.sessionKey ||
            payload.find(({ id }) => id === binding.connectionId)
              ?.targetSessionKey !== binding.sessionKey)
        ) {
          invalidateBookmark();
          const queryState = useQueryStore.getState();
          if (!current && queryState.status === "running") {
            setError(
              "The target changed during the query. If the query reached the previous target, it may have executed there.",
            );
          } else if (queryState.status === "success") {
            reset();
          }
        }
        const currentConnectionId =
          useConnectionStore.getState().activeConnectionId;
        if (
          !pinned &&
          !binding &&
          currentConnectionId &&
          !payload.some((connection) => connection.id === currentConnectionId)
        ) {
          invalidateBookmark();
          handleConnectionChange("");
        }
        setConnectionsLoaded(true);
        setConnections(payload);
      },
    );

    const unsubscribeSchema = onMessage<QuerySchemaPayload<SchemaObject>>(
      "schema",
      (payload) => {
        const { [payload.connectionId]: _previous, ...otherErrors } =
          schemaErrorsRef.current;
        if (payload.error) {
          schemaFetchedRef.current.delete(payload.connectionId);
          schemaErrorsRef.current = {
            ...otherErrors,
            [payload.connectionId]: payload.error,
          };
        } else {
          schemaErrorsRef.current = otherErrors;
        }
        setSchemaErrors(schemaErrorsRef.current);
        setSchema(payload.connectionId, payload.schema);
      },
    );

    const unsubscribeBookmark = onMessage<unknown>("bookmarkSaved", (input) => {
      const payload = parseBookmarkSavedPayload(input);
      const pending = pendingBookmarkRef.current;
      if (
        !payload ||
        !pending ||
        payload.requestId !== pending.requestId ||
        pending.queryText !== bookmarkTextRef.current ||
        pending.connectionId !==
          useConnectionStore.getState().activeConnectionId
      ) {
        return;
      }
      pendingBookmarkRef.current = null;
      setBookmarking(false);
      if (payload.ok) {
        bookmarkedRef.current = true;
        setBookmarked(true);
        setBookmarkError(null);
      } else {
        const detail = payload.error?.trim();
        setBookmarkError(
          detail
            ? `Failed to save bookmark: ${detail}`
            : "Failed to save bookmark.",
        );
      }
    });

    return () => {
      unsubscribeTarget();
      unsubscribeTargetChange();
      unsubscribeResult();
      unsubscribeConnections();
      unsubscribeSchema();
      unsubscribeBookmark();
    };
  }, [
    editorInitialValue,
    handleConnectionChange,
    invalidateBookmark,
    markerRef,
    markUnknown,
    matchesCurrentRunTarget,
    panelId,
    pinned,
    queryRunStatus,
    reset,
    setActiveConnection,
    setConnections,
    setError,
    setResult,
    setSchema,
    settle,
  ]);

  useEffect(() => {
    if (
      !editorState.shouldFormatOnOpen ||
      !editorInitialValue ||
      didAutoFormat.current ||
      !editorState.canFormat ||
      targetReviewRequired ||
      (editorState.monacoLanguage === "sql" && !editorState.sqlDialect)
    ) {
      return;
    }

    if (connections.length === 0) {
      return;
    }

    didAutoFormat.current = true;
    requestAnimationFrame(() => {
      editorRef.current?.format(editorState.sqlDialect);

      requestAnimationFrame(() => {
        editorRef.current?.placeCursor();
      });
    });
  }, [
    connections.length,
    editorState.canFormat,
    editorState.monacoLanguage,
    editorState.shouldFormatOnOpen,
    editorState.sqlDialect,
    editorInitialValue,
    targetReviewRequired,
  ]);

  useEffect(() => {
    if (editorState.shouldFormatOnOpen || didPlaceCursor.current) {
      return;
    }

    didPlaceCursor.current = true;
    requestAnimationFrame(() => {
      editorRef.current?.placeCursor();
    });
  }, [editorState.shouldFormatOnOpen]);

  const canUseTarget = useCallback(
    (binding: QueryTargetBinding | undefined) => {
      if (pendingTargetChangeRef.current) return false;
      if (!binding) return !pinnedTargetUnavailable;
      return (
        connectionsLoaded &&
        !!binding.sessionKey &&
        useConnectionStore
          .getState()
          .connections.find(({ id }) => id === binding.connectionId)
          ?.targetSessionKey === binding.sessionKey
      );
    },
    [connectionsLoaded, pinnedTargetUnavailable],
  );

  const executeQuery = useCallback(() => {
    const binding = targetBindingRef.current;
    if (
      markerRef.current ||
      !canUseTarget(binding) ||
      useQueryStore.getState().status === "running"
    )
      return;
    const queryConnectionId =
      binding?.connectionId ??
      (pinned
        ? connectionId
        : useConnectionStore.getState().activeConnectionId);
    if (!queryConnectionId) return;
    const selectedText = editorRef.current?.getSelectionOrValue();
    if (selectedText === null) {
      blankQueryValidationRef.current = false;
      setError(MULTIPLE_SELECTIONS_MESSAGE);
      return;
    }
    const queryText = selectedText?.trim() ?? "";
    if (!queryText) {
      blankQueryValidationRef.current = true;
      setError(BLANK_QUERY_VALIDATION_MESSAGE);
      return;
    }

    blankQueryValidationRef.current = false;
    // The host may outlive this document and still own an earlier execution.
    const operation = {
      operationId: crypto.randomUUID(),
      connectionId: queryConnectionId,
    };
    const run: QueryRunIdentity = {
      ...operation,
      panelId,
      targetSessionKey: binding?.sessionKey ?? null,
      ...(postgresDatabase === undefined ? {} : { postgresDatabase }),
      ...(mssqlDatabase === undefined ? {} : { mssqlDatabase }),
    };
    if (!prepareRun(run)) return;
    setRunning(operation);
    try {
      postMessage("executeQuery", {
        ...operation,
        queryText,
        ...(binding?.sessionKey
          ? { targetSessionKey: binding.sessionKey }
          : {}),
        ...(queryRunStatus ? { run } : {}),
      });
    } catch {
      markUnknown();
    }
  }, [
    connectionId,
    canUseTarget,
    markerRef,
    markUnknown,
    mssqlDatabase,
    panelId,
    pinned,
    postgresDatabase,
    prepareRun,
    queryRunStatus,
    setError,
    setRunning,
  ]);

  const cancelQuery = useCallback(() => {
    const current = markerRef.current;
    const { activeOperation, status } = useQueryStore.getState();
    const operation =
      current?.run ??
      (status === "running" && !queryRunStatus ? activeOperation : null);
    if (
      !operation ||
      (current &&
        (current.phase !== "pending" || !matchesCurrentRunTarget(current.run)))
    )
      return;
    const { connectionId, operationId } = operation;
    try {
      postMessage("cancelQuery", {
        connectionId,
        operationId,
        ...(queryRunStatus && current ? { run: current.run } : {}),
      });
    } catch {
      // Do not report cancellation without a terminal response; status polling continues.
    }
  }, [markerRef, matchesCurrentRunTarget, queryRunStatus]);

  const clearQuery = useCallback(() => {
    editorRef.current?.clearValue();
  }, []);

  const formatQuery = useCallback(() => {
    if (!editorState.canFormat) {
      return;
    }

    const error = editorRef.current?.format(editorState.sqlDialect) ?? null;
    if (error) {
      blankQueryValidationRef.current = false;
      setError(`${editorState.formatErrorPrefix}: ${error}`);
    }
  }, [
    editorState.canFormat,
    editorState.formatErrorPrefix,
    editorState.sqlDialect,
    setError,
  ]);

  const handleBookmark = useCallback(() => {
    const binding = targetBindingRef.current;
    if (
      !canUseTarget(binding) ||
      bookmarkedRef.current ||
      pendingBookmarkRef.current
    ) {
      return;
    }

    const queryText = bookmarkTextRef.current;
    const bookmarkConnectionId =
      binding?.connectionId ??
      (pinned
        ? connectionId
        : useConnectionStore.getState().activeConnectionId);
    if (!queryText || !bookmarkConnectionId) {
      return;
    }

    const pending = {
      requestId: `${panelId}:bookmark:${++bookmarkSequenceRef.current}`,
      queryText,
      connectionId: bookmarkConnectionId,
      ...(binding?.sessionKey ? { targetSessionKey: binding.sessionKey } : {}),
    };
    pendingBookmarkRef.current = pending;
    setBookmarkError(null);
    setBookmarking(true);
    postMessage("addBookmark", pending satisfies QueryBookmarkPayload);
  }, [connectionId, canUseTarget, panelId, pinned]);

  const handleEditorChange = useCallback(
    (value: string) => {
      didAutoFormat.current = true;
      updateWebviewState((state) => ({
        ...state,
        queryDraft: {
          panelId,
          text: value,
          activeConnectionId: resolvedConnectionId,
          ...(targetBindingRef.current
            ? { targetBinding: targetBindingRef.current }
            : {}),
        },
      }));
      bookmarkTextRef.current = value.trim();
      invalidateBookmark();
      clearBlankQueryValidation(
        editorRef.current?.getSelectionOrValue() ?? value,
      );
    },
    [
      clearBlankQueryValidation,
      invalidateBookmark,
      panelId,
      resolvedConnectionId,
    ],
  );

  const handleEditorSelectionChange = useCallback(() => {
    clearBlankQueryValidation(editorRef.current?.getSelectionOrValue() ?? "");
  }, [clearBlankQueryValidation]);

  const startResizing = useCallback(
    (event: React.MouseEvent<HTMLButtonElement>) => {
      event.preventDefault();
      dragStartY.current = event.clientY;
      dragStartH.current = editorHeight;
      setIsResizing(true);
    },
    [editorHeight],
  );

  useEffect(() => {
    if (!isResizing) {
      return;
    }

    const handleMouseMove = (event: MouseEvent) => {
      const delta = event.clientY - dragStartY.current;
      const nextHeight = Math.max(MIN_EDITOR_H, dragStartH.current + delta);
      const maxHeight =
        (containerRef.current?.clientHeight ?? 600) -
        TOOLBAR_H -
        DIVIDER_H -
        40;

      setEditorHeight(Math.min(nextHeight, maxHeight));
    };
    const handleMouseUp = () => {
      setIsResizing(false);
    };

    window.addEventListener("mousemove", handleMouseMove);
    window.addEventListener("mouseup", handleMouseUp);

    return () => {
      window.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("mouseup", handleMouseUp);
    };
  }, [isResizing]);

  return {
    activeConnectionId: resolvedConnectionId,
    selectedConnectionId: pendingTargetId ?? resolvedConnectionId,
    targetChangePending: pendingTargetId !== null,
    targetChangeError,
    pinnedTargetUnavailable,
    targetReviewRequired,
    targetChecking: !!targetBinding && !connectionsLoaded,
    currentTargetAvailable,
    bookmarked,
    bookmarking,
    bookmarkError,
    connections,
    containerRef,
    editorHeight,
    editorRef,
    editorState,
    executeQuery,
    formatQuery,
    handleBookmark,
    handleConnectionChange,
    handleEditorChange,
    handleEditorSelectionChange,
    isResizing,
    result,
    schema,
    schemaError: schemaErrors[resolvedConnectionId] ?? null,
    schemaLoading:
      !!resolvedConnectionId &&
      connections.length > 0 &&
      schemaByConnection[resolvedConnectionId] === undefined,
    startResizing,
    status: marker?.phase === "pending" ? ("running" as const) : status,
    unknownRun: marker?.phase === "unknown" ? marker.run : null,
    runNotice,
    acknowledgeUnknown,
    clearQuery,
    cancelQuery,
    editorInitialValue,
  };
}
