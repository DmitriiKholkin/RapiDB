import { useCallback, useEffect, useRef, useState } from "react";
import {
  isRecord,
  parseBookmarkSavedPayload,
  parseQueryTargetBinding,
  type QueryBookmarkPayload,
  type QueryEditorLanguage,
  type QueryEditorPresentation,
  type QuerySchemaPayload,
  type QueryTargetBinding,
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

interface QueryViewControllerParams {
  panelId: string;
  connectionId: string;
  postgresDatabase?: string;
  initialTargetBinding?: QueryTargetBinding;
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
  postgresDatabase,
  initialTargetBinding,
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
  const pinned = postgresDatabase !== undefined;
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

  const { status, result, setRunning, setResult, setError, reset } =
    useQueryStore();
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
        const target = useConnectionStore
          .getState()
          .connections.find(({ id }) => id === nextConnectionId);
        if (target?.targetSessionKey) {
          postMessage("activeConnectionChanged", {
            connectionId: nextConnectionId,
            targetSessionKey: target.targetSessionKey,
          });
        }
        return;
      }
      if (pinned) return;
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
      panelId,
      pinned,
      reset,
      setActiveConnection,
    ],
  );

  useEffect(() => {
    const unsubscribeTarget = onMessage<unknown>(
      "queryTargetBinding",
      (payload) => {
        const previousBinding = targetBindingRef.current;
        const binding = parseQueryTargetBinding(payload);
        if (
          !previousBinding ||
          !binding ||
          (previousBinding.connectionId === binding.connectionId &&
            previousBinding.sessionKey === binding.sessionKey)
        ) {
          return;
        }
        targetBindingRef.current = binding;
        setTargetBinding(binding);
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
        invalidateBookmark();
        blankQueryValidationRef.current = false;
        reset();
        setActiveConnection(binding.connectionId);
        schemaFetchedRef.current.delete(binding.connectionId);
        setSchema(binding.connectionId, []);
      },
    );
    const unsubscribeResult = onMessage<unknown>(
      "queryResult",
      // Check attribution before treating a malformed reply as terminal.
      (payload) => {
        const { activeOperation, status } = useQueryStore.getState();
        if (
          status !== "running" ||
          !activeOperation ||
          !isRecord(payload) ||
          payload.operationId !== activeOperation.operationId ||
          payload.connectionId !== activeOperation.connectionId
        ) {
          return;
        }
        blankQueryValidationRef.current = false;
        if (!isQueryResult(payload)) {
          setError(INVALID_QUERY_RESULT);
          return;
        }
        if (payload.error) {
          if (hasQueryResultDetails(payload)) {
            setResult(payload);
            return;
          }
          setError(payload.error);
          return;
        }

        setResult(payload);
      },
    );

    const unsubscribeConnections = onMessage<ConnectionEntry[]>(
      "connections",
      (payload) => {
        const binding = targetBindingRef.current;
        if (
          binding &&
          (!binding.sessionKey ||
            payload.find(({ id }) => id === binding.connectionId)
              ?.targetSessionKey !== binding.sessionKey)
        ) {
          invalidateBookmark();
          const queryState = useQueryStore.getState();
          if (queryState.status === "running") {
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
      unsubscribeResult();
      unsubscribeConnections();
      unsubscribeSchema();
      unsubscribeBookmark();
    };
  }, [
    editorInitialValue,
    handleConnectionChange,
    invalidateBookmark,
    panelId,
    pinned,
    reset,
    setActiveConnection,
    setConnections,
    setError,
    setResult,
    setSchema,
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

  const executeQuery = useCallback(() => {
    if (
      pinnedTargetUnavailable ||
      targetReviewRequired ||
      (targetBindingRef.current && !connectionsLoaded) ||
      useQueryStore.getState().status === "running"
    )
      return;
    const queryConnectionId = pinned
      ? connectionId
      : useConnectionStore.getState().activeConnectionId;
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
    setRunning(operation);
    postMessage("executeQuery", {
      ...operation,
      queryText,
      ...(targetBindingRef.current?.sessionKey
        ? { targetSessionKey: targetBindingRef.current.sessionKey }
        : {}),
    });
  }, [
    connectionId,
    connectionsLoaded,
    pinned,
    pinnedTargetUnavailable,
    setError,
    setRunning,
    targetReviewRequired,
  ]);

  const cancelQuery = useCallback(() => {
    const { activeOperation, status } = useQueryStore.getState();
    if (!activeOperation || status !== "running") {
      return;
    }
    postMessage("cancelQuery", activeOperation);
  }, []);

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
    if (
      pinnedTargetUnavailable ||
      targetReviewRequired ||
      (targetBindingRef.current && !connectionsLoaded) ||
      bookmarkedRef.current ||
      pendingBookmarkRef.current
    ) {
      return;
    }

    const queryText = bookmarkTextRef.current;
    const bookmarkConnectionId = pinned
      ? connectionId
      : useConnectionStore.getState().activeConnectionId;
    if (!queryText || !bookmarkConnectionId) {
      return;
    }

    const pending = {
      requestId: `${panelId}:bookmark:${++bookmarkSequenceRef.current}`,
      queryText,
      connectionId: bookmarkConnectionId,
      ...(targetBindingRef.current?.sessionKey
        ? { targetSessionKey: targetBindingRef.current.sessionKey }
        : {}),
    };
    pendingBookmarkRef.current = pending;
    setBookmarkError(null);
    setBookmarking(true);
    postMessage("addBookmark", pending satisfies QueryBookmarkPayload);
  }, [
    connectionId,
    connectionsLoaded,
    panelId,
    pinned,
    pinnedTargetUnavailable,
    targetReviewRequired,
  ]);

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
    status,
    clearQuery,
    cancelQuery,
    editorInitialValue,
  };
}
