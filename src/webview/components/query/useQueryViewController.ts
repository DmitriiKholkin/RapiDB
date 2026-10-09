import { useCallback, useEffect, useRef, useState } from "react";
import {
  parseBookmarkSavedPayload,
  type QueryBookmarkPayload,
  type QueryEditorLanguage,
  type QueryEditorPresentation,
} from "../../../shared/webviewContracts";
import {
  type ConnectionEntry,
  type QueryResult,
  type SchemaObject,
  useConnectionStore,
  useQueryStore,
  useSchemaStore,
} from "../../store";
import { onMessage, postMessage } from "../../utils/messaging";
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

interface QueryViewControllerParams {
  panelId: string;
  connectionId: string;
  editorLanguage?: QueryEditorLanguage;
  editorPresentation?: QueryEditorPresentation;
  formatOnOpen: boolean;
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
  const draftInitiallyBookmarked =
    initialIsBookmarked &&
    editorInitialValue.trim() === initialQueryText.trim() &&
    (queryDraft.activeConnectionId ?? connectionId) === connectionId;
  const editorRef = useRef<MonacoEditorHandle>(null);
  const containerRef = useRef<HTMLDivElement>(null);

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
  const operationSequenceRef = useRef(0);
  const activeOperationRef = useRef<{
    operationId: string;
    connectionId: string;
  } | null>(null);
  const blankQueryValidationRef = useRef(false);

  const [editorHeight, setEditorHeight] = useState(DEFAULT_EDITOR_H);
  const [isResizing, setIsResizing] = useState(false);
  const [connectionsLoaded, setConnectionsLoaded] = useState(false);
  const [bookmarked, setBookmarked] = useState(draftInitiallyBookmarked);
  const [bookmarking, setBookmarking] = useState(false);

  const invalidateBookmark = useCallback(() => {
    pendingBookmarkRef.current = null;
    bookmarkedRef.current = false;
    setBookmarked(false);
    setBookmarking(false);
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

  const resolvedConnectionId =
    activeConnectionId ||
    (connectionsLoaded ? "" : (queryDraft.activeConnectionId ?? connectionId));
  useEffect(() => {
    if (bookmarkConnectionRef.current !== resolvedConnectionId) {
      bookmarkConnectionRef.current = resolvedConnectionId;
      invalidateBookmark();
    }
  }, [invalidateBookmark, resolvedConnectionId]);
  const activeConnection = connections.find(
    (connection) => connection.id === resolvedConnectionId,
  );
  const schema: SchemaObject[] = schemaByConnection[resolvedConnectionId] ?? [];
  const editorState = resolveQueryEditorState({
    activeConnection,
    editorLanguage,
    editorPresentation,
    formatOnOpen,
    initialConnectionId: connectionId,
    resolvedConnectionId,
  });

  useEffect(() => {
    setActiveConnection(queryDraft.activeConnectionId ?? connectionId);
    postMessage("getConnections");
  }, [connectionId, queryDraft.activeConnectionId, setActiveConnection]);

  useEffect(() => {
    if (!resolvedConnectionId) {
      return;
    }

    if (schemaFetchedRef.current.has(resolvedConnectionId)) {
      return;
    }

    const cachedSchema =
      useSchemaStore.getState().schemaByConnection[resolvedConnectionId];
    if (cachedSchema !== undefined) {
      schemaFetchedRef.current.add(resolvedConnectionId);
      return;
    }

    schemaFetchedRef.current.add(resolvedConnectionId);
    postMessage("getSchema", { connectionId: resolvedConnectionId });
  }, [resolvedConnectionId]);

  const handleConnectionChange = useCallback(
    (nextConnectionId: string) => {
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
      activeOperationRef.current = null;
      reset();
      setActiveConnection(nextConnectionId);
      if (!nextConnectionId) return;
      postMessage("activeConnectionChanged", {
        connectionId: nextConnectionId,
      });

      const cachedSchema =
        useSchemaStore.getState().schemaByConnection[nextConnectionId];
      if (cachedSchema === undefined) {
        schemaFetchedRef.current.delete(nextConnectionId);
      }
    },
    [
      editorInitialValue,
      invalidateBookmark,
      panelId,
      reset,
      setActiveConnection,
    ],
  );

  useEffect(() => {
    const unsubscribeResult = onMessage<QueryResult>(
      "queryResult",
      (payload) => {
        const activeOperation = activeOperationRef.current;
        if (
          !activeOperation ||
          payload.operationId !== activeOperation.operationId ||
          payload.connectionId !== activeOperation.connectionId
        ) {
          return;
        }
        if (payload.error) {
          blankQueryValidationRef.current = false;
          setError(payload.error);
          return;
        }

        blankQueryValidationRef.current = false;
        setResult(payload);
      },
    );

    const unsubscribeConnections = onMessage<ConnectionEntry[]>(
      "connections",
      (payload) => {
        const currentConnectionId =
          useConnectionStore.getState().activeConnectionId;
        if (
          !payload.some((connection) => connection.id === currentConnectionId)
        ) {
          invalidateBookmark();
          handleConnectionChange(payload[0]?.id ?? "");
        }
        setConnectionsLoaded(true);
        setConnections(payload);
      },
    );

    const unsubscribeSchema = onMessage<{
      connectionId: string;
      schema: SchemaObject[];
    }>("schema", (payload) => {
      setSchema(payload.connectionId, payload.schema);
    });

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
      }
    });

    return () => {
      unsubscribeResult();
      unsubscribeConnections();
      unsubscribeSchema();
      unsubscribeBookmark();
    };
  }, [
    handleConnectionChange,
    invalidateBookmark,
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
    // Monaco shortcuts remain available while Run is disabled. Do not let a
    // repeated submission (including blank validation) replace an active run.
    if (useQueryStore.getState().status === "running") return;
    // Read live state so a shortcut in the same event as connection removal
    // cannot use the previous render's connection or the initial fallback.
    const queryConnectionId = useConnectionStore.getState().activeConnectionId;
    if (!queryConnectionId) return;
    const queryText = editorRef.current?.getSelectionOrValue().trim() ?? "";
    if (!queryText) {
      blankQueryValidationRef.current = true;
      setError(BLANK_QUERY_VALIDATION_MESSAGE);
      return;
    }

    blankQueryValidationRef.current = false;
    setRunning();
    const operationId = `${panelId}:${++operationSequenceRef.current}`;
    activeOperationRef.current = {
      operationId,
      connectionId: queryConnectionId,
    };
    postMessage("executeQuery", {
      queryText,
      connectionId: queryConnectionId,
      operationId,
    });
  }, [panelId, setError, setRunning]);

  const cancelQuery = useCallback(() => {
    const activeOperation = activeOperationRef.current;
    if (!activeOperation || useQueryStore.getState().status !== "running") {
      return;
    }
    postMessage("cancelQuery", {
      operationId: activeOperation.operationId,
    });
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
    if (bookmarkedRef.current || pendingBookmarkRef.current) {
      return;
    }

    const queryText = bookmarkTextRef.current;
    const bookmarkConnectionId =
      useConnectionStore.getState().activeConnectionId;
    if (!queryText || !bookmarkConnectionId) {
      return;
    }

    const pending = {
      requestId: `${panelId}:bookmark:${++bookmarkSequenceRef.current}`,
      queryText,
      connectionId: bookmarkConnectionId,
    };
    pendingBookmarkRef.current = pending;
    setBookmarking(true);
    postMessage("addBookmark", pending satisfies QueryBookmarkPayload);
  }, [panelId]);

  const handleEditorChange = useCallback(
    (value: string) => {
      updateWebviewState((state) => ({
        ...state,
        queryDraft: {
          panelId,
          text: value,
          activeConnectionId: resolvedConnectionId,
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
    bookmarked,
    bookmarking,
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
    schemaLoading:
      connections.length > 0 &&
      schemaByConnection[resolvedConnectionId] === undefined,
    startResizing,
    status,
    clearQuery,
    cancelQuery,
    editorInitialValue,
  };
}
