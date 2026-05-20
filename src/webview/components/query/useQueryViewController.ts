import { useCallback, useEffect, useRef, useState } from "react";
import type {
  QueryEditorLanguage,
  QueryEditorPresentation,
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
import type { MonacoEditorHandle } from "../MonacoEditor";
import {
  DEFAULT_EDITOR_H,
  DEFAULT_EDITOR_RATIO,
  DIVIDER_H,
  MIN_EDITOR_H,
  resolveQueryEditorState,
  TOOLBAR_H,
} from "./queryViewHelpers";

interface QueryViewControllerParams {
  connectionId: string;
  editorLanguage?: QueryEditorLanguage;
  editorPresentation?: QueryEditorPresentation;
  formatOnOpen: boolean;
  initialIsBookmarked: boolean;
  initialQueryText: string;
}

export function useQueryViewController({
  connectionId,
  editorLanguage,
  editorPresentation,
  formatOnOpen,
  initialIsBookmarked,
  initialQueryText,
}: QueryViewControllerParams) {
  const editorRef = useRef<MonacoEditorHandle>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  const { status, result, setRunning, setResult, setError } = useQueryStore();
  const {
    connections,
    activeConnectionId,
    setConnections,
    setActiveConnection,
  } = useConnectionStore();
  const { schemaByConnection, setSchema } = useSchemaStore();

  const schemaFetchedRef = useRef<Set<string>>(new Set());
  const bookmarkedRef = useRef(initialIsBookmarked);
  const dragStartY = useRef(0);
  const dragStartH = useRef(DEFAULT_EDITOR_H);
  const didAutoFormat = useRef(false);
  const didPlaceCursor = useRef(false);

  const [editorHeight, setEditorHeight] = useState(DEFAULT_EDITOR_H);
  const [isResizing, setIsResizing] = useState(false);
  const [bookmarked, setBookmarked] = useState(initialIsBookmarked);
  const [bookmarking, setBookmarking] = useState(false);

  useEffect(() => {
    bookmarkedRef.current = bookmarked;
  }, [bookmarked]);

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

  const resolvedConnectionId = activeConnectionId || connectionId;
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
    setActiveConnection(connectionId);
    postMessage("getConnections");
  }, [connectionId, setActiveConnection]);

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

  useEffect(() => {
    const unsubscribeResult = onMessage<QueryResult>(
      "queryResult",
      (payload) => {
        if (payload.error) {
          setError(payload.error);
          return;
        }

        setResult(payload);
      },
    );

    const unsubscribeConnections = onMessage<ConnectionEntry[]>(
      "connections",
      (payload) => {
        setConnections(payload);
      },
    );

    const unsubscribeSchema = onMessage<{
      connectionId: string;
      schema: SchemaObject[];
    }>("schema", (payload) => {
      setSchema(payload.connectionId, payload.schema);
    });

    const unsubscribeBookmark = onMessage<{ ok: boolean; error?: string }>(
      "bookmarkSaved",
      (payload) => {
        setBookmarking(false);
        if (payload.ok) {
          setBookmarked(true);
        }
      },
    );

    return () => {
      unsubscribeResult();
      unsubscribeConnections();
      unsubscribeSchema();
      unsubscribeBookmark();
    };
  }, [setConnections, setError, setResult, setSchema]);

  const handleConnectionChange = useCallback(
    (nextConnectionId: string) => {
      setActiveConnection(nextConnectionId);
      postMessage("activeConnectionChanged", {
        connectionId: nextConnectionId,
      });

      const cachedSchema =
        useSchemaStore.getState().schemaByConnection[nextConnectionId];
      if (cachedSchema === undefined) {
        schemaFetchedRef.current.delete(nextConnectionId);
      }
    },
    [setActiveConnection],
  );

  useEffect(() => {
    if (
      !editorState.shouldFormatOnOpen ||
      !initialQueryText ||
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
    initialQueryText,
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
    const queryText = editorRef.current?.getSelectionOrValue().trim() ?? "";
    if (!queryText) {
      return;
    }

    setRunning();
    postMessage("executeQuery", {
      queryText,
      connectionId: resolvedConnectionId,
    });
  }, [resolvedConnectionId, setRunning]);

  const clearQuery = useCallback(() => {
    editorRef.current?.setValue("");
  }, []);

  const formatQuery = useCallback(() => {
    if (!editorState.canFormat) {
      return;
    }

    const error = editorRef.current?.format(editorState.sqlDialect) ?? null;
    if (error) {
      setError(`${editorState.formatErrorPrefix}: ${error}`);
    }
  }, [
    editorState.canFormat,
    editorState.formatErrorPrefix,
    editorState.sqlDialect,
    setError,
  ]);

  const handleBookmark = useCallback(() => {
    if (bookmarked || bookmarking) {
      return;
    }

    const queryText = editorRef.current?.getValue().trim() ?? "";
    if (!queryText) {
      return;
    }

    setBookmarking(true);
    postMessage("addBookmark", {
      queryText,
      connectionId: resolvedConnectionId,
    });
  }, [bookmarked, bookmarking, resolvedConnectionId]);

  const handleEditorChange = useCallback(() => {
    if (bookmarkedRef.current) {
      setBookmarked(false);
    }
  }, []);

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
    isResizing,
    result,
    schema,
    schemaLoading:
      connections.length > 0 &&
      schemaByConnection[resolvedConnectionId] === undefined,
    startResizing,
    status,
    clearQuery,
  };
}
