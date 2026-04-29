import React, { useCallback, useEffect, useRef, useState } from "react";
import {
  type QueryResult,
  type SchemaObject,
  useConnectionStore,
  useQueryStore,
  useSchemaStore,
} from "../store";
import { buildButtonStyle } from "../utils/buttonStyles";
import { buildSelectControlStyle } from "../utils/controlStyles";
import { onMessage, postMessage } from "../utils/messaging";
import { Icon } from "./Icon";
import {
  connTypeToDialect,
  MonacoEditor,
  type MonacoEditorHandle,
} from "./MonacoEditor";
import { ResultsPanel } from "./ResultsPanel";

interface Props {
  connectionId: string;
  initialSql: string;
  formatOnOpen?: boolean;
  connectionType?: string;
  isBookmarked?: boolean;
}

const btnStyle = (disabled = false): React.CSSProperties =>
  buildButtonStyle("primary", { disabled, gap: 4, size: "sm" });

const btnGhostStyle = (disabled = false): React.CSSProperties =>
  buildButtonStyle("ghost", { disabled, gap: 4, size: "sm" });

const selectStyle: React.CSSProperties = {
  ...buildSelectControlStyle("sm"),
  maxWidth: 220,
};

const TOOLBAR_H = 36;
const DIVIDER_H = 5;
const MIN_EDITOR_H = 80;

const DEFAULT_EDITOR_RATIO = 0.5;
const DEFAULT_EDITOR_H = 400;

export function QueryView({
  connectionId,
  initialSql,
  formatOnOpen = false,
  connectionType = "",
  isBookmarked: initialIsBookmarked = false,
}: Props): React.ReactElement {
  const editorRef = useRef<MonacoEditorHandle>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  const { status, result, setRunning, setResult, setError } = useQueryStore();
  const {
    connections,
    activeConnectionId,
    setConnections,
    setActiveConnection,
  } = useConnectionStore();
  const { setSchema, schemaByConnection } = useSchemaStore();

  const schemaFetchedRef = useRef<Set<string>>(new Set());

  const [editorHeight, setEditorHeight] = useState(DEFAULT_EDITOR_H);
  const [isResizing, setIsResizing] = useState(false);
  const [bookmarked, setBookmarked] = useState(initialIsBookmarked);
  const [bookmarking, setBookmarking] = useState(false);

  const bookmarkedRef = useRef(initialIsBookmarked);
  useEffect(() => {
    bookmarkedRef.current = bookmarked;
  }, [bookmarked]);

  const dragStartY = useRef(0);
  const dragStartH = useRef(DEFAULT_EDITOR_H);

  const didAutoFormat = useRef(false);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) {
      return;
    }
    const usable = el.clientHeight - TOOLBAR_H - DIVIDER_H;
    if (usable > MIN_EDITOR_H * 2) {
      const h = Math.round(usable * DEFAULT_EDITOR_RATIO);
      setEditorHeight(h);
      dragStartH.current = h;
    }
  }, []);

  const schema: SchemaObject[] =
    schemaByConnection[activeConnectionId || connectionId] ?? [];

  const activeConn = connections.find(
    (c) => c.id === (activeConnectionId || connectionId),
  );
  const sqlDialect = connTypeToDialect(activeConn?.type ?? connectionType);

  useEffect(() => {
    setActiveConnection(connectionId);
    postMessage("getConnections");
  }, [connectionId, setActiveConnection]);

  useEffect(() => {
    const id = activeConnectionId || connectionId;
    if (!id) {
      return;
    }
    if (schemaFetchedRef.current.has(id)) {
      return;
    }
    const already = useSchemaStore.getState().schemaByConnection[id];
    if (already !== undefined) {
      schemaFetchedRef.current.add(id);
      return;
    }
    schemaFetchedRef.current.add(id);
    postMessage("getSchema", { connectionId: id });
  }, [activeConnectionId, connectionId]);

  useEffect(() => {
    const unsubResult = onMessage<QueryResult>("queryResult", (payload) => {
      if (payload.error) {
        setError(payload.error);
      } else {
        setResult(payload);
      }
    });

    const unsubConns = onMessage<{ id: string; name: string; type: string }[]>(
      "connections",
      (payload) => {
        setConnections(payload);
      },
    );

    const unsubSchema = onMessage<{
      connectionId: string;
      schema: SchemaObject[];
    }>("schema", (payload) => {
      setSchema(payload.connectionId, payload.schema);
    });

    const unsubBookmark = onMessage<{ ok: boolean; error?: string }>(
      "bookmarkSaved",
      (payload) => {
        setBookmarking(false);
        if (payload.ok) {
          setBookmarked(true);
        }
      },
    );

    return () => {
      unsubResult();
      unsubConns();
      unsubSchema();
      unsubBookmark();
    };
  }, [setConnections, setError, setResult, setSchema]);

  const handleConnectionChange = useCallback(
    (newId: string) => {
      setActiveConnection(newId);
      postMessage("activeConnectionChanged", { connectionId: newId });

      const already = useSchemaStore.getState().schemaByConnection[newId];
      if (already === undefined) {
        schemaFetchedRef.current.delete(newId);
      }
    },
    [setActiveConnection],
  );

  useEffect(() => {
    if (!formatOnOpen || !initialSql || didAutoFormat.current) {
      return;
    }
    if (connections.length === 0) {
      return;
    }
    didAutoFormat.current = true;

    requestAnimationFrame(() => {
      editorRef.current?.format(sqlDialect);

      requestAnimationFrame(() => {
        editorRef.current?.placeCursor();
      });
    });
  }, [connections, formatOnOpen, initialSql, sqlDialect]);

  const didPlaceCursor = useRef(false);
  useEffect(() => {
    if (formatOnOpen) {
      return;
    }
    if (didPlaceCursor.current) {
      return;
    }
    didPlaceCursor.current = true;
    requestAnimationFrame(() => {
      editorRef.current?.placeCursor();
    });
  }, [formatOnOpen]);

  const executeQuery = useCallback(() => {
    const sql = editorRef.current?.getSelectionOrValue().trim() ?? "";
    if (!sql) {
      return;
    }
    setRunning();
    postMessage("executeQuery", {
      sql,
      connectionId: activeConnectionId || connectionId,
    });
  }, [activeConnectionId, connectionId, setRunning]);

  const handleBookmark = useCallback(() => {
    if (bookmarked || bookmarking) {
      return;
    }
    const sql = editorRef.current?.getValue().trim() ?? "";
    if (!sql) {
      return;
    }
    setBookmarking(true);
    postMessage("addBookmark", {
      sql,
      connectionId: activeConnectionId || connectionId,
    });
  }, [bookmarked, bookmarking, activeConnectionId, connectionId]);

  const handleEditorChange = useCallback(() => {
    if (bookmarkedRef.current) {
      setBookmarked(false);
    }
  }, []);

  const onMouseDownDivider = (e: React.MouseEvent) => {
    e.preventDefault();
    dragStartY.current = e.clientY;
    dragStartH.current = editorHeight;
    setIsResizing(true);
  };

  useEffect(() => {
    if (!isResizing) {
      return;
    }
    const onMove = (e: MouseEvent) => {
      const delta = e.clientY - dragStartY.current;
      const next = Math.max(MIN_EDITOR_H, dragStartH.current + delta);
      const maxH =
        (containerRef.current?.clientHeight ?? 600) -
        TOOLBAR_H -
        DIVIDER_H -
        40;
      setEditorHeight(Math.min(next, maxH));
    };
    const onUp = () => setIsResizing(false);
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
    return () => {
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };
  }, [isResizing]);

  return (
    <div
      ref={containerRef}
      style={{
        display: "flex",
        flexDirection: "column",
        height: "100vh",
        overflow: "hidden",
        background: "var(--vscode-editor-background)",
        color: "var(--vscode-foreground)",
      }}
    >
      {}
      <div
        style={{
          height: TOOLBAR_H,
          flexShrink: 0,
          display: "flex",
          alignItems: "center",
          gap: 8,
          padding: "0 10px",
          borderBottom: "1px solid var(--vscode-panel-border)",
          background: "var(--vscode-editorGroupHeader-tabsBackground)",
        }}
      >
        {}
        <select
          aria-label="Active connection"
          style={selectStyle}
          value={activeConnectionId || connectionId}
          onChange={(e) => handleConnectionChange(e.target.value)}
        >
          {connections.length === 0 ? (
            <option value={connectionId}>{connectionId}</option>
          ) : (
            connections.map((c) => (
              <option key={c.id} value={c.id}>
                {c.name} ({c.type})
              </option>
            ))
          )}
        </select>
        {connections.length === 0 && (
          <Icon
            name="sync"
            size={11}
            spin
            style={{ opacity: 0.55, marginLeft: 2, flexShrink: 0 }}
            title="Loading connections…"
          />
        )}

        {}
        <button
          type="button"
          style={btnStyle(status === "running")}
          disabled={status === "running"}
          onClick={executeQuery}
          title="Run query (Ctrl+Enter / F5)"
        >
          <Icon name="run" size={13} style={{ marginRight: 4 }} />
          Run
        </button>

        {}
        <button
          type="button"
          style={btnGhostStyle(false)}
          onClick={() => editorRef.current?.setValue("")}
          title="Clear SQL"
        >
          <Icon name="close" size={13} style={{ marginRight: 4 }} />
          Clear
        </button>

        {}
        <button
          type="button"
          style={btnGhostStyle(false)}
          onClick={() => {
            const err = editorRef.current?.format(sqlDialect) ?? null;
            if (err) {
              setError(`SQL format error: ${err}`);
            }
          }}
          title="Format SQL (Shift+Alt+F)"
        >
          <Icon name="symbol-color" size={13} style={{ marginRight: 4 }} />
          Format
        </button>

        {}
        <button
          type="button"
          style={{
            ...btnGhostStyle(bookmarked || bookmarking),
            ...(bookmarked
              ? {
                  color: "var(--vscode-charts-yellow, #e5c07b)",
                  border: "1px solid var(--vscode-charts-yellow, #e5c07b)",
                }
              : {}),
          }}
          disabled={bookmarked || bookmarking}
          onClick={handleBookmark}
          title={bookmarked ? "Already bookmarked" : "Add to Bookmarks"}
        >
          <Icon name="bookmark" size={13} style={{ marginRight: 4 }} />
          Bookmark
        </button>

        <div style={{ flex: 1 }} />

        {connections.length > 0 &&
          schemaByConnection[activeConnectionId || connectionId] ===
            undefined && (
            <span
              style={{
                fontSize: 11,
                opacity: 0.45,
                display: "flex",
                alignItems: "center",
                gap: 4,
                flexShrink: 0,
              }}
            >
              <Icon name="sync" size={11} spin />
              Indexing schema…
            </span>
          )}

        <span style={{ fontSize: 11, opacity: 0.35 }}>Ctrl+Enter</span>
      </div>

      {}
      <div style={{ height: editorHeight, flexShrink: 0, overflow: "hidden" }}>
        <MonacoEditor
          ref={editorRef}
          initialValue={initialSql || ""}
          schema={schema}
          dialect={sqlDialect}
          onExecute={executeQuery}
          onChange={handleEditorChange}
          height="100%"
        />
      </div>

      {}
      <button
        type="button"
        aria-label="Resize editor and results panels"
        onMouseDown={onMouseDownDivider}
        style={{
          display: "block",
          width: "100%",
          height: 5,
          flexShrink: 0,
          cursor: "row-resize",
          background: isResizing
            ? "var(--vscode-focusBorder)"
            : "var(--vscode-panel-border)",
          transition: isResizing ? "none" : "background 150ms",
          userSelect: "none",
          border: "none",
          padding: 0,
        }}
        title="Drag to resize"
      />

      {}
      <div style={{ flex: 1, overflow: "hidden", minHeight: 0 }}>
        <ResultsPanel status={status} result={result} />
      </div>
    </div>
  );
}
