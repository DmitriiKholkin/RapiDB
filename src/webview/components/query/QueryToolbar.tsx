import React from "react";
import type { ConnectionEntry, QueryStatus } from "../../store";
import { Icon } from "../Icon";
import {
  buildQueryGhostButtonStyle,
  buildQueryPrimaryButtonStyle,
  querySelectStyle,
  queryToolbarStyle,
} from "./queryViewHelpers";

interface QueryToolbarProps {
  bookmarked: boolean;
  bookmarking: boolean;
  canFormat: boolean;
  connectionId: string;
  connections: readonly ConnectionEntry[];
  formatButtonTitle: string;
  schemaLoading: boolean;
  selectedConnectionId: string;
  status: QueryStatus;
  onBookmark: () => void;
  onClear: () => void;
  onConnectionChange: (connectionId: string) => void;
  onFormat: () => void;
  onRun: () => void;
}

export function QueryToolbar({
  bookmarked,
  bookmarking,
  canFormat,
  connectionId,
  connections,
  formatButtonTitle,
  schemaLoading,
  selectedConnectionId,
  status,
  onBookmark,
  onClear,
  onConnectionChange,
  onFormat,
  onRun,
}: QueryToolbarProps): React.ReactElement {
  return (
    <div style={queryToolbarStyle}>
      <select
        aria-label="Active connection"
        style={querySelectStyle}
        value={selectedConnectionId}
        onChange={(event) => onConnectionChange(event.target.value)}
      >
        {connections.length === 0 ? (
          <option value={connectionId}>{connectionId}</option>
        ) : (
          connections.map((connection) => (
            <option key={connection.id} value={connection.id}>
              {connection.name} ({connection.type})
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

      <button
        type="button"
        style={buildQueryPrimaryButtonStyle(status === "running")}
        disabled={status === "running"}
        onClick={onRun}
        title="Run query (Ctrl+Enter / F5)"
      >
        <Icon name="run" size={13} style={{ marginRight: 4 }} />
        Run
      </button>

      <button
        type="button"
        style={buildQueryGhostButtonStyle(false)}
        onClick={onClear}
        title="Clear query"
      >
        <Icon name="close" size={13} style={{ marginRight: 4 }} />
        Clear
      </button>

      <button
        type="button"
        style={buildQueryGhostButtonStyle(!canFormat)}
        disabled={!canFormat}
        onClick={onFormat}
        title={formatButtonTitle}
      >
        <Icon name="symbol-color" size={13} style={{ marginRight: 4 }} />
        Format
      </button>

      <button
        type="button"
        style={{
          ...buildQueryGhostButtonStyle(bookmarked || bookmarking),
          ...(bookmarked
            ? {
                color: "var(--vscode-charts-yellow, #e5c07b)",
                border: "1px solid var(--vscode-charts-yellow, #e5c07b)",
              }
            : {}),
        }}
        disabled={bookmarked || bookmarking}
        onClick={onBookmark}
        title={bookmarked ? "Already bookmarked" : "Add to Bookmarks"}
      >
        <Icon name="bookmark" size={13} style={{ marginRight: 4 }} />
        Bookmark
      </button>

      <div style={{ flex: 1 }} />

      {schemaLoading && (
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
  );
}
