import React, { ReactElement } from "react";
import type {
  QueryInitialState,
  WebviewInitialState,
} from "../../shared/webviewContracts";
import { parseWebviewInitialState } from "../../shared/webviewContracts";
import { ConnectionFormView } from "./ConnectionFormView";
import { ErdView } from "./ErdView";
import { ErrorBoundary } from "./ErrorBoundary";
import { QueryView } from "./QueryView";
import { SchemaView } from "./SchemaView";
import { TableView } from "./TableView";

const fallbackState: QueryInitialState = {
  view: "query",
  connectionId: "",
  connectionType: "",
};

export function App(): ReactElement {
  const state: WebviewInitialState =
    parseWebviewInitialState(window.__RAPIDB_INITIAL_STATE__) ?? fallbackState;

  switch (state.view) {
    case "query":
      return (
        <ErrorBoundary context="QueryView">
          <QueryView
            connectionId={state.connectionId ?? ""}
            connectionType={state.connectionType ?? ""}
            initialSql={state.initialSql ?? ""}
            formatOnOpen={state.formatOnOpen ?? false}
            isBookmarked={state.isBookmarked ?? false}
          />
        </ErrorBoundary>
      );
    case "table":
      return (
        <ErrorBoundary context="TableView">
          <TableView
            connectionId={state.connectionId ?? ""}
            database={state.database ?? ""}
            schema={state.schema ?? ""}
            table={state.table ?? ""}
            isView={state.isView ?? false}
            defaultPageSize={state.defaultPageSize}
          />
        </ErrorBoundary>
      );
    case "schema":
      return (
        <ErrorBoundary context="SchemaView">
          <SchemaView
            connectionId={state.connectionId ?? ""}
            database={state.database ?? ""}
            schema={state.schema ?? ""}
            table={state.table ?? ""}
          />
        </ErrorBoundary>
      );
    case "erd":
      return (
        <ErrorBoundary context="ErdView">
          <ErdView
            connectionId={state.connectionId ?? ""}
            database={state.database}
            schema={state.schema}
          />
        </ErrorBoundary>
      );
    case "connection":
      return (
        <ErrorBoundary context="ConnectionFormView">
          <ConnectionFormView existing={state.existing} />
        </ErrorBoundary>
      );

    default: {
      const unknownView = (state as { view?: string }).view ?? "unknown";
      return (
        <div style={{ padding: 16, color: "var(--vscode-errorForeground)" }}>
          Unknown view: {unknownView}
        </div>
      );
    }
  }
}
