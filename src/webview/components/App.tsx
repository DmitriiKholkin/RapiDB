// biome-ignore lint/style/useImportType: React needed for JSX
import React from "react";
import { ConnectionFormView } from "./ConnectionFormView";
import { ErrorBoundary } from "./ErrorBoundary";
import { QueryView } from "./QueryView";
import { SchemaView } from "./SchemaView";
import { TableView } from "./TableView";

type ViewName = "query" | "table" | "schema" | "connection";

interface InitialState {
  view: ViewName;
  connectionId?: string;
  connectionType?: string;
  formatOnOpen?: boolean;
  isBookmarked?: boolean;
  database?: string;
  schema?: string;
  table?: string;
  initialSql?: string;
  existing?: any | null;
  isView?: boolean;
  defaultPageSize?: number;
}

const state: InitialState = (window as any).__RAPIDB_INITIAL_STATE__ ?? {
  view: "query",
  connectionId: "",
};

export function App(): React.ReactElement {
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
    case "connection":
      return (
        <ErrorBoundary context="ConnectionFormView">
          <ConnectionFormView existing={state.existing} />
        </ErrorBoundary>
      );

    default:
      return (
        <div style={{ padding: 16, color: "var(--vscode-errorForeground)" }}>
          Unknown view: {(state as any).view}
        </div>
      );
  }
}
