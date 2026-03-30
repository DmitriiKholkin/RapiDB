// biome-ignore lint/style/useImportType: <explanation>
import React from "react";
import { ConnectionFormView } from "./ConnectionFormView";
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

const state: InitialState = (window as any).__HAPPYDB_INITIAL_STATE__ ?? {
  view: "query",
  connectionId: "",
};

export function App(): React.ReactElement {
  switch (state.view) {
    case "query":
      return (
        <QueryView
          connectionId={state.connectionId ?? ""}
          connectionType={state.connectionType ?? ""}
          initialSql={state.initialSql ?? ""}
          formatOnOpen={state.formatOnOpen ?? false}
          isBookmarked={state.isBookmarked ?? false}
        />
      );
    case "table":
      return (
        <TableView
          connectionId={state.connectionId ?? ""}
          database={state.database ?? ""}
          schema={state.schema ?? ""}
          table={state.table ?? ""}
          isView={state.isView ?? false}
          defaultPageSize={state.defaultPageSize}
        />
      );
    case "schema":
      return (
        <SchemaView
          connectionId={state.connectionId ?? ""}
          database={state.database ?? ""}
          schema={state.schema ?? ""}
          table={state.table ?? ""}
        />
      );
    case "connection":
      return <ConnectionFormView existing={state.existing} />;

    default:
      return (
        <div style={{ padding: 16, color: "var(--vscode-errorForeground)" }}>
          Unknown view: {(state as any).view}
        </div>
      );
  }
}
