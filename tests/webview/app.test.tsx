import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type {
  QueryInitialState,
  TableInitialState,
} from "../../src/shared/webviewContracts";

type AppHostWindow = Window & {
  __RAPIDB_INITIAL_STATE__?: unknown;
};

vi.mock("../../src/webview/components/ConnectionFormView", () => ({
  ConnectionFormView: ({
    existing,
  }: {
    existing?: { name?: string } | null;
  }) => <div>Connection:{existing?.name ?? "new"}</div>,
}));

vi.mock("../../src/webview/components/QueryView", () => ({
  QueryView: ({
    connectionId,
    editorLanguage,
    editorPresentation,
  }: {
    connectionId: string;
    editorLanguage?: string;
    editorPresentation?: { sqlDialect?: string };
  }) => (
    <div>
      Query:{connectionId}:{editorLanguage ?? "default"}:
      {editorPresentation?.sqlDialect ?? "none"}
    </div>
  ),
}));

vi.mock("../../src/webview/components/TableView", () => ({
  TableView: ({
    table,
    isView,
    connectionReadOnly,
  }: {
    table: string;
    isView?: boolean;
    connectionReadOnly?: boolean;
  }) => (
    <div>
      Table:{table}:{String(isView)}:{String(connectionReadOnly)}
    </div>
  ),
}));

vi.mock("../../src/webview/components/ErdView", () => ({
  ErdView: ({ schema }: { schema?: string }) => (
    <div>ERD:{schema ?? "all"}</div>
  ),
}));

import { App } from "../../src/webview/components/App";

describe("App", () => {
  it("selects the current view from window state on each render", () => {
    const hostWindow = window as AppHostWindow;
    const queryState: QueryInitialState = {
      view: "query",
      connectionId: "conn-1",
      connectionType: "pg",
      queryText: "select 1",
      initialSql: "select 1",
      formatOnOpen: false,
      isBookmarked: false,
      editorLanguage: "sql",
      editorPresentation: {
        formatOnOpen: false,
        editorLanguage: "sql",
        sqlDialect: "postgresql",
      },
    };
    const tableState: TableInitialState = {
      view: "table",
      connectionId: "conn-1",
      database: "main",
      schema: "public",
      table: "users",
      isView: false,
      connectionReadOnly: true,
      defaultPageSize: 25,
    };

    hostWindow.__RAPIDB_INITIAL_STATE__ = queryState;

    const { rerender } = render(<App />);
    expect(screen.getByText("Query:conn-1:sql:postgresql")).toBeTruthy();

    hostWindow.__RAPIDB_INITIAL_STATE__ = tableState;
    rerender(<App />);

    expect(screen.getByText("Table:users:false:true")).toBeTruthy();
  });

  it("falls back to the default query state when the initial state is invalid", () => {
    Reflect.set(
      window as unknown as Record<string, unknown>,
      "__RAPIDB_INITIAL_STATE__",
      {
        view: "query",
      },
    );

    render(<App />);

    expect(screen.getByText("Query::default:none")).toBeTruthy();
  });

  it("renders erd view when requested by initial state", () => {
    const hostWindow = window as AppHostWindow;

    hostWindow.__RAPIDB_INITIAL_STATE__ = {
      view: "erd",
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
    };

    render(<App />);

    expect(screen.getByText("ERD:public")).toBeTruthy();
  });
});
