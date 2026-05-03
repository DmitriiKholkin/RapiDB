import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type {
  QueryInitialState,
  TableInitialState,
} from "../../src/shared/webviewContracts";

vi.mock("../../src/webview/components/ConnectionFormView", () => ({
  ConnectionFormView: ({
    existing,
  }: {
    existing?: { name?: string } | null;
  }) => <div>Connection:{existing?.name ?? "new"}</div>,
}));

vi.mock("../../src/webview/components/QueryView", () => ({
  QueryView: ({ connectionId }: { connectionId: string }) => (
    <div>Query:{connectionId}</div>
  ),
}));

vi.mock("../../src/webview/components/SchemaView", () => ({
  SchemaView: ({ table }: { table: string }) => <div>Schema:{table}</div>,
}));

vi.mock("../../src/webview/components/TableView", () => ({
  TableView: ({ table }: { table: string }) => <div>Table:{table}</div>,
}));

vi.mock("../../src/webview/components/ErdView", () => ({
  ErdView: ({ schema }: { schema?: string }) => (
    <div>ERD:{schema ?? "all"}</div>
  ),
}));

import { App } from "../../src/webview/components/App";

describe("App", () => {
  it("selects the current view from window state on each render", () => {
    const queryState: QueryInitialState = {
      view: "query",
      connectionId: "conn-1",
      connectionType: "pg",
      initialSql: "select 1",
      formatOnOpen: false,
      isBookmarked: false,
    };
    const tableState: TableInitialState = {
      view: "table",
      connectionId: "conn-1",
      database: "main",
      schema: "public",
      table: "users",
      isView: false,
      defaultPageSize: 25,
    };

    window.__RAPIDB_INITIAL_STATE__ = queryState;

    const { rerender } = render(<App />);
    expect(screen.getByText("Query:conn-1")).toBeTruthy();

    window.__RAPIDB_INITIAL_STATE__ = tableState;
    rerender(<App />);

    expect(screen.getByText("Table:users")).toBeTruthy();
  });

  it("falls back to the default query state when the initial state is invalid", () => {
    window.__RAPIDB_INITIAL_STATE__ = {
      view: "query",
    } as unknown as Window["__RAPIDB_INITIAL_STATE__"];

    render(<App />);

    expect(screen.getByText("Query:")).toBeTruthy();
  });

  it("renders erd view when requested by initial state", () => {
    window.__RAPIDB_INITIAL_STATE__ = {
      view: "erd",
      connectionId: "conn-1",
      database: "app_db",
      schema: "public",
    };

    render(<App />);

    expect(screen.getByText("ERD:public")).toBeTruthy();
  });
});
