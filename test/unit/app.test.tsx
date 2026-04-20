/**
 * @vitest-environment jsdom
 */

import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("../../src/webview/components/ConnectionFormView", () => ({
  ConnectionFormView: () => <div data-testid="connection-view" />,
}));

vi.mock("../../src/webview/components/QueryView", () => ({
  QueryView: (props: { connectionId: string; connectionType: string }) => (
    <div data-testid="query-view">
      {JSON.stringify({
        connectionId: props.connectionId,
        connectionType: props.connectionType,
      })}
    </div>
  ),
}));

vi.mock("../../src/webview/components/SchemaView", () => ({
  SchemaView: () => <div data-testid="schema-view" />,
}));

vi.mock("../../src/webview/components/TableView", () => ({
  TableView: () => <div data-testid="table-view" />,
}));

afterEach(() => {
  cleanup();
  vi.resetModules();
});

describe("App", () => {
  beforeEach(() => {
    window.__RAPIDB_INITIAL_STATE__ = undefined;
  });

  it("falls back to the query view when the initial state is malformed", async () => {
    window.__RAPIDB_INITIAL_STATE__ = {
      view: "connection",
      existing: {
        id: "conn-1",
        type: "pg",
      },
    };

    const { App } = await import("../../src/webview/components/App");
    render(<App />);

    expect(screen.getByTestId("query-view").textContent).toBe(
      JSON.stringify({ connectionId: "", connectionType: "" }),
    );
  });
});
