import { act, fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { App } from "../../src/webview/components/App";
import { ErrorBoundary } from "../../src/webview/components/ErrorBoundary";
import { useQueryStore } from "../../src/webview/store";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  getLastPostedMessage,
  getPostedMessages,
} from "./testUtils";

beforeEach(() => {
  vi.spyOn(console, "error").mockImplementation(() => {});
});

describe("webview crash recovery", () => {
  it("retries a transient render failure", () => {
    let ready = false;
    function View() {
      if (!ready) throw new Error("Not ready");
      return <div>View recovered</div>;
    }

    render(
      <ErrorBoundary>
        <View />
      </ErrorBoundary>,
    );

    expect(screen.getByText("Not ready")).toBeTruthy();
    ready = true;
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    expect(screen.getByText("View recovered")).toBeTruthy();
  });

  it("offers a non-destructive next step when the same props crash again", () => {
    function View({ error }: { error: string }): never {
      throw new Error(error);
    }

    render(
      <ErrorBoundary>
        <View error="Persistent failure" />
      </ErrorBoundary>,
    );
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));

    expect(screen.getByText("Persistent failure")).toBeTruthy();
    expect(screen.getByText("Developer: Reload Webviews")).toBeTruthy();
    expect(screen.getByText(/let active operations finish first/)).toBeTruthy();
    expect(getPostedMessages()).toEqual([]);
    expect(window.__vscode?.setState).not.toHaveBeenCalled();
  });

  it("hides a crashing stored result without remounting the editor or altering an operation", () => {
    const vscode = window.__vscode;
    if (!vscode) throw new Error("VS Code API is unavailable");
    const text = "select 'saved draft'";
    const persisted = {
      queryDraft: { panelId: "panel", text, activeConnectionId: "conn" },
      tableDraft: { dirty: true, reconciliation: { unknown: true } },
    };
    vi.mocked(vscode.getState).mockReturnValue(persisted);
    window.__RAPIDB_INITIAL_STATE__ = {
      view: "query",
      panelId: "panel",
      connectionId: "conn",
      connectionType: "pg",
      queryText: "select 'initial'",
    };

    render(<App />);
    const editor = screen.getByTestId("mock-monaco-dom");
    const run = () => {
      fireEvent.click(screen.getByRole("button", { name: /^Run$/ }));
      const request = getLastPostedMessage();
      expect(request).toMatchObject({
        type: "executeQuery",
        payload: { queryText: text, connectionId: "conn" },
      });
      return request?.payload as { operationId: string; connectionId: string };
    };
    const operation = run();
    const result = {
      columns: ["value"],
      columnMeta: [],
      rows: [{ __col_0: 1 }],
      rowCount: 1,
      executionTimeMs: 2,
    };
    // The existing reply checks accept this shape, but the formatter can't
    // display the truncation count. It lives in the store across React retries.
    act(() =>
      dispatchIncomingMessage("queryResult", {
        ...result,
        ...operation,
        truncated: true,
        truncatedAt: { toLocaleString: null },
      }),
    );
    expect(useQueryStore.getState().status).toBe("success");
    const crashedState = useQueryStore.getState();

    clearPostedMessages();
    expect(screen.getByRole("alert").textContent).toContain(
      "Query results crashed",
    );
    expect(screen.getByTestId("mock-monaco-dom")).toBe(editor);
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    expect(screen.getByRole("alert").textContent).toContain(
      "Query results crashed",
    );
    fireEvent.click(screen.getByRole("button", { name: "Hide results" }));

    expect(screen.getByText(/Results hidden/)).toBeTruthy();
    expect(screen.queryByText(/crashed/)).toBeNull();
    expect(screen.getByTestId("mock-monaco-dom")).toBe(editor);
    expect(getPostedMessages()).toEqual([]);
    expect(vscode.setState).not.toHaveBeenCalled();
    expect(useQueryStore.getState()).toBe(crashedState);

    // Only the user's explicit Run starts a new operation. It can receive a
    // result normally, and a late reply for the old operation is still ignored.
    const next = run();
    expect(next.operationId).not.toBe(operation.operationId);
    act(() =>
      dispatchIncomingMessage("queryResult", { ...result, ...operation }),
    );
    expect(useQueryStore.getState().status).toBe("running");
    expect(screen.getByRole("button", { name: "Stop" })).toBeTruthy();
    act(() => dispatchIncomingMessage("queryResult", { ...result, ...next }));
    expect(screen.getByText("1 row")).toBeTruthy();
  });
});
