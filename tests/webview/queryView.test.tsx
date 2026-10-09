import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { QUERY_LIMIT_POLICY } from "../../src/shared/safetyContracts";

const formatMock = vi.hoisted(() => vi.fn((_dialect?: string) => null));

vi.mock("../../src/webview/components/MonacoEditor", async () => {
  const React = await import("react");

  interface MockMonacoEditorHandle {
    getSelectionOrValue(): string;
    getValue(): string;
    setValue(value: string): void;
    clearValue(): void;
    format(dialect?: string): string | null;
    placeCursor(): void;
  }

  interface MockMonacoEditorProps {
    initialValue?: string;
    onChange?: (value: string) => void;
    onSelectionChange?: () => void;
    readOnly?: boolean;
    ariaLabel?: string;
    schema?: Array<unknown>;
    dialect?: string;
    language?: string;
    onExecute?: (value: string) => void;
  }

  const MonacoEditor = React.forwardRef<
    MockMonacoEditorHandle,
    MockMonacoEditorProps
  >(function MockMonacoEditor(props, ref) {
    const [value, setValue] = React.useState(props.initialValue ?? "");
    const currentValueRef = React.useRef(props.initialValue ?? "");
    const textareaRef = React.useRef<HTMLTextAreaElement>(null);
    const updateValue = React.useCallback((nextValue: string) => {
      currentValueRef.current = nextValue;
      setValue(nextValue);
    }, []);

    React.useImperativeHandle(
      ref,
      () => ({
        getSelectionOrValue: () => {
          const textarea = textareaRef.current;
          const start = textarea?.selectionStart ?? 0;
          const end = textarea?.selectionEnd ?? 0;
          return start !== end
            ? currentValueRef.current.slice(start, end)
            : currentValueRef.current;
        },
        getValue: () => currentValueRef.current,
        setValue: (nextValue: string) => {
          updateValue(nextValue);
          props.onChange?.(nextValue);
        },
        clearValue: () => {
          updateValue("");
          props.onChange?.("");
        },
        format: (dialect?: string) => {
          formatMock(dialect);

          if (props.language === "json") {
            try {
              const nextValue = JSON.stringify(
                JSON.parse(currentValueRef.current) as unknown,
                null,
                2,
              );

              if (nextValue !== currentValueRef.current) {
                updateValue(nextValue);
                props.onChange?.(nextValue);
              }

              return null;
            } catch (error: unknown) {
              return error instanceof Error ? error.message : String(error);
            }
          }

          return null;
        },
        placeCursor: () => undefined,
      }),
      [props, updateValue],
    );

    return (
      <div>
        <div data-testid="monaco-schema-count">
          {String(props.schema?.length ?? 0)}
        </div>
        <div data-testid="monaco-language">{props.language ?? "sql"}</div>
        <div data-testid="monaco-dialect">{props.dialect ?? "none"}</div>
        <textarea
          ref={textareaRef}
          aria-label={props.ariaLabel ?? "SQL editor"}
          readOnly={props.readOnly}
          value={value}
          onChange={(event) => {
            updateValue(event.target.value);
            props.onChange?.(event.target.value);
          }}
          onSelect={() => props.onSelectionChange?.()}
          onKeyDown={(event) => {
            if (
              event.key === "F5" ||
              (event.key === "Enter" && (event.ctrlKey || event.metaKey))
            ) {
              props.onExecute?.(value);
            }
          }}
        />
      </div>
    );
  });

  return {
    MonacoEditor,
    connTypeToDialect: (type: string) => {
      switch (type) {
        case "mysql":
          return "mysql";
        case "pg":
          return "postgresql";
        case "sqlite":
          return "sqlite";
        case "mssql":
          return "transactsql";
        case "oracle":
          return "plsql";
        default:
          return "sql";
      }
    },
  };
});

vi.mock("../../src/webview/components/table/TableGrid", () => ({
  TableGrid: ({
    mode,
    status,
    result,
  }: {
    mode?: string;
    status?: string;
    result?: { error?: string; rowCount?: number } | null;
  }) => (
    <div data-testid="results-panel">
      {String(mode ?? "table")}:{status ?? "none"}:
      {result?.error ?? String(result?.rowCount ?? "none")}
    </div>
  ),
}));

import { QueryView } from "../../src/webview/components/QueryView";
import {
  useConnectionStore,
  useQueryStore,
  useSchemaStore,
} from "../../src/webview/store";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  expectNoAxeViolations,
  getLastPostedMessage,
  getPostedMessages,
} from "./testUtils";

describe("QueryView", () => {
  function receiveBookmark(payload: unknown) {
    act(() => dispatchIncomingMessage("bookmarkSaved", payload));
  }
  beforeEach(() => {
    clearPostedMessages();
    formatMock.mockClear();
    useQueryStore.setState({ status: "idle", result: null });
    useConnectionStore.setState({ connections: [], activeConnectionId: "" });
    useSchemaStore.setState({ schemaByConnection: {} });
  });

  it("auto-formats on open for SQL editors", async () => {
    render(
      <QueryView
        connectionId="conn-1"
        initialQueryText="select 1"
        editorPresentation={{
          formatOnOpen: true,
          editorLanguage: "sql",
          sqlDialect: "postgresql",
        }}
      />,
    );

    dispatchIncomingMessage("connections", [
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        editorPresentation: {
          editorLanguage: "sql",
          sqlDialect: "postgresql",
        },
      },
    ]);

    await waitFor(() => {
      expect(formatMock).toHaveBeenCalledWith("postgresql");
    });
  });

  it("invalidates pending bookmarks on edits and ignores late, duplicate and out-of-order acknowledgements", async () => {
    const user = userEvent.setup();
    render(<QueryView connectionId="conn-1" initialQueryText="select 1" />);
    const button = screen.getByRole("button", {
      name: "Bookmark",
    }) as HTMLButtonElement;
    const editor = screen.getByLabelText("SQL editor");
    await user.click(button);
    const first = getLastPostedMessage()?.payload;
    await user.type(editor, ";");
    expect(button.disabled).toBe(false);
    receiveBookmark({ ...(first as object), ok: true });
    expect(button.disabled).toBe(false);
    expect(button.title).toBe("Add to Bookmarks");
    await user.click(button);
    const second = getLastPostedMessage()?.payload;
    await user.type(editor, " ");
    await user.click(button);
    const third = getLastPostedMessage()?.payload;
    expect(third).not.toEqual(second);
    for (const payload of [second, first, second]) {
      receiveBookmark({ ...(payload as object), ok: true });
      expect(button.disabled).toBe(true);
      expect(button.title).toBe("Add to Bookmarks");
    }
    receiveBookmark({ ...(third as object), ok: true });
    expect(button.title).toBe("Already bookmarked");
    await user.type(editor, "--changed");
    receiveBookmark({ ...(third as object), ok: true });
    expect(button.disabled).toBe(false);
    expect(button.title).toBe("Add to Bookmarks");
  });

  it("invalidates A on connection switch, preserves editor text and keeps B busy when A replies", async () => {
    const user = userEvent.setup();
    render(<QueryView connectionId="conn-1" initialQueryText="select 1" />);
    dispatchIncomingMessage("connections", [
      { id: "conn-1", name: "A", type: "pg" },
      { id: "conn-2", name: "B", type: "pg" },
    ]);
    const button = screen.getByRole("button", {
      name: "Bookmark",
    }) as HTMLButtonElement;
    await user.click(button);
    const first = getLastPostedMessage()?.payload;
    await user.selectOptions(
      screen.getByRole("combobox", { name: "Active connection" }),
      "conn-2",
    );
    expect(button.disabled).toBe(false);
    expect(
      (screen.getByLabelText("SQL editor") as HTMLTextAreaElement).value,
    ).toBe("select 1");
    await user.click(button);
    const second = getLastPostedMessage()?.payload;
    expect(second).toMatchObject({
      connectionId: "conn-2",
      queryText: "select 1",
      requestId: expect.any(String),
    });
    receiveBookmark({ ...(first as object), ok: true });
    expect(button.disabled).toBe(true);
    expect(button.title).toBe("Add to Bookmarks");
    receiveBookmark({ ...(second as object), ok: true });
    expect(button.title).toBe("Already bookmarked");
  });

  it("accepts only matching acknowledgements and allows retry after failure", async () => {
    const user = userEvent.setup();
    render(<QueryView connectionId="conn-1" initialQueryText="  select 1  " />);
    const button = screen.getByRole("button", {
      name: "Bookmark",
    }) as HTMLButtonElement;
    await user.click(button);
    const first = getLastPostedMessage()?.payload;
    expect(first).toMatchObject({
      queryText: "select 1",
      requestId: expect.any(String),
    });
    for (const payload of [
      { ok: true },
      { ok: true, requestId: "other" },
      { ok: "yes", ...(first as object) },
    ]) {
      receiveBookmark(payload);
      expect(button.disabled).toBe(true);
      expect(button.title).toBe("Add to Bookmarks");
    }
    receiveBookmark({
      ...(first as object),
      ok: false,
      error: "Storage failed",
    });
    expect(button.disabled).toBe(false);
    await user.click(button);
    const retry = getLastPostedMessage()?.payload;
    expect(retry).not.toEqual(first);
    receiveBookmark({ ...(first as object), ok: false });
    expect(button.disabled).toBe(true);
    receiveBookmark({ ...(retry as object), ok: true });
    expect(button.title).toBe("Already bookmarked");
  });

  it("resets an initially saved bookmark when switching connections", async () => {
    const user = userEvent.setup();
    render(
      <QueryView
        connectionId="conn-1"
        initialQueryText="select 1"
        isBookmarked
      />,
    );
    act(() =>
      dispatchIncomingMessage("connections", [
        { id: "conn-1", name: "A", type: "pg" },
        { id: "conn-2", name: "B", type: "pg" },
      ]),
    );
    const button = screen.getByRole("button", {
      name: "Bookmark",
    }) as HTMLButtonElement;
    expect(button.title).toBe("Already bookmarked");
    await user.selectOptions(
      screen.getByRole("combobox", { name: "Active connection" }),
      "conn-2",
    );
    expect(button.disabled).toBe(false);
    expect(button.title).toBe("Add to Bookmarks");
  });

  it("keeps refs and state in sync when a matching ack and an edit occur in one event", async () => {
    const user = userEvent.setup();
    render(<QueryView connectionId="conn-1" initialQueryText="select 1" />);
    const button = screen.getByRole("button", {
      name: "Bookmark",
    }) as HTMLButtonElement;
    const editor = screen.getByLabelText("SQL editor");
    await user.click(button);
    const first = getLastPostedMessage()?.payload;
    act(() => {
      dispatchIncomingMessage("bookmarkSaved", {
        ...(first as object),
        ok: true,
      });
      fireEvent.change(editor, { target: { value: "select 2" } });
    });
    expect(button.disabled).toBe(false);
    expect(button.title).toBe("Add to Bookmarks");
    act(() => {
      fireEvent.click(button);
      fireEvent.click(button);
    });
    expect(
      getPostedMessages().filter((message) => message.type === "addBookmark"),
    ).toHaveLength(2);
    expect(getLastPostedMessage()?.payload).toEqual({
      queryText: "select 2",
      connectionId: "conn-1",
      requestId: "query:bookmark:2",
    });
    await user.click(screen.getByRole("button", { name: "Run" }));
    expect(getLastPostedMessage()?.payload).toMatchObject({
      operationId: "query:1",
    });
  });

  it("invalidates pending saves when the active connection is removed without replacing editor text", async () => {
    const user = userEvent.setup();
    render(<QueryView connectionId="conn-1" initialQueryText="select 1" />);
    dispatchIncomingMessage("connections", [
      { id: "conn-1", name: "A", type: "pg" },
      { id: "conn-2", name: "B", type: "pg" },
    ]);
    const button = screen.getByRole("button", {
      name: "Bookmark",
    }) as HTMLButtonElement;
    await user.click(button);
    const first = getLastPostedMessage()?.payload;
    act(() =>
      dispatchIncomingMessage("connections", [
        { id: "conn-2", name: "B", type: "pg" },
      ]),
    );
    receiveBookmark({ ...(first as object), ok: true });
    expect(button.disabled).toBe(false);
    expect(button.title).toBe("Add to Bookmarks");
    expect(
      (screen.getByLabelText("SQL editor") as HTMLTextAreaElement).value,
    ).toBe("select 1");
    await user.selectOptions(
      screen.getByRole("combobox", { name: "Active connection" }),
      "conn-2",
    );
    await user.click(button);
    expect(getLastPostedMessage()?.payload).toMatchObject({
      connectionId: "conn-2",
      queryText: "select 1",
    });
  });

  it("offers Stop and sends a cancellation request for the active query", async () => {
    const user = userEvent.setup();
    render(
      <QueryView
        panelId="query-stop"
        connectionId="conn-1"
        initialQueryText="select 1"
      />,
    );

    await user.click(screen.getByRole("button", { name: "Run" }));
    const operation = getPostedMessages().find(
      (message) => message.type === "executeQuery",
    )?.payload as { operationId?: string } | undefined;
    expect(operation?.operationId).toBeTruthy();

    await user.click(screen.getByRole("button", { name: "Stop" }));
    expect(getLastPostedMessage()).toEqual({
      type: "cancelQuery",
      payload: { operationId: operation?.operationId },
    });
  });

  it("persists and restores the current editor text by panel ID", () => {
    const api = window.__vscode as NonNullable<Window["__vscode"]> & {
      getState: ReturnType<typeof vi.fn>;
      setState: ReturnType<typeof vi.fn>;
    };
    const first = render(
      <QueryView
        panelId="query-draft"
        connectionId="conn-1"
        initialQueryText="select 1"
      />,
    );
    fireEvent.change(screen.getByLabelText("SQL editor"), {
      target: { value: "select 2" },
    });
    const persisted = api.setState.mock.calls.at(-1)?.[0] as
      | Record<string, unknown>
      | undefined;
    expect(persisted?.queryDraft).toEqual({
      panelId: "query-draft",
      text: "select 2",
      activeConnectionId: "conn-1",
    });
    first.unmount();

    api.getState.mockReturnValue(persisted);
    render(
      <QueryView
        panelId="query-draft"
        connectionId="conn-1"
        initialQueryText="select 1"
      />,
    );
    expect(
      (screen.getByLabelText("SQL editor") as HTMLTextAreaElement).value,
    ).toBe("select 2");
  });

  it("falls back to an available connection when the persisted active connection was removed", () => {
    const api = window.__vscode as NonNullable<Window["__vscode"]> & {
      getState: ReturnType<typeof vi.fn>;
    };
    api.getState.mockReturnValue({
      queryDraft: {
        panelId: "query-removed-connection",
        text: "select 1",
        activeConnectionId: "removed-connection",
      },
    });
    render(
      <QueryView
        panelId="query-removed-connection"
        connectionId="removed-connection"
        initialQueryText="select 0"
      />,
    );

    act(() =>
      dispatchIncomingMessage("connections", [
        { id: "conn-2", name: "Secondary", type: "pg" },
        { id: "conn-3", name: "Tertiary", type: "mysql" },
      ]),
    );

    expect(
      (screen.getByLabelText("Active connection") as HTMLSelectElement).value,
    ).toBe("conn-2");
    expect(getPostedMessages()).toContainEqual({
      type: "activeConnectionChanged",
      payload: { connectionId: "conn-2" },
    });
  });

  it.each([
    {
      text: "select 2",
      draftConnectionId: "conn-1",
      expectedBookmarked: false,
    },
    { text: "select 1", draftConnectionId: "conn-1", expectedBookmarked: true },
    {
      text: "  select 1  ",
      draftConnectionId: "conn-1",
      expectedBookmarked: true,
    },
    {
      text: "select 1",
      draftConnectionId: "conn-2",
      expectedBookmarked: false,
    },
  ])("restores bookmark status for draft $text on $draftConnectionId (bookmarked=$expectedBookmarked)", async ({
    text,
    draftConnectionId,
    expectedBookmarked,
  }) => {
    const user = userEvent.setup();
    const api = window.__vscode as NonNullable<Window["__vscode"]> & {
      getState: ReturnType<typeof vi.fn>;
    };
    api.getState.mockReturnValue({
      queryDraft: {
        panelId: "qp_42",
        text,
        activeConnectionId: draftConnectionId,
      },
    });
    render(
      <QueryView
        panelId="qp_42"
        connectionId="conn-1"
        initialQueryText="select 1"
        isBookmarked
      />,
    );
    act(() =>
      dispatchIncomingMessage("connections", [
        { id: "conn-1", name: "Original", type: "pg" },
        { id: "conn-2", name: "Other", type: "pg" },
      ]),
    );
    const button = screen.getByRole("button", {
      name: "Bookmark",
    }) as HTMLButtonElement;
    expect(
      (screen.getByLabelText("SQL editor") as HTMLTextAreaElement).value,
    ).toBe(text);
    expect(button.disabled).toBe(expectedBookmarked);
    expect(button.title).toBe(
      expectedBookmarked ? "Already bookmarked" : "Add to Bookmarks",
    );
    clearPostedMessages();
    await user.click(button);
    if (expectedBookmarked) {
      expect(getPostedMessages()).toEqual([]);
      // A bookmark belongs to its connection, not just its SQL text.
      await user.selectOptions(
        screen.getByLabelText("Active connection"),
        "conn-2",
      );
      expect(button.disabled).toBe(false);
      await user.click(button);
      expect(getLastPostedMessage()?.payload).toMatchObject({
        queryText: text.trim(),
        connectionId: "conn-2",
      });
    } else {
      const request = getLastPostedMessage();
      expect(request).toMatchObject({
        type: "addBookmark",
        payload: {
          queryText: text.trim(),
          connectionId: draftConnectionId,
          requestId: expect.any(String),
        },
      });
      receiveBookmark({ ...(request?.payload as object), ok: true });
      expect(button.disabled).toBe(true);
      expect(button.title).toBe("Already bookmarked");
    }
  });

  it("invalidates bookmarks on externally changed active connections", async () => {
    const user = userEvent.setup();
    render(<QueryView connectionId="conn-1" initialQueryText="select 1" />);
    const button = screen.getByRole("button", {
      name: "Bookmark",
    }) as HTMLButtonElement;
    await user.click(button);
    const first = getLastPostedMessage()?.payload;
    act(() => {
      useConnectionStore.getState().setActiveConnection("conn-2");
      dispatchIncomingMessage("bookmarkSaved", {
        ...(first as object),
        ok: true,
      });
    });
    expect(button.disabled).toBe(false);
    expect(button.title).toBe("Add to Bookmarks");
    await user.click(button);
    expect(getLastPostedMessage()?.payload).toMatchObject({
      connectionId: "conn-2",
    });
  });

  it("resets running state on connection switch and ignores the old completion", async () => {
    const user = userEvent.setup();
    render(<QueryView connectionId="conn-1" initialQueryText="select 1" />);
    dispatchIncomingMessage("connections", [
      { id: "conn-1", name: "First", type: "pg" },
      { id: "conn-2", name: "Second", type: "pg" },
    ]);
    await user.click(screen.getByRole("button", { name: "Run" }));
    const oldRequest = getPostedMessages().find(
      (message) => message.type === "executeQuery",
    )?.payload;
    expect(oldRequest).toMatchObject({
      connectionId: "conn-1",
      operationId: expect.any(String),
    });
    expect(useQueryStore.getState().status).toBe("running");
    await user.selectOptions(
      screen.getByRole("combobox", { name: "Active connection" }),
      "conn-2",
    );
    expect(useQueryStore.getState().status).toBe("idle");
    dispatchIncomingMessage("queryResult", {
      ...(oldRequest as object),
      connectionId: "conn-1",
      columns: [],
      rows: [],
      rowCount: 0,
      executionTimeMs: 0,
    });
    expect(useQueryStore.getState().status).toBe("idle");
    await user.click(screen.getByRole("button", { name: "Run" }));
    expect(useQueryStore.getState().status).toBe("running");
  });

  it("resets a running query on removed-connection fallback and ignores its stale responses", async () => {
    const user = userEvent.setup();
    render(<QueryView connectionId="conn-1" initialQueryText="select 1" />);
    act(() =>
      dispatchIncomingMessage("connections", [
        { id: "conn-1", name: "First", type: "pg" },
        { id: "conn-2", name: "Second", type: "pg" },
      ]),
    );
    await user.click(screen.getByRole("button", { name: "Run" }));
    const oldRequest = getLastPostedMessage()?.payload as {
      operationId: string;
      connectionId: string;
    };
    expect(useQueryStore.getState().status).toBe("running");

    act(() =>
      dispatchIncomingMessage("connections", [
        { id: "conn-2", name: "Second", type: "pg" },
      ]),
    );
    expect(useQueryStore.getState().status).toBe("idle");
    expect(screen.queryByRole("button", { name: "Stop" })).toBeNull();
    expect(
      (screen.getByRole("button", { name: "Run" }) as HTMLButtonElement)
        .disabled,
    ).toBe(false);
    expect(
      (screen.getByLabelText("SQL editor") as HTMLTextAreaElement).value,
    ).toBe("select 1");
    expect(getPostedMessages()).toContainEqual({
      type: "activeConnectionChanged",
      payload: { connectionId: "conn-2" },
    });
    const staleResult = {
      ...oldRequest,
      columns: [],
      rows: [],
      rowCount: 0,
      executionTimeMs: 0,
      error: "Old connection failed",
    };
    act(() => dispatchIncomingMessage("queryResult", staleResult));
    expect(useQueryStore.getState().status).toBe("idle");
    await user.click(screen.getByRole("button", { name: "Run" }));
    const newRequest = getLastPostedMessage()?.payload as {
      operationId: string;
      connectionId: string;
    };
    expect(newRequest.connectionId).toBe("conn-2");
    expect(newRequest.operationId).not.toBe(oldRequest.operationId);
    act(() => dispatchIncomingMessage("queryResult", staleResult));
    expect(useQueryStore.getState().status).toBe("running");
    await user.click(screen.getByRole("button", { name: "Stop" }));
    expect(getLastPostedMessage()).toEqual({
      type: "cancelQuery",
      payload: { operationId: newRequest.operationId },
    });
    act(() =>
      dispatchIncomingMessage("queryResult", {
        ...newRequest,
        columns: [],
        rows: [],
        rowCount: 1,
        executionTimeMs: 1,
      }),
    );
    expect(useQueryStore.getState().status).toBe("success");
  });

  it.each([
    "success",
    "error",
  ])("cleans up after the sole connection is removed and ignores stale %s", async (outcome) => {
    const user = userEvent.setup();
    render(
      <QueryView
        panelId="query-empty-connections"
        connectionId="conn-1"
        initialQueryText="select 1"
      />,
    );
    act(() =>
      dispatchIncomingMessage("connections", [
        { id: "conn-1", name: "Only connection", type: "pg" },
      ]),
    );
    await user.click(screen.getByRole("button", { name: "Run" }));
    const oldRequest = getLastPostedMessage()?.payload as {
      operationId: string;
      connectionId: string;
    };
    const staleResult = {
      ...oldRequest,
      columns: [],
      rows: [],
      rowCount: 1,
      executionTimeMs: 1,
      ...(outcome === "error" ? { error: "Removed connection failed" } : {}),
    };
    clearPostedMessages();
    act(() => {
      dispatchIncomingMessage("connections", []);
      // The old render's shortcut callback must also be safe before rerender.
      fireEvent.keyDown(screen.getByLabelText("SQL editor"), { key: "F5" });
      fireEvent.click(screen.getByRole("button", { name: "Bookmark" }));
      dispatchIncomingMessage("queryResult", staleResult);
    });
    expect(useQueryStore.getState()).toMatchObject({
      status: "idle",
      result: null,
    });
    expect(useConnectionStore.getState().activeConnectionId).toBe("");
    expect(screen.queryByRole("button", { name: "Stop" })).toBeNull();
    expect(
      (screen.getByRole("button", { name: "Run" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    expect(
      (screen.getByRole("button", { name: "Bookmark" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    expect(
      (screen.getByLabelText("Active connection") as HTMLSelectElement).value,
    ).toBe("");
    expect(
      screen.getByRole("option", { name: "No connections available" }),
    ).toBeTruthy();
    expect(screen.queryByTitle("Loading connections…")).toBeNull();
    expect(
      (screen.getByLabelText("SQL editor") as HTMLTextAreaElement).value,
    ).toBe("select 1");
    fireEvent.keyDown(screen.getByLabelText("SQL editor"), {
      key: "Enter",
      ctrlKey: true,
    });
    expect(getPostedMessages()).toEqual([]);
    const api = window.__vscode as NonNullable<Window["__vscode"]> & {
      setState: ReturnType<typeof vi.fn>;
    };
    expect(api.setState.mock.calls.at(-1)?.[0]).toMatchObject({
      queryDraft: {
        panelId: "query-empty-connections",
        text: "select 1",
        activeConnectionId: "",
      },
    });

    // Returning connections restores a usable selection without reviving the old run.
    act(() =>
      dispatchIncomingMessage("connections", [
        { id: "conn-2", name: "New connection", type: "pg" },
      ]),
    );
    await user.click(screen.getByRole("button", { name: "Run" }));
    const newRequest = getLastPostedMessage()?.payload as {
      operationId: string;
      connectionId: string;
    };
    expect(newRequest.connectionId).toBe("conn-2");
    expect(newRequest.operationId).not.toBe(oldRequest.operationId);
    act(() => dispatchIncomingMessage("queryResult", staleResult));
    expect(useQueryStore.getState().status).toBe("running");
    expect(getPostedMessages()).not.toContainEqual({
      type: "getSchema",
      payload: { connectionId: "conn-1" },
    });
  });

  it("does not restore the removed initial connection from a persisted empty selection", () => {
    const api = window.__vscode as NonNullable<Window["__vscode"]> & {
      getState: ReturnType<typeof vi.fn>;
    };
    api.getState.mockReturnValue({
      queryDraft: {
        panelId: "query-no-connection",
        text: "select 1",
        activeConnectionId: "",
      },
    });
    render(
      <QueryView
        panelId="query-no-connection"
        connectionId="removed-connection"
        initialQueryText="select 0"
      />,
    );
    act(() => dispatchIncomingMessage("connections", []));
    fireEvent.keyDown(screen.getByLabelText("SQL editor"), { key: "F5" });
    expect(useConnectionStore.getState().activeConnectionId).toBe("");
    expect(getPostedMessages()).toEqual([{ type: "getConnections" }]);
    expect(useQueryStore.getState().status).toBe("idle");
  });

  it("auto-formats when the active connection presentation arrives after mount", async () => {
    render(<QueryView connectionId="conn-1" initialQueryText="select 1" />);

    dispatchIncomingMessage("connections", [
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        editorPresentation: {
          formatOnOpen: true,
          editorLanguage: "sql",
          sqlDialect: "postgresql",
        },
      },
    ]);

    await waitFor(() => {
      expect(formatMock).toHaveBeenCalledWith("postgresql");
    });
  });

  it("requests connections and schema, updates the active connection, and reacts to schema messages", async () => {
    const user = userEvent.setup();
    const { container } = render(
      <QueryView
        connectionId="conn-1"
        initialQueryText="select * from users"
        connectionType="pg"
        editorPresentation={{
          editorLanguage: "sql",
          sqlDialect: "postgresql",
        }}
      />,
    );

    await waitFor(() => {
      expect(getPostedMessages()).toEqual(
        expect.arrayContaining([
          { type: "getConnections" },
          { type: "getSchema", payload: { connectionId: "conn-1" } },
        ]),
      );
    });

    dispatchIncomingMessage("connections", [
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        editorPresentation: {
          editorLanguage: "sql",
          sqlDialect: "postgresql",
        },
      },
      {
        id: "conn-2",
        name: "Mongo",
        type: "mongodb",
        editorPresentation: {
          editorLanguage: "javascript",
        },
      },
      {
        id: "conn-3",
        name: "Redis",
        type: "redis",
        editorPresentation: {
          editorLanguage: "plaintext",
        },
      },
      {
        id: "conn-4",
        name: "Dynamo",
        type: "dynamodb",
        editorPresentation: {
          queryMode: "text",
          editorLanguage: "json",
          formatOnOpen: false,
          allowFormatting: false,
        },
      },
    ]);
    dispatchIncomingMessage("schema", {
      connectionId: "conn-1",
      schema: [
        {
          database: "app_db",
          schema: "public",
          object: "users",
          columns: [],
        },
      ],
    });

    await waitFor(() => {
      expect(screen.getByTestId("monaco-schema-count").textContent).toBe("1");
    });

    await expectNoAxeViolations(container);

    clearPostedMessages();

    await user.selectOptions(
      screen.getByRole("combobox", { name: "Active connection" }),
      "conn-2",
    );

    await waitFor(() => {
      expect(getPostedMessages()).toEqual(
        expect.arrayContaining([
          {
            type: "activeConnectionChanged",
            payload: { connectionId: "conn-2" },
          },
          { type: "getSchema", payload: { connectionId: "conn-2" } },
        ]),
      );
    });

    await waitFor(() => {
      expect(screen.getByLabelText("Query editor")).toBeTruthy();
      expect(screen.getByTestId("monaco-language").textContent).toBe(
        "javascript",
      );
      expect(screen.getByTestId("monaco-dialect").textContent).toBe("none");
      expect(
        (screen.getByRole("button", { name: "Format" }) as HTMLButtonElement)
          .disabled,
      ).toBe(true);
    });

    clearPostedMessages();

    await user.selectOptions(
      screen.getByRole("combobox", { name: "Active connection" }),
      "conn-3",
    );

    await waitFor(() => {
      expect(getPostedMessages()).toEqual(
        expect.arrayContaining([
          {
            type: "activeConnectionChanged",
            payload: { connectionId: "conn-3" },
          },
          { type: "getSchema", payload: { connectionId: "conn-3" } },
        ]),
      );
      expect(screen.getByLabelText("Query editor")).toBeTruthy();
      expect(screen.getByTestId("monaco-language").textContent).toBe(
        "plaintext",
      );
      expect(screen.getByTestId("monaco-dialect").textContent).toBe("none");
    });

    clearPostedMessages();

    await user.selectOptions(
      screen.getByRole("combobox", { name: "Active connection" }),
      "conn-4",
    );

    await waitFor(() => {
      expect(getPostedMessages()).toEqual(
        expect.arrayContaining([
          {
            type: "activeConnectionChanged",
            payload: { connectionId: "conn-4" },
          },
          { type: "getSchema", payload: { connectionId: "conn-4" } },
        ]),
      );
      expect(screen.getByLabelText("Query editor")).toBeTruthy();
      expect(screen.getByTestId("monaco-language").textContent).toBe("json");
      expect(screen.getByTestId("monaco-dialect").textContent).toBe("none");
      expect(
        (screen.getByRole("button", { name: "Format" }) as HTMLButtonElement)
          .disabled,
      ).toBe(false);
    });
  });

  it("lets active connection presentation override the initial editor language", async () => {
    const user = userEvent.setup();

    render(
      <QueryView
        connectionId="conn-1"
        initialQueryText={"db.users.find({ active: true })"}
        editorLanguage="javascript"
        editorPresentation={{
          formatOnOpen: false,
          editorLanguage: "javascript",
        }}
      />,
    );

    dispatchIncomingMessage("connections", [
      {
        id: "conn-1",
        name: "Mongo",
        type: "mongodb",
        editorPresentation: {
          formatOnOpen: false,
          editorLanguage: "javascript",
        },
      },
      {
        id: "conn-2",
        name: "Primary",
        type: "pg",
        editorPresentation: {
          formatOnOpen: true,
          editorLanguage: "sql",
          sqlDialect: "postgresql",
        },
      },
    ]);

    await waitFor(() => {
      expect(screen.getByTestId("monaco-language").textContent).toBe(
        "javascript",
      );
    });

    await user.selectOptions(
      screen.getByRole("combobox", { name: "Active connection" }),
      "conn-2",
    );

    await waitFor(() => {
      expect(screen.getByLabelText("SQL editor")).toBeTruthy();
      expect(screen.getByTestId("monaco-language").textContent).toBe("sql");
      expect(screen.getByTestId("monaco-dialect").textContent).toBe(
        "postgresql",
      );
    });
  });

  it("replaces the active connection flattened schema array as shared-cache scopes expand", async () => {
    render(
      <QueryView
        connectionId="conn-1"
        initialQueryText="select * from users"
        editorPresentation={{
          editorLanguage: "sql",
          sqlDialect: "postgresql",
        }}
      />,
    );

    dispatchIncomingMessage("connections", [
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        editorPresentation: {
          editorLanguage: "sql",
          sqlDialect: "postgresql",
        },
      },
    ]);

    dispatchIncomingMessage("schema", {
      connectionId: "conn-1",
      schema: [
        {
          database: "app_db",
          schema: "public",
          object: "users",
          columns: [{ name: "id", type: "int" }],
        },
      ],
    });

    await waitFor(() => {
      expect(screen.getByTestId("monaco-schema-count").textContent).toBe("1");
    });

    dispatchIncomingMessage("schema", {
      connectionId: "conn-1",
      schema: [
        {
          database: "app_db",
          schema: "public",
          object: "users",
          columns: [{ name: "id", type: "int" }],
        },
        {
          database: "app_db",
          schema: "audit",
          object: "sync_events",
          columns: [],
        },
      ],
    });

    await waitFor(() => {
      expect(screen.getByTestId("monaco-schema-count").textContent).toBe("2");
    });

    dispatchIncomingMessage("schema", {
      connectionId: "conn-1",
      schema: [
        {
          database: "app_db",
          schema: "public",
          object: "users",
          columns: [{ name: "id", type: "int" }],
        },
      ],
    });

    await waitFor(() => {
      expect(screen.getByTestId("monaco-schema-count").textContent).toBe("1");
    });
  });

  it("executes queries, shows result errors, and resets bookmark state after edits", async () => {
    const user = userEvent.setup();

    render(
      <QueryView
        connectionId="conn-1"
        initialQueryText="select 1"
        editorPresentation={{
          editorLanguage: "sql",
          sqlDialect: "postgresql",
        }}
      />,
    );

    dispatchIncomingMessage("connections", [
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        editorPresentation: {
          editorLanguage: "sql",
          sqlDialect: "postgresql",
        },
      },
    ]);

    const editor = screen.getByLabelText("SQL editor");
    await user.clear(editor);
    await user.type(editor, "select 42");

    clearPostedMessages();

    await user.click(screen.getByRole("button", { name: "Run" }));

    expect(getLastPostedMessage()).toEqual({
      type: "executeQuery",
      payload: {
        queryText: "select 42",
        connectionId: "conn-1",
        operationId: "query:1",
      },
    });

    dispatchIncomingMessage("queryResult", {
      columns: [],
      columnMeta: [],
      rows: [],
      rowCount: 0,
      executionTimeMs: 1,
      error: "Stale error",
      operationId: "other-panel:1",
      connectionId: "conn-1",
    });
    expect(screen.getByTestId("results-panel").textContent).toBe(
      "query:running:none",
    );

    dispatchIncomingMessage("queryResult", {
      columns: [],
      columnMeta: [],
      rows: [],
      rowCount: 0,
      executionTimeMs: 3,
      error: "Bad SQL",
      operationId: "query:1",
      connectionId: "conn-1",
    });

    await waitFor(() => {
      expect(screen.getByTestId("results-panel").textContent).toBe(
        "query:error:Bad SQL",
      );
    });

    clearPostedMessages();

    const bookmarkButton = screen.getByRole("button", { name: "Bookmark" });
    await user.click(bookmarkButton);

    expect(getLastPostedMessage()).toEqual({
      type: "addBookmark",
      payload: {
        queryText: "select 42",
        connectionId: "conn-1",
        requestId: "query:bookmark:1",
      },
    });

    dispatchIncomingMessage("bookmarkSaved", {
      ok: true,
      requestId: "query:bookmark:1",
    });

    await waitFor(() => {
      expect((bookmarkButton as HTMLButtonElement).disabled).toBe(true);
    });

    await user.type(editor, ";");

    await waitFor(() => {
      expect((bookmarkButton as HTMLButtonElement).disabled).toBe(false);
    });
    expect(screen.getByTestId("results-panel").textContent).toBe(
      "query:error:Bad SQL",
    );
  });

  it.each([
    { queryText: "", trigger: "button" },
    { queryText: " \t\n", trigger: "keyboard" },
  ])("shows validation feedback for a blank query submitted by $trigger", async ({
    queryText,
    trigger,
  }) => {
    const user = userEvent.setup();
    render(<QueryView connectionId="conn-1" initialQueryText={queryText} />);

    if (trigger === "button") {
      await user.click(screen.getByRole("button", { name: "Run" }));
    } else {
      fireEvent.keyDown(screen.getByLabelText("SQL editor"), {
        key: "Enter",
        ctrlKey: true,
      });
    }

    expect(
      getPostedMessages().filter((message) => message.type === "executeQuery"),
    ).toHaveLength(0);
    expect(useQueryStore.getState().status).toBe("error");
    expect(screen.getByTestId("results-panel").textContent).toContain(
      "Select or enter a query before running.",
    );
    expect(
      (screen.getByRole("button", { name: "Run" }) as HTMLButtonElement)
        .disabled,
    ).toBe(false);
  });

  it.each([
    { key: "Enter", blank: "editor" },
    { key: "F5", blank: "editor" },
    { key: "Enter", blank: "selection" },
    { key: "F5", blank: "selection" },
  ])("keeps the pending operation running after blank $blank submission by $key", async ({
    key,
    blank,
  }) => {
    const user = userEvent.setup();
    render(<QueryView connectionId="conn-1" initialQueryText="  select 1  " />);
    await user.click(screen.getByRole("button", { name: "Run" }));
    const operation = getLastPostedMessage()?.payload as {
      operationId: string;
      connectionId: string;
    };
    const editor = screen.getByLabelText("SQL editor") as HTMLTextAreaElement;
    if (blank === "editor") {
      fireEvent.change(editor, { target: { value: " \t\n" } });
    } else {
      editor.setSelectionRange(0, 2);
      fireEvent.select(editor);
    }
    fireEvent.keyDown(editor, { key, ctrlKey: key === "Enter" });
    expect(useQueryStore.getState().status).toBe("running");
    expect(screen.getByTestId("results-panel").textContent).toBe(
      "query:running:none",
    );
    expect(
      (screen.getByRole("button", { name: "Run" }) as HTMLButtonElement)
        .disabled,
    ).toBe(true);
    expect(
      getPostedMessages().filter((message) => message.type === "executeQuery"),
    ).toHaveLength(1);
    act(() =>
      dispatchIncomingMessage("queryResult", {
        ...operation,
        columns: [],
        rows: [],
        rowCount: 1,
        executionTimeMs: 1,
      }),
    );
    expect(useQueryStore.getState().status).toBe("success");
    expect(screen.getByTestId("results-panel").textContent).toBe(
      "query:success:1",
    );
  });

  it("clears blank-query validation as soon as a nonblank query is typed", async () => {
    const user = userEvent.setup();
    render(<QueryView connectionId="conn-1" initialQueryText="" />);

    await user.click(screen.getByRole("button", { name: "Run" }));
    expect(screen.getByTestId("results-panel").textContent).toContain(
      "Select or enter a query before running.",
    );

    await user.type(screen.getByLabelText("SQL editor"), "select 1");

    expect(useQueryStore.getState().status).toBe("idle");
    expect(screen.getByTestId("results-panel").textContent).toBe(
      "query:idle:none",
    );
    expect(
      getPostedMessages().filter((message) => message.type === "executeQuery"),
    ).toHaveLength(0);
  });

  it("clears blank-query validation when a nonblank selection becomes effective", () => {
    render(<QueryView connectionId="conn-1" initialQueryText="  select 1  " />);
    const editor = screen.getByLabelText("SQL editor") as HTMLTextAreaElement;
    editor.setSelectionRange(0, 2);
    fireEvent.select(editor);
    fireEvent.keyDown(editor, { key: "Enter", ctrlKey: true });

    expect(screen.getByTestId("results-panel").textContent).toContain(
      "Select or enter a query before running.",
    );

    editor.setSelectionRange(2, 10);
    fireEvent.select(editor);

    expect(useQueryStore.getState().status).toBe("idle");
    expect(screen.getByTestId("results-panel").textContent).toBe(
      "query:idle:none",
    );
  });

  it("caps oversized query results in webview state as a defensive fallback", async () => {
    const user = userEvent.setup();
    const hardCap = QUERY_LIMIT_POLICY.hardCap;

    render(
      <QueryView
        connectionId="conn-1"
        initialQueryText="select 1"
        editorPresentation={{
          editorLanguage: "sql",
          sqlDialect: "postgresql",
        }}
      />,
    );

    dispatchIncomingMessage("connections", [
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        editorPresentation: {
          editorLanguage: "sql",
          sqlDialect: "postgresql",
        },
      },
    ]);

    const oversizedRows = Array.from({ length: hardCap + 2 }, (_, index) => ({
      __col_0: index,
    }));

    await user.click(screen.getByRole("button", { name: "Run" }));

    dispatchIncomingMessage("queryResult", {
      columns: ["n"],
      columnMeta: [],
      rows: oversizedRows,
      rowCount: oversizedRows.length,
      executionTimeMs: 1,
      operationId: "query:1",
      connectionId: "conn-1",
    });

    await waitFor(() => {
      const result = useQueryStore.getState().result;
      expect(result?.rows.length).toBe(hardCap);
      expect(result?.truncated).toBe(true);
      expect(result?.truncatedAt).toBe(hardCap);
    });
  });

  it("disables SQL formatting affordances for non-SQL connections", async () => {
    const user = userEvent.setup();

    render(
      <QueryView
        connectionId="conn-1"
        initialQueryText="db.users.find({})"
        editorPresentation={{
          editorLanguage: "javascript",
          formatOnOpen: false,
        }}
      />,
    );

    dispatchIncomingMessage("connections", [
      {
        id: "conn-1",
        name: "Mongo",
        type: "mongodb",
        editorPresentation: {
          editorLanguage: "javascript",
          formatOnOpen: false,
        },
      },
    ]);

    expect(screen.getByLabelText("Query editor")).toBeTruthy();

    const formatButton = screen.getByRole("button", { name: "Format" });
    expect((formatButton as HTMLButtonElement).disabled).toBe(true);

    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Run" }));

    expect(getLastPostedMessage()).toEqual({
      type: "executeQuery",
      payload: {
        queryText: "db.users.find({})",
        connectionId: "conn-1",
        operationId: "query:1",
      },
    });
  });

  it("defaults Redis queries to plaintext and disables formatting", async () => {
    render(
      <QueryView
        connectionId="conn-1"
        initialQueryText="GET app:key"
        formatOnOpen
        editorPresentation={{
          editorLanguage: "plaintext",
          formatOnOpen: false,
        }}
      />,
    );

    dispatchIncomingMessage("connections", [
      {
        id: "conn-1",
        name: "Redis",
        type: "redis",
        editorPresentation: {
          editorLanguage: "plaintext",
          formatOnOpen: false,
        },
      },
    ]);

    expect(screen.getByLabelText("Query editor")).toBeTruthy();
    expect(screen.getByTestId("monaco-language").textContent).toBe("plaintext");
    expect(screen.getByTestId("monaco-dialect").textContent).toBe("none");

    const formatButton = screen.getByRole("button", { name: "Format" });
    expect((formatButton as HTMLButtonElement).disabled).toBe(true);
    expect(formatMock).not.toHaveBeenCalled();
  });

  it("uses DynamoDB native mode in JSON editor language and formats valid JSON", async () => {
    const user = userEvent.setup();
    const initialValue = '{"TableName":"Users"}';
    const formattedValue = JSON.stringify({ TableName: "Users" }, null, 2);

    render(
      <QueryView
        connectionId="conn-1"
        initialQueryText={initialValue}
        formatOnOpen
        editorPresentation={{
          queryMode: "text",
          editorLanguage: "json",
          formatOnOpen: false,
          allowFormatting: false,
        }}
      />,
    );

    dispatchIncomingMessage("connections", [
      {
        id: "conn-1",
        name: "Dynamo",
        type: "dynamodb",
        editorPresentation: {
          queryMode: "text",
          editorLanguage: "json",
          formatOnOpen: false,
          allowFormatting: false,
        },
      },
    ]);

    expect(screen.getByLabelText("Query editor")).toBeTruthy();
    expect(screen.getByTestId("monaco-language").textContent).toBe("json");

    const formatButton = screen.getByRole("button", { name: "Format" });
    expect((formatButton as HTMLButtonElement).disabled).toBe(false);

    await user.click(formatButton);

    expect(formatMock).toHaveBeenCalledWith(undefined);
    expect(
      (screen.getByLabelText("Query editor") as HTMLTextAreaElement).value,
    ).toBe(formattedValue);

    clearPostedMessages();
    await user.click(screen.getByRole("button", { name: "Run" }));

    expect(getLastPostedMessage()).toEqual({
      type: "executeQuery",
      payload: {
        queryText: formattedValue,
        connectionId: "conn-1",
        operationId: "query:1",
      },
    });
  });

  it("shows JSON parse failures when formatting invalid DynamoDB native queries", async () => {
    const user = userEvent.setup();

    render(
      <QueryView
        connectionId="conn-1"
        initialQueryText='{"TableName":'
        editorPresentation={{
          queryMode: "text",
          editorLanguage: "json",
          formatOnOpen: false,
          allowFormatting: false,
        }}
      />,
    );

    dispatchIncomingMessage("connections", [
      {
        id: "conn-1",
        name: "Dynamo",
        type: "dynamodb",
        editorPresentation: {
          queryMode: "text",
          editorLanguage: "json",
          formatOnOpen: false,
          allowFormatting: false,
        },
      },
    ]);

    await user.click(screen.getByRole("button", { name: "Format" }));

    await waitFor(() => {
      expect(screen.getByTestId("results-panel").textContent).toContain(
        "error:JSON parse error:",
      );
    });
  });

  it("prefers explicit editorLanguage over connection-derived SQL mode", async () => {
    render(
      <QueryView
        connectionId="conn-1"
        initialQueryText="db.users.find({})"
        connectionType="pg"
        editorLanguage="plaintext"
        formatOnOpen
      />,
    );

    dispatchIncomingMessage("connections", [
      {
        id: "conn-1",
        name: "Primary",
        type: "pg",
        editorPresentation: {
          editorLanguage: "sql",
          sqlDialect: "postgresql",
        },
      },
    ]);

    expect(screen.getByLabelText("Query editor")).toBeTruthy();
    expect(screen.getByTestId("monaco-language").textContent).toBe("plaintext");
    expect(screen.getByTestId("monaco-dialect").textContent).toBe("none");

    const formatButton = screen.getByRole("button", { name: "Format" });
    expect((formatButton as HTMLButtonElement).disabled).toBe(true);
    expect(formatMock).not.toHaveBeenCalled();
  });
});
