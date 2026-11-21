import { act, fireEvent, render, screen } from "@testing-library/react";
import { createRef } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { App } from "../../src/webview/components/App";
import {
  formatSQLOrError,
  MonacoEditor,
  type MonacoEditorHandle,
} from "../../src/webview/components/MonacoEditor";
import { resolveQueryEditorState } from "../../src/webview/components/query/queryViewHelpers";
import {
  useConnectionStore,
  useQueryStore,
  useSchemaStore,
} from "../../src/webview/store";
import { editor as mockMonaco } from "../mocks/monaco-editor";
import {
  clearPostedMessages,
  dispatchIncomingMessage,
  getLastPostedMessage,
} from "./testUtils";

describe("PostgreSQL query formatting", () => {
  it.each([
    ["plain literals", "select 'a'\n 'b';"],
    ["CRLF and escape strings", "select E'it\\'s\\t'\r\n 'fine';"],
    ["CR", "select 'a'\r 'b';"],
    ["line comments", "select 'a' -- join 'quotes'\n -- another\n 'b';"],
    ["doubled quotes", "select 'it''s'\n ' fine';"],
    ["Unicode strings", "select U&'d\\0061'\n 'ta';"],
    ["national strings", "select N'a'\n 'b';"],
    ["bit and hex strings", "select B'10'\n '11', X'fa'\n 'ce';"],
    [
      "multiple literals and statements",
      "select 'a'\n 'b'\n 'c', 'd';\nselect E'e'\n 'f';",
    ],
    ["legacy backslash escapes", "select 'it\\'s'\n ' fine' as value;"],
    [
      "continuation after dollar-quoted text and nested comments",
      "select $$ note's $$, /* outer /* inner */ note's */ 'a'\n 'b';",
    ],
  ])("leaves %s unchanged when a literal is continued", (_label, source) => {
    expect(formatSQLOrError(source, "postgresql")).toEqual({ result: source });
  });

  it.each([
    ["ordinary SELECT", "select 'a', 'b';", "SELECT\n  'a',\n  'b';"],
    [
      "newlines inside a string, including doubled quotes",
      "select 'a''\r\n''b' as value;",
      "SELECT\n  'a''\r\n''b' AS value;",
    ],
    [
      "escape string with quote and newline in its body",
      "select E'a\\'\n ' as value;",
      "SELECT\n  E'a\\'\n ' AS value;",
    ],
    [
      "quote-like text in dollar-quoted strings and quoted identifiers",
      `select $tag$'a'\n 'b'$tag$ as "'c'\n 'd'";`,
      `SELECT\n  $tag$'a'\n 'b'$tag$ AS "'c'\n 'd'";`,
    ],
    [
      "ordinary comments",
      "select /* 'a'\n 'b' */ 'c', -- 'd'\n 'e';",
      "SELECT\n  /* 'a'\n  'b' */\n  'c', -- 'd'\n  'e';",
    ],
    [
      "a line comment with a quote after a newline following a literal",
      "select 'a'\n -- 'b'\n as value;",
      "SELECT\n  'a'\n  -- 'b'\n  AS value;",
    ],
  ])("still formats %s and stays stable", (_label, source, expected) => {
    expect(formatSQLOrError(source, "postgresql")).toEqual({
      result: expected,
    });
    expect(formatSQLOrError(expected, "postgresql")).toEqual({
      result: expected,
    });
  });

  it("keeps the Monaco model unchanged through both manual format entry points", () => {
    const create = mockMonaco.create;
    const addAction = vi.fn<ReturnType<typeof create>["addAction"]>();
    const executeEdits = vi.fn();
    vi.spyOn(mockMonaco, "create").mockImplementation((...args) => ({
      ...create(...args),
      addAction,
      executeEdits,
    }));
    addAction.mockReturnValue({ dispose: () => {} });

    const ref = createRef<MonacoEditorHandle>();
    const source = "select 'a'\n 'b';";
    render(
      <MonacoEditor ref={ref} initialValue={source} dialect="postgresql" />,
    );

    expect(ref.current?.format()).toBeNull();
    const shortcut = addAction.mock.calls
      .map(([action]) => action)
      .find((action) => action.id === "rapidb.format");
    expect(shortcut).toBeDefined();
    shortcut?.run();
    expect(executeEdits).not.toHaveBeenCalled();
    expect(ref.current?.getValue()).toBe(source);
  });
});

describe("query panel formatting on open", () => {
  const targetSessionKey = "a".repeat(64);
  beforeEach(() => {
    clearPostedMessages();
    useConnectionStore.setState({ connections: [], activeConnectionId: "" });
    useQueryStore.setState({
      status: "idle",
      result: null,
      activeOperation: null,
    });
    useSchemaStore.setState({ schemaByConnection: {} });
  });

  afterEach(() => {
    delete window.__RAPIDB_INITIAL_STATE__;
  });

  it.each([
    { label: "ordinary query", source: "select 1;", expected: "SELECT\n  1;" },
    {
      label: "query with a continued literal",
      source: "select 'a'\n 'b';",
      expected: "select 'a'\n 'b';",
    },
    {
      label: "bookmark",
      source: "select 1;",
      expected: "select 1;",
      bookmark: true,
    },
    {
      label: "pinned bookmark",
      source: "select 1;",
      expected: "select 1;",
      bookmark: true,
      postgresDatabase: "app_db",
    },
    {
      label: "SQL Server pinned bookmark",
      source: "select DB_NAME();",
      expected: "select DB_NAME();",
      bookmark: true,
      mssqlDatabase: "  Case]Δ  ",
    },
    {
      label: "ordinary pinned query",
      source: "select 1;",
      expected: "SELECT\n  1;",
      postgresDatabase: "app_db",
    },
    {
      label: "query with an explicit opt-in",
      source: "select 1;",
      expected: "SELECT\n  1;",
      optIn: true,
    },
  ])("opens and runs a $label without unwanted formatting", async ({
    source,
    expected,
    bookmark,
    postgresDatabase,
    mssqlDatabase,
    optIn,
  }) => {
    const type = mssqlDatabase === undefined ? "pg" : "mssql";
    const pinnedDatabase = postgresDatabase ?? mssqlDatabase;
    window.__RAPIDB_INITIAL_STATE__ = {
      view: "query",
      connectionId: "conn-1",
      connectionType: type,
      targetBinding: { connectionId: "conn-1", sessionKey: targetSessionKey },
      postgresDatabase,
      mssqlDatabase,
      queryText: source,
      formatOnOpen: bookmark ? false : optIn,
      isBookmarked: bookmark,
      ...(bookmark && pinnedDatabase
        ? { editorPresentation: { formatOnOpen: false } }
        : {}),
    };
    render(<App />);
    act(() =>
      dispatchIncomingMessage("connections", [
        {
          id: "conn-1",
          name: "Primary",
          type,
          targetSessionKey,
          editorPresentation: {
            formatOnOpen: !optIn,
            editorLanguage: "sql",
            sqlDialect: type === "pg" ? "postgresql" : "transactsql",
          },
        },
      ]),
    );
    await act(
      () =>
        new Promise<void>((resolve) => {
          requestAnimationFrame(() => requestAnimationFrame(() => resolve()));
        }),
    );

    if (pinnedDatabase !== undefined) {
      expect(
        screen.getByText(
          `Pinned target: Primary / ${JSON.stringify(pinnedDatabase)}`,
          {
            normalizer: (text) => text,
          },
        ),
      ).toBeTruthy();
      expect(
        screen.queryByRole("combobox", { name: "Active connection" }),
      ).toBeNull();
    }
    if (bookmark) {
      expect(screen.getByRole("button", { name: "Bookmark" }).title).toBe(
        "Already bookmarked",
      );
    }
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    expect(getLastPostedMessage()).toEqual({
      type: "executeQuery",
      payload: expect.objectContaining({
        queryText: expected,
        connectionId: "conn-1",
        targetSessionKey,
      }),
    });
  });

  it("does not carry the original panel preference to another connection", () => {
    const state = resolveQueryEditorState({
      initialConnectionId: "conn-1",
      editorPresentation: { formatOnOpen: false, sqlDialect: "postgresql" },
      formatOnOpen: false,
      resolvedConnectionId: "conn-2",
      activeConnection: {
        id: "conn-2",
        name: "MySQL",
        type: "mysql",
        editorPresentation: {
          formatOnOpen: true,
          editorLanguage: "sql",
          sqlDialect: "mysql",
        },
      },
    });
    expect(state.shouldFormatOnOpen).toBe(true);
    expect(state.sqlDialect).toBe("mysql");
  });
});
