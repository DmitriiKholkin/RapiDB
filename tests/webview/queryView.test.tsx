import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

const formatMock = vi.hoisted(() => vi.fn((_dialect?: string) => null));

vi.mock("../../src/webview/components/MonacoEditor", async () => {
  const React = await import("react");

  interface MockMonacoEditorHandle {
    getSelectionOrValue(): string;
    getValue(): string;
    setValue(value: string): void;
    format(dialect?: string): string | null;
    placeCursor(): void;
  }

  interface MockMonacoEditorProps {
    initialValue?: string;
    onChange?: (value: string) => void;
    readOnly?: boolean;
    ariaLabel?: string;
    schema?: Array<unknown>;
    dialect?: string;
    language?: string;
  }

  const MonacoEditor = React.forwardRef<
    MockMonacoEditorHandle,
    MockMonacoEditorProps
  >(function MockMonacoEditor(props, ref) {
    const [value, setValue] = React.useState(props.initialValue ?? "");

    React.useImperativeHandle(
      ref,
      () => ({
        getSelectionOrValue: () => value,
        getValue: () => value,
        setValue: (nextValue: string) => {
          setValue(nextValue);
          props.onChange?.(nextValue);
        },
        format: (dialect?: string) => formatMock(dialect),
        placeCursor: () => undefined,
      }),
      [props, value],
    );

    return (
      <div>
        <div data-testid="monaco-schema-count">
          {String(props.schema?.length ?? 0)}
        </div>
        <div data-testid="monaco-language">{props.language ?? "sql"}</div>
        <div data-testid="monaco-dialect">{props.dialect ?? "none"}</div>
        <textarea
          aria-label={props.ariaLabel ?? "SQL editor"}
          readOnly={props.readOnly}
          value={value}
          onChange={(event) => {
            setValue(event.target.value);
            props.onChange?.(event.target.value);
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

vi.mock("../../src/webview/components/ResultsPanel", () => ({
  ResultsPanel: ({
    status,
    result,
  }: {
    status: string;
    result: { error?: string; rowCount?: number } | null;
  }) => (
    <div data-testid="results-panel">
      {status}:{result?.error ?? String(result?.rowCount ?? "none")}
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
        initialSql="select 1"
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

  it("auto-formats when the active connection presentation arrives after mount", async () => {
    render(<QueryView connectionId="conn-1" initialSql="select 1" />);

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
        initialSql="select * from users"
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
          editorLanguage: "sql",
          sqlDialect: "sql",
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
      expect(screen.getByLabelText("SQL editor")).toBeTruthy();
      expect(screen.getByTestId("monaco-language").textContent).toBe("sql");
      expect(screen.getByTestId("monaco-dialect").textContent).toBe("sql");
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
        initialSql={"db.users.find({ active: true })"}
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
        initialSql="select * from users"
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
        initialSql="select 1"
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
      payload: { sql: "select 42", connectionId: "conn-1" },
    });

    dispatchIncomingMessage("queryResult", {
      columns: [],
      columnMeta: [],
      rows: [],
      rowCount: 0,
      executionTimeMs: 3,
      error: "Bad SQL",
    });

    await waitFor(() => {
      expect(screen.getByTestId("results-panel").textContent).toBe(
        "error:Bad SQL",
      );
    });

    clearPostedMessages();

    const bookmarkButton = screen.getByRole("button", { name: "Bookmark" });
    await user.click(bookmarkButton);

    expect(getLastPostedMessage()).toEqual({
      type: "addBookmark",
      payload: { sql: "select 42", connectionId: "conn-1" },
    });

    dispatchIncomingMessage("bookmarkSaved", { ok: true });

    await waitFor(() => {
      expect((bookmarkButton as HTMLButtonElement).disabled).toBe(true);
    });

    await user.type(editor, ";");

    await waitFor(() => {
      expect((bookmarkButton as HTMLButtonElement).disabled).toBe(false);
    });
  });

  it("disables SQL formatting affordances for non-SQL connections", async () => {
    const user = userEvent.setup();

    render(
      <QueryView
        connectionId="conn-1"
        initialSql="db.users.find({})"
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
        sql: "db.users.find({})",
        connectionId: "conn-1",
      },
    });
  });

  it("defaults Redis queries to plaintext and disables formatting", async () => {
    render(
      <QueryView
        connectionId="conn-1"
        initialSql="GET app:key"
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

  it("keeps DynamoDB PartiQL in SQL mode while disabling formatting", async () => {
    render(
      <QueryView
        connectionId="conn-1"
        initialSql={'SELECT * FROM "Users"'}
        formatOnOpen
        editorPresentation={{
          editorLanguage: "sql",
          sqlDialect: "sql",
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
          editorLanguage: "sql",
          sqlDialect: "sql",
          formatOnOpen: false,
          allowFormatting: false,
        },
      },
    ]);

    expect(screen.getByLabelText("SQL editor")).toBeTruthy();
    expect(screen.getByTestId("monaco-language").textContent).toBe("sql");
    expect(formatMock).not.toHaveBeenCalled();

    const formatButton = screen.getByRole("button", { name: "Format" });
    expect((formatButton as HTMLButtonElement).disabled).toBe(true);
  });

  it("prefers explicit editorLanguage over connection-derived SQL mode", async () => {
    render(
      <QueryView
        connectionId="conn-1"
        initialSql="db.users.find({})"
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
