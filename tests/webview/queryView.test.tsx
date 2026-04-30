import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

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
        format: () => null,
        placeCursor: () => undefined,
      }),
      [props, value],
    );

    return (
      <div>
        <div data-testid="monaco-schema-count">
          {String(props.schema?.length ?? 0)}
        </div>
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
    connTypeToDialect: (type: string) => type || "sql",
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
  clearPostedMessages,
  dispatchIncomingMessage,
  expectNoAxeViolations,
  getLastPostedMessage,
  getPostedMessages,
} from "./testUtils";

describe("QueryView", () => {
  it("requests connections and schema, updates the active connection, and reacts to schema messages", async () => {
    const user = userEvent.setup();
    const { container } = render(
      <QueryView
        connectionId="conn-1"
        initialSql="select * from users"
        connectionType="pg"
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
      { id: "conn-1", name: "Primary", type: "pg" },
      { id: "conn-2", name: "Replica", type: "mysql" },
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
  });

  it("replaces the active connection flattened schema array as shared-cache scopes expand", async () => {
    render(
      <QueryView
        connectionId="conn-1"
        initialSql="select * from users"
        connectionType="pg"
      />,
    );

    dispatchIncomingMessage("connections", [
      { id: "conn-1", name: "Primary", type: "pg" },
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
        connectionType="pg"
      />,
    );

    dispatchIncomingMessage("connections", [
      { id: "conn-1", name: "Primary", type: "pg" },
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
});
