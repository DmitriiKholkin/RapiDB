/**
 * @vitest-environment jsdom
 */

import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { TableView } from "../../src/webview/components/TableView";
import type { ColumnMeta } from "../../src/webview/types";

afterEach(cleanup);

const postMessage = vi.fn();

function makeColumn(
  overrides: Partial<ColumnMeta> & { name: string; type: string },
): ColumnMeta {
  return {
    name: overrides.name,
    type: overrides.type,
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    isAutoIncrement: false,
    category: "text",
    nativeType: overrides.type,
    filterable: true,
    editable: true,
    filterOperators: ["like"],
    isBoolean: false,
    ...overrides,
  };
}

function emit(type: string, payload: unknown): void {
  window.dispatchEvent(
    new MessageEvent("message", { data: { type, payload } }),
  );
}

describe("TableView", () => {
  beforeEach(() => {
    postMessage.mockReset();
    (
      window as Window & { __vscode?: { postMessage: typeof postMessage } }
    ).__vscode = {
      postMessage,
    };
  });

  it("renders filter controls only for filterable columns", async () => {
    render(
      <TableView
        connectionId="conn1"
        database="db"
        schema="public"
        table="users"
      />,
    );

    await waitFor(() => {
      expect(postMessage).toHaveBeenCalledWith({
        type: "ready",
        payload: undefined,
      });
    });

    emit("tableInit", {
      columns: [
        makeColumn({
          name: "id",
          type: "integer",
          category: "integer",
          isPrimaryKey: true,
          filterable: false,
          editable: false,
          isAutoIncrement: true,
        }),
        makeColumn({ name: "name", type: "text" }),
      ],
      primaryKeyColumns: ["id"],
    });
    emit("tableData", {
      rows: [{ id: 1, name: "Alice" }],
      totalCount: 1,
    });

    await waitFor(() => {
      expect(screen.getByText("Alice")).toBeDefined();
    });

    const inputs = screen.getAllByRole("textbox");
    expect(inputs).toHaveLength(2);
    expect((inputs[0] as HTMLInputElement).disabled).toBe(true);
    expect((inputs[1] as HTMLInputElement).disabled).toBe(false);
    expect(screen.getByPlaceholderText("filter")).toBeDefined();
  });

  it("blocks editing for read-only auto-increment cells but allows editable cells", async () => {
    const { container } = render(
      <TableView
        connectionId="conn1"
        database="db"
        schema="public"
        table="users"
      />,
    );

    await waitFor(() => {
      expect(postMessage).toHaveBeenCalledWith({
        type: "ready",
        payload: undefined,
      });
    });

    emit("tableInit", {
      columns: [
        makeColumn({
          name: "id",
          type: "integer",
          category: "integer",
          isPrimaryKey: true,
          filterable: false,
          editable: false,
          isAutoIncrement: true,
        }),
        makeColumn({ name: "name", type: "text" }),
      ],
      primaryKeyColumns: ["id"],
    });
    emit("tableData", {
      rows: [{ id: 1, name: "Alice" }],
      totalCount: 1,
    });

    await waitFor(() => {
      expect(screen.getByText("Alice")).toBeDefined();
    });

    const pkCell = container.querySelector('td[title="PK: 1"]');
    expect(pkCell).not.toBeNull();
    fireEvent.doubleClick(pkCell as HTMLElement);
    expect(container.querySelector('td[title="PK: 1"] input')).toBeNull();

    fireEvent.doubleClick(
      screen.getByText("Alice").closest("td") as HTMLElement,
    );
    expect(screen.getByDisplayValue("Alice")).toBeDefined();
  });

  it("serializes plain-text filters as structured filter expressions", async () => {
    render(
      <TableView
        connectionId="conn1"
        database="db"
        schema="public"
        table="users"
      />,
    );

    emit("tableInit", {
      columns: [makeColumn({ name: "name", type: "text" })],
      primaryKeyColumns: [],
    });
    emit("tableData", {
      rows: [{ name: "Alice" }],
      totalCount: 1,
    });

    const input = await screen.findByPlaceholderText("filter");
    fireEvent.change(input, { target: { value: "Alice" } });

    await waitFor(() => {
      expect(postMessage).toHaveBeenCalledWith(
        expect.objectContaining({
          type: "fetchPage",
          payload: expect.objectContaining({
            filters: [{ column: "name", operator: "like", value: "Alice" }],
          }),
        }),
      );
    });
  });

  it("allows NULL-only filters while keeping the value input disabled", async () => {
    render(
      <TableView
        connectionId="conn1"
        database="db"
        schema="public"
        table="users"
      />,
    );

    emit("tableInit", {
      columns: [
        makeColumn({
          name: "payload",
          type: "jsonb",
          filterable: false,
          editable: false,
          filterOperators: ["is_null"],
        }),
      ],
      primaryKeyColumns: [],
    });
    emit("tableData", {
      rows: [{ payload: null }],
      totalCount: 1,
    });

    const input = (await screen.findByRole("textbox")) as HTMLInputElement;
    expect(input.disabled).toBe(true);
    expect(input.placeholder).toBe("");

    const nullButton = screen.getByRole("button", { name: "NULL" });
    expect(nullButton).toBeEnabled();

    fireEvent.click(nullButton);

    await waitFor(() => {
      expect(postMessage).toHaveBeenCalledWith(
        expect.objectContaining({
          type: "fetchPage",
          payload: expect.objectContaining({
            filters: [{ column: "payload", operator: "is_null" }],
          }),
        }),
      );
    });

    expect(input.disabled).toBe(true);
    expect(input.placeholder).toBe("");
  });

  it("keeps NULL toggle disabled when is_null is unsupported", async () => {
    render(
      <TableView
        connectionId="conn1"
        database="db"
        schema="public"
        table="users"
      />,
    );

    emit("tableInit", {
      columns: [
        makeColumn({
          name: "notes",
          type: "text",
          filterable: false,
          editable: false,
          filterOperators: [],
        }),
      ],
      primaryKeyColumns: [],
    });
    emit("tableData", {
      rows: [{ notes: null }],
      totalCount: 1,
    });

    const input = (await screen.findByRole("textbox")) as HTMLInputElement;
    expect(input.disabled).toBe(true);
    expect(input.placeholder).toBe("");

    const nullButton = screen.getByRole("button", { name: "NULL" });
    expect(nullButton).toBeDisabled();
  });
});
